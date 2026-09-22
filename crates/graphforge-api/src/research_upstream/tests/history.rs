use super::*;

#[test]
fn history_pages_bind_generation_branch_and_page_size() {
    let mut graph = GraphForge::new(None).unwrap();
    let cancel = CancellationToken::new();
    let mut update = recovery::prepare(&mut graph);
    let view = preview::load(&graph, &update.preview, &cancel).unwrap();
    let y = view
        .rows
        .iter()
        .find(|row| row.key.2 == "property:y")
        .unwrap();
    let ResearchUpstreamSelection::Selected { decisions } = &mut update.selection else {
        unreachable!()
    };
    decisions.push(ResearchUpstreamDecision {
        unit: ResearchFieldIdentity {
            object_kind: y.key.0.clone(),
            object_uuid: y.key.1,
            field: y.key.2.clone(),
        },
        resolution: ResearchUpstreamResolution::KeepLocal,
    });
    graph.update_research_branch(&update, &cancel).unwrap();
    let mut request = ResearchUpstreamHistoryRequest {
        branch_uuid: update.preview.branch_uuid,
        page_size: 1,
        after: None,
    };
    let first = graph.research_upstream_history(&request, &cancel).unwrap();
    assert_eq!(first.stats.rows_produced, 1);
    let cursor = first.schema.metadata()["graphforge.upstream.next_cursor"].clone();
    assert!(!cursor.is_empty());
    request.after = Some(cursor);
    let second = graph.research_upstream_history(&request, &cancel).unwrap();
    assert_eq!(second.stats.rows_produced, 1);
    assert!(second.schema.metadata()["graphforge.upstream.next_cursor"].is_empty());
    let field = |result: &ExecutionResult| {
        result.batches[0]
            .column_by_name("field")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    };
    assert_eq!(
        [field(&first), field(&second)],
        ["property:x", "property:y"]
    );
    request.page_size = 2;
    assert_eq!(
        graph
            .research_upstream_history(&request, &cancel)
            .unwrap_err()
            .code(),
        "GF_PAGE_SNAPSHOT_GONE"
    );
    request.page_size = 1;
    graph.execute("CREATE (:Unrelated)").unwrap();
    assert_eq!(
        graph
            .research_upstream_history(&request, &cancel)
            .unwrap_err()
            .code(),
        "GF_PAGE_SNAPSHOT_GONE"
    );
    request.after = None;
    request.page_size = 0;
    assert!(graph.research_upstream_history(&request, &cancel).is_err());
    request.page_size = 1001;
    assert!(graph.research_upstream_history(&request, &cancel).is_err());
}

#[test]
fn rejected_updates_leave_content_baselines_and_history_unchanged() {
    let mut graph = GraphForge::new(None).unwrap();
    let request = recovery::prepare(&mut graph);
    let before = graph.research_version_retention().unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(graph.update_research_branch(&request, &cancelled).is_err());

    let generation = graph.generation_for_read().unwrap().generation_uuid();
    let mut invalid = request.clone();
    invalid.actor_uuid = Uuid::nil();
    assert!(
        graph
            .update_research_branch(&invalid, &CancellationToken::new())
            .is_err()
    );
    invalid = request.clone();
    let ResearchUpstreamSelection::Selected { decisions } = &mut invalid.selection else {
        unreachable!()
    };
    decisions.push(decisions[0].clone());
    assert!(
        graph
            .update_research_branch(&invalid, &CancellationToken::new())
            .is_err()
    );
    invalid = request.clone();
    invalid.preview_sha256[0] ^= 1;
    assert!(
        graph
            .update_research_branch(&invalid, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(
        graph.generation_for_read().unwrap().generation_uuid(),
        generation
    );
    assert_eq!(graph.research_version_retention().unwrap(), before);
}
