//! Real pinned research comparisons, independent baselines and accepted subsets.
use arrow::array::{BooleanArray, StringArray};
use graphforge_api::*;
use uuid::Uuid;
fn current(g: &GraphForge) -> Uuid {
    g.research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn branch(g: &mut GraphForge) -> Uuid {
    let r = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Study".into(),
    };
    g.create_research_branch(&r, &CancellationToken::new())
        .unwrap();
    r.branch_uuid
}
fn edit(g: &mut GraphForge, id: Uuid, query: &str) -> Uuid {
    let r = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        branch_uuid: id,
        version_uuid: Uuid::now_v7(),
        created_at: 2,
        query: query.into(),
    };
    g.execute_research_branch(&r, &CancellationToken::new())
        .unwrap();
    r.version_uuid
}
fn capture(g: &mut GraphForge) -> Uuid {
    capture_context(g, Uuid::now_v7())
}
fn capture_context(g: &mut GraphForge, context: Uuid) -> Uuid {
    let id = Uuid::now_v7();
    let op = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: id,
            context_uuid: context,
            label: None,
            description: None,
            created_at: 3,
            required_versions: Default::default(),
        })
        .unwrap();
    g.commit_research_version_operation(op, &CancellationToken::new())
        .unwrap();
    id
}
fn request(
    left: ResearchComparisonEndpoint,
    right: ResearchComparisonEndpoint,
) -> ResearchComparisonRequest {
    ResearchComparisonRequest {
        left,
        right,
        left_authority: None,
        right_authority: None,
        detail: ResearchComparisonDetail::Changes,
        accepted: vec![],
        max_fields: 40_000,
        max_bytes: 64 * 1024 * 1024,
        page_size: 1000,
        after: None,
    }
}
fn texts(result: &ExecutionResult, column: &str) -> Vec<String> {
    result
        .batches
        .iter()
        .flat_map(|b| {
            b.column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_owned())
        })
        .collect()
}
fn flag(result: &ExecutionResult, name: &str) -> bool {
    result.batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0)
}
fn changes(result: &ExecutionResult) -> std::collections::BTreeMap<String, String> {
    texts(result, "field")
        .into_iter()
        .zip(texts(result, "change"))
        .collect()
}
#[test]
fn independent_local_upstream_and_conflicting_fields_ignore_unrelated_parent_content() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut g = GraphForge::new(root.to_str()).unwrap();
    g.execute("CREATE (:Item {x:0,y:0,z:0})").unwrap();
    let b = branch(&mut g);
    let mut q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    q.detail = ResearchComparisonDetail::Summary;
    assert!(flag(
        &g.compare_research(&q, &CancellationToken::new()).unwrap(),
        "current_with_parent"
    ));
    edit(&mut g, b, "MATCH (n:Item) SET n.x = 1, n.z = 1");
    g.execute("MATCH (n:Item) SET n.y = 2, n.z = 2").unwrap();
    g.execute("CREATE (:Unrelated {x:99,y:99})").unwrap();
    let before = current(&g);
    q.detail = ResearchComparisonDetail::Changes;
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(
        changes(&result),
        std::collections::BTreeMap::from([
            ("property:x".into(), "local".into()),
            ("property:y".into(), "upstream".into()),
            ("property:z".into(), "conflict".into())
        ])
    );
    assert_eq!(current(&g), before);
    q.detail = ResearchComparisonDetail::Summary;
    let summary = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert!(flag(&summary, "updates_available"));
    assert!(flag(&summary, "research_divergence"));
    assert!(flag(&summary, "conflicts_require_review"));
    drop(g);
    let g = GraphForge::new(root.to_str()).unwrap();
    q.detail = ResearchComparisonDetail::Changes;
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap()
            .batches,
        result.batches
    );
    assert!(
        g.open_research_branch(b)
            .unwrap()
            .graph()
            .compare_research(&q, &CancellationToken::new())
            .is_err()
    );
}
#[test]
fn exact_versions_page_deterministically_while_live_continuations_fail_stale() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let first = capture(&mut g);
    let b = branch(&mut g);
    let other = branch(&mut g);
    g.execute("MATCH (n:Item) SET n.x = 1, n.y = 1").unwrap();
    let second = capture(&mut g);
    let mut frozen = request(
        ResearchComparisonEndpoint::Version {
            version_uuid: first,
        },
        ResearchComparisonEndpoint::Version {
            version_uuid: second,
        },
    );
    frozen.page_size = 1;
    let page = g
        .compare_research(&frozen, &CancellationToken::new())
        .unwrap();
    let token = page.schema.metadata()["graphforge.comparison.next_cursor"].clone();
    assert!(!token.is_empty());
    let mut live = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    live.page_size = 1;
    let live_page = g
        .compare_research(&live, &CancellationToken::new())
        .unwrap();
    live.after = Some(live_page.schema.metadata()["graphforge.comparison.next_cursor"].clone());
    g.execute("CREATE (:Noise)").unwrap();
    frozen.after = Some(token);
    let next = g
        .compare_research(&frozen, &CancellationToken::new())
        .unwrap();
    assert_ne!(texts(&page, "field"), texts(&next, "field"));
    assert_eq!(
        g.compare_research(&live, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_PAGE_SNAPSHOT_GONE"
    );
    let q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Branch { branch_uuid: other },
    );
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
    let head = g.open_research_branch(b).unwrap().version_uuid();
    let q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Version { version_uuid: head },
    );
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
}
fn contribution(g: &GraphForge, branch: Uuid, field: &str) -> (Uuid, Uuid) {
    let view = g.open_research_branch(branch).unwrap();
    let result = view.fields().unwrap();
    let batch = &result.batches[0];
    let col = |name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    let row = (0..batch.num_rows())
        .find(|&i| col("field").value(i) == field)
        .unwrap();
    (
        Uuid::parse_str(col("object_uuid").value(row)).unwrap(),
        Uuid::parse_str(col("contribution_uuid").value(row)).unwrap(),
    )
}
#[test]
fn accepted_subsets_from_distinct_versions_do_not_hide_later_local_edits() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let b = branch(&mut g);
    let sx = edit(&mut g, b, "MATCH (n:Item) SET n.x = 1");
    g.execute("MATCH (n:Item) SET n.x = 1").unwrap();
    let dx = capture(&mut g);
    let sy = edit(&mut g, b, "MATCH (n:Item) SET n.y = 1");
    g.execute("MATCH (n:Item) SET n.y = 1").unwrap();
    let dy = capture(&mut g);
    let mut q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    for (field, source, destination) in [("property:x", sx, dx), ("property:y", sy, dy)] {
        let (id, contribution) = contribution(&g, b, field);
        q.accepted.push(ResearchAcceptedContribution {
            source_version_uuid: source,
            contribution_uuid: contribution,
            destination_version_uuid: destination,
            unit: ResearchFieldIdentity {
                object_kind: "node".into(),
                object_uuid: id,
                field: field.into(),
            },
        });
    }
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(texts(&result, "change"), vec!["accepted", "accepted"]);
    edit(&mut g, b, "MATCH (n:Item) SET n.x = 3");
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(changes(&result)["property:x"], "local");
    assert_eq!(changes(&result)["property:y"], "accepted");
    g.execute("MATCH (n:Item) SET n.x = 2").unwrap();
    assert_eq!(
        changes(&g.compare_research(&q, &CancellationToken::new()).unwrap())["property:x"],
        "conflict"
    );
    let before = current(&g);
    q.accepted[0].contribution_uuid = Uuid::now_v7();
    assert!(g.compare_research(&q, &CancellationToken::new()).is_err());
    assert_eq!(current(&g), before);
}
#[test]
fn cancellation_bounds_and_missing_history_are_read_only() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0})").unwrap();
    let b = branch(&mut g);
    let mut q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    let before = current(&g);
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        g.compare_research(&q, &token).unwrap_err().code(),
        "GF_CANCELLED"
    );
    q.max_fields = 1;
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );
    q.max_fields = 40000;
    q.right = ResearchComparisonEndpoint::Version {
        version_uuid: Uuid::now_v7(),
    };
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_RESULT_NOT_RETAINED"
    );
    assert_eq!(current(&g), before);
}

#[test]
fn pinned_cursor_rejects_changed_reference_retention() {
    use graphforge_storage::research_versions::{ResearchMutation, ResearchOperation};
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let context = Uuid::now_v7();
    let cited = capture_context(&mut g, context);
    let b = branch(&mut g);
    let r = ReferenceResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: b,
        version_uuid: Uuid::now_v7(),
        reference_uuid: Uuid::now_v7(),
        source_version_uuid: cited,
        label: "Prior evidence".into(),
        created_at: 4,
    };
    g.reference_research_branch(&r, &CancellationToken::new())
        .unwrap();
    let left = r.version_uuid;
    let right = edit(&mut g, b, "MATCH (n:Item) SET n.x = 1, n.y = 1");
    capture_context(&mut g, context);
    let mut q = request(
        ResearchComparisonEndpoint::Version { version_uuid: left },
        ResearchComparisonEndpoint::Version {
            version_uuid: right,
        },
    );
    q.page_size = 1;
    let first = g.compare_research(&q, &CancellationToken::new()).unwrap();
    q.after = Some(first.schema.metadata()["graphforge.comparison.next_cursor"].clone());
    assert!(!q.after.as_ref().unwrap().is_empty());
    g.commit_research_version_operation(
        ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            mutation: ResearchMutation::DeleteVersion {
                version_uuid: cited,
            },
        },
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_PAGE_SNAPSHOT_GONE"
    );
    q.after = None;
    q.page_size = 1000;
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(
        texts(&result, "change")
            .iter()
            .filter(|s| *s == "dependency_unavailable")
            .count(),
        2
    );
}

#[path = "research_comparison/domains.rs"]
mod domains;

#[test]
fn public_comparison_contract_is_closed_and_bounded() {
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/contracts/research-comparison-api-v1.json"
    ))
    .unwrap();
    assert_eq!(contract["method"], "GraphForge.compare_research");
    let q = request(
        ResearchComparisonEndpoint::Project,
        ResearchComparisonEndpoint::Project,
    );
    let mut value = serde_json::to_value(&q).unwrap();
    value["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ResearchComparisonRequest>(value).is_err());
    let mut value = serde_json::to_value(&q).unwrap();
    value["left"]["kind"] = serde_json::json!("latest_accepted");
    assert!(serde_json::from_value::<ResearchComparisonRequest>(value).is_err());
    assert_eq!(contract["limits"]["max_fields"], q.max_fields);
    assert_eq!(contract["limits"]["max_bytes"], q.max_bytes);
}
#[test]
fn bounded_multi_page_diff_has_no_duplicate_or_missing_units() {
    let mut g = GraphForge::new(None).unwrap();
    let nodes = (0..120)
        .map(|i| format!("(:Item {{x:0,ordinal:{i}}})"))
        .collect::<Vec<_>>()
        .join(",");
    g.execute(&format!("CREATE {nodes}")).unwrap();
    let left = capture(&mut g);
    g.execute("MATCH (n:Item) SET n.x = 1").unwrap();
    let right = capture(&mut g);
    let before = current(&g);
    let mut q = request(
        ResearchComparisonEndpoint::Version { version_uuid: left },
        ResearchComparisonEndpoint::Version {
            version_uuid: right,
        },
    );
    q.max_fields = 500;
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );
    q.max_fields = 40000;
    q.max_bytes = 1024;
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );
    q.max_bytes = 64 * 1024 * 1024;
    q.page_size = 37;
    let mut ids = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
        assert!(result.batches[0].num_rows() <= 37);
        let batch = &result.batches[0];
        let column = batch
            .column_by_name("object_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(ids.insert(Uuid::from_slice(column.value(i)).unwrap()));
        }
        pages += 1;
        let next = result.schema.metadata()["graphforge.comparison.next_cursor"].clone();
        if next.is_empty() {
            break;
        }
        q.after = Some(next);
    }
    assert_eq!(ids.len(), 120);
    assert_eq!(pages, 4);
    assert_eq!(current(&g), before);
}
