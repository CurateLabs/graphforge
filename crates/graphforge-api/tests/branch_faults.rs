//! Branch publication faults exercise the facade refresh wrapper across CURRENT.
use arrow::array::Int64Array;
use graphforge_api::*;
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn count(graph: &GraphForge) -> i64 {
    let result = graph.execute("MATCH (n) RETURN count(n)").unwrap();
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

fn assert_views(graph: &GraphForge, branch: Uuid, sibling: Uuid, branch_count: i64) {
    assert_eq!(count(graph), 2, "parent graph must remain independent");
    assert_eq!(
        count(graph.open_research_branch(branch).unwrap().graph()),
        branch_count
    );
    assert_eq!(
        count(graph.open_research_branch(sibling).unwrap().graph()),
        1
    );
}

#[test]
fn branch_fault_child_refreshes_authority_and_replays_committed_mutation() {
    let Ok(root) = std::env::var("GF_BRANCH_FAULT_ROOT") else {
        return;
    };
    let request: ExecuteResearchBranchRequest =
        serde_json::from_str(&std::env::var("GF_BRANCH_FAULT_REQUEST").unwrap()).unwrap();
    let sibling = Uuid::parse_str(&std::env::var("GF_BRANCH_FAULT_SIBLING").unwrap()).unwrap();
    let committed = std::env::var("GF_BRANCH_FAULT_COMMITTED").unwrap() == "true";
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let before = graph.research_version_retention().unwrap();
    let error = graph
        .execute_research_branch(&request, &CancellationToken::new())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("committed={committed}")),
        "{error}"
    );
    assert_views(
        &graph,
        request.branch_uuid,
        sibling,
        if committed { 2 } else { 1 },
    );
    let after = graph.research_version_retention().unwrap();
    assert_eq!(after.branches, before.branches);
    assert_eq!(after.heads[&sibling], before.heads[&sibling]);
    if committed {
        let receipt = after.receipts[&request.operation_uuid].clone();
        assert_eq!(after.heads[&request.branch_uuid], request.version_uuid);
        assert_eq!(current(&graph), receipt.generation_uuid);
        let replay = graph
            .execute_research_branch(&request, &CancellationToken::new())
            .unwrap();
        assert_eq!(replay, receipt);
        assert_eq!(graph.research_version_retention().unwrap(), after);
        assert_eq!(current(&graph), receipt.generation_uuid);
        assert_views(&graph, request.branch_uuid, sibling, 2);
    } else {
        assert_eq!(current(&graph), request.expected_generation_uuid);
        assert_eq!(after, before);
    }
    drop(graph);
    let reopened = GraphForge::new(Some(&root)).unwrap();
    assert_eq!(reopened.research_version_retention().unwrap(), after);
    assert_views(
        &reopened,
        request.branch_uuid,
        sibling,
        if committed { 2 } else { 1 },
    );
}

#[test]
fn branch_mutation_faults_preserve_parent_sibling_and_exact_retry_after_reopen() {
    for committed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .execute("CREATE (:Person {name:'inherited'})")
            .unwrap();
        let branch = create(&mut graph, "edited");
        let sibling = create(&mut graph, "sibling");
        graph
            .execute("CREATE (:Person {name:'parent only'})")
            .unwrap();
        let request = ExecuteResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&graph),
            branch_uuid: branch.branch_uuid,
            version_uuid: Uuid::now_v7(),
            query: "CREATE (:Person {name:'branch only'})".into(),
            created_at: 2,
        };
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "branch_fault_child_refreshes_authority_and_replays_committed_mutation",
                "--nocapture",
            ])
            .env("GF_BRANCH_FAULT_ROOT", root.path())
            .env(
                "GF_BRANCH_FAULT_REQUEST",
                serde_json::to_string(&request).unwrap(),
            )
            .env("GF_BRANCH_FAULT_SIBLING", sibling.branch_uuid.to_string())
            .env("GF_BRANCH_FAULT_COMMITTED", committed.to_string())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                request.operation_uuid.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                if committed {
                    "project.after_current_replace.error"
                } else {
                    "project.before_current_replace.error"
                },
            )
            .status()
            .unwrap();
        assert!(
            status.success(),
            "Branch fault child failed: committed={committed}"
        );
        // Fault injection is confined to the child. An exact pre-CURRENT retry
        // publishes once; an exact post-CURRENT retry returns the stored receipt.
        let mut graph = GraphForge::new(root.path().to_str()).unwrap();
        let before_retry = current(&graph);
        if !committed {
            let before_conflict = graph.research_version_retention().unwrap();
            let mut conflicting = request.clone();
            conflicting.query = "CREATE (:Person {name:'different retry content'})".into();
            let error = graph
                .execute_research_branch(&conflicting, &CancellationToken::new())
                .unwrap_err();
            assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT", "{error}");
            assert_eq!(current(&graph), before_retry);
            assert_eq!(graph.research_version_retention().unwrap(), before_conflict);
            assert_views(&graph, branch.branch_uuid, sibling.branch_uuid, 1);
        }
        let receipt = graph
            .execute_research_branch(&request, &CancellationToken::new())
            .unwrap();
        if committed {
            assert_eq!(current(&graph), before_retry);
        }
        assert_eq!(current(&graph), receipt.generation_uuid);
        assert_views(&graph, branch.branch_uuid, sibling.branch_uuid, 2);
        assert_eq!(
            graph
                .execute_research_branch(&request, &CancellationToken::new())
                .unwrap(),
            receipt
        );
        assert_eq!(current(&graph), receipt.generation_uuid);
        // A real parent write after retry must start from authoritative state.
        graph
            .execute("CREATE (:Person {name:'after retry'})")
            .unwrap();
        assert_eq!(count(&graph), 3);
        drop(graph);
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(count(&graph), 3);
        assert_eq!(
            count(
                graph
                    .open_research_branch(branch.branch_uuid)
                    .unwrap()
                    .graph()
            ),
            2
        );
        assert_eq!(
            count(
                graph
                    .open_research_branch(sibling.branch_uuid)
                    .unwrap()
                    .graph()
            ),
            1
        );
        assert_eq!(
            graph.research_version_retention().unwrap().receipts[&request.operation_uuid],
            receipt
        );
    }
}
