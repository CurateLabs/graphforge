//! Later acceptance must not reinterpret immutable historical comparison endpoints.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision;

#[test]
fn historical_comparison_does_not_inherit_later_acceptance_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let node_uuid = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let branch_zero = graph
        .open_research_branch(branch_uuid)
        .unwrap()
        .version_uuid();
    let project_zero = Uuid::now_v7();
    let capture = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: project_zero,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 3,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    let historical = ResearchComparisonRequest {
        left: ResearchComparisonEndpoint::Version {
            version_uuid: branch_zero,
        },
        right: ResearchComparisonEndpoint::Version {
            version_uuid: project_zero,
        },
        left_authority: None,
        right_authority: None,
        detail: ResearchComparisonDetail::Changes,
        accepted: vec![],
        max_fields: 40_000,
        max_bytes: 64 * 1024 * 1024,
        page_size: 1000,
        after: None,
    };
    let before = graph
        .compare_research(&historical, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        before.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0
    );

    let changed_version = edit(&mut graph, branch_uuid, "MATCH (n:Character) SET n.score=1");
    let proposal = submit(
        &mut graph,
        branch_uuid,
        changed_version,
        node_uuid,
        &["property:score"],
    );
    let review = decision(&graph, proposal.proposal_uuid, |_| {
        ResearchProposalDecision::Accept
    });
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    let score = graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        score.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );

    let after = graph
        .compare_research(&historical, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        after.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "future acceptance must not manufacture equivalent/modified rows for identical old Versions"
    );
    let live = ResearchComparisonRequest {
        left: ResearchComparisonEndpoint::Branch { branch_uuid },
        right: ResearchComparisonEndpoint::Project,
        ..historical.clone()
    };
    let accepted = graph
        .compare_research(&live, &CancellationToken::new())
        .unwrap();
    assert!(
        accepted.batches.iter().any(|batch| {
            let changes = batch
                .column_by_name("change")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..batch.num_rows()).any(|row| changes.value(row) == "accepted")
        }),
        "live comparison must still recognize the durable acceptance"
    );

    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let generation = current(&graph);
    let reopened = graph
        .compare_research(&historical, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        reopened.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0
    );
    assert_eq!(
        current(&graph),
        generation,
        "historical comparison remains read-only"
    );
}
