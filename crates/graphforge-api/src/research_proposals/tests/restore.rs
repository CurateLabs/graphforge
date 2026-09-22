use super::*;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchProposalDecision::Accept,
};

#[test]
fn restored_branch_reproposal_cannot_repeat_acceptance_after_cleanup_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let node = node(&mut graph);
    let first = branch(&mut graph);
    let second = branch(&mut graph);
    let accepted_version = edit(&mut graph, first, "MATCH (n:Character) SET n.score=1");
    let proposal = submit(
        &mut graph,
        first,
        accepted_version,
        node,
        &["property:score"],
    );
    let review = decision(&graph, proposal.proposal_uuid, |_| Accept);
    let receipt = graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    edit(&mut graph, first, "MATCH (n:Character) SET n.score=2");
    let unrelated = edit(&mut graph, second, "MATCH (n:Character) SET n.score=73");
    graph.execute("MATCH (n:Character) SET n.score=99").unwrap();
    let restored = Uuid::now_v7();
    graph
        .restore_research_branch(
            &RestoreResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: first,
                source_version_uuid: accepted_version,
                version_uuid: restored,
                created_at: 5,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let reproposal = submit(&mut graph, first, restored, node, &["property:score"]);
    let re_review = decision(&graph, reproposal.proposal_uuid, |_| Accept);
    assert!(
        graph
            .review_research_proposal(&re_review, &CancellationToken::new())
            .unwrap()
            .version_uuid
            .is_none()
    );
    for id in [proposal.proposal_uuid, reproposal.proposal_uuid] {
        graph
            .release_research_proposal(
                &ReleaseResearchProposalRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    proposal_uuid: id,
                },
                &CancellationToken::new(),
            )
            .unwrap();
    }
    let registry = graph.research_version_retention().unwrap();
    graph
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                mutation: ResearchMutation::Compact {
                    versions: registry.heads.values().copied().collect(),
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    drop(registry);
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let generation = current(&graph);
    assert_eq!(
        graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    assert_eq!(current(&graph), generation);
    assert_eq!(
        graph.open_research_branch(second).unwrap().version_uuid(),
        unrelated
    );
    for (branch_uuid, expected_score) in [(first, 1), (second, 73)] {
        let branch = graph.open_research_branch(branch_uuid).unwrap();
        let values = branch
            .graph()
            .execute("MATCH (n:Character) RETURN n.score AS score")
            .unwrap();
        assert_eq!(values.batches[0].num_rows(), 1);
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected_score
        );
    }
    let reproposal = submit(&mut graph, first, restored, node, &["property:score"]);
    let re_review = decision(&graph, reproposal.proposal_uuid, |_| Accept);
    assert!(
        graph
            .review_research_proposal(&re_review, &CancellationToken::new())
            .unwrap()
            .version_uuid
            .is_none()
    );
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .accepted
            .len(),
        1
    );
    let values = graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        values.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        99
    );
}
