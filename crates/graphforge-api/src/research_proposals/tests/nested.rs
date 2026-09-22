use super::*;
use graphforge_storage::research_versions::{
    ResearchProposalDecision::Accept, ResearchProposalDestination,
};

#[test]
fn nested_acceptance_deduplicates_per_destination_and_preserves_contribution() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let node = node(&mut graph);
    let parent = branch(&mut graph);
    let child = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: child,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Branch {
                    branch_uuid: parent,
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 2,
                label: "Child".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let source = edit(&mut graph, child, "MATCH (n:Character) SET n.score=1");
    let proposal = submit(&mut graph, child, source, node, &["property:score"]);
    let first = decision(&graph, proposal.proposal_uuid, |_| Accept);
    graph
        .review_research_proposal(&first, &CancellationToken::new())
        .unwrap();
    let parent_version = graph.open_research_branch(parent).unwrap().version_uuid();
    let forwarded = submit(
        &mut graph,
        parent,
        parent_version,
        node,
        &["property:score"],
    );
    let second = decision(&graph, forwarded.proposal_uuid, |_| Accept);
    graph
        .review_research_proposal(&second, &CancellationToken::new())
        .unwrap();
    let registry = graph.research_version_retention().unwrap();
    let mappings: Vec<_> = registry.proposals.accepted.values().collect();
    assert_eq!(mappings.len(), 2);
    assert_eq!(mappings[0].contribution_uuid, mappings[1].contribution_uuid);
    assert_ne!(mappings[0].destination, mappings[1].destination);
    assert!(mappings.iter().any(|m| m.destination
        == ResearchProposalDestination::Branch {
            branch_uuid: parent
        }));
    graph.execute("MATCH (n:Character) SET n.score=99").unwrap();
    let parent_advanced = edit(&mut graph, parent, "MATCH (n:Character) SET n.score=88");
    for (branch, version) in [(child, source), (parent, parent_version)] {
        let reproposal = submit(&mut graph, branch, version, node, &["property:score"]);
        let retry = decision(&graph, reproposal.proposal_uuid, |_| Accept);
        let receipt = graph
            .review_research_proposal(&retry, &CancellationToken::new())
            .unwrap();
        assert!(receipt.version_uuid.is_none());
    }
    assert_eq!(
        graph.open_research_branch(parent).unwrap().version_uuid(),
        parent_advanced
    );
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .accepted
            .len(),
        2
    );
    let result = graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        99
    );
}
