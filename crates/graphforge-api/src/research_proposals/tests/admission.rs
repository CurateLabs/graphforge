use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::Accept;
#[test]
fn cancelled_stale_and_incomplete_reviews_leave_authority_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(temp.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let node = node(&mut graph);
    let branch = branch(&mut graph);
    let version = edit(&mut graph, branch, "MATCH (n:Character) SET n.score=1");
    let submission = submit(&mut graph, branch, version, node, &["property:score"]);
    let review = decision(&graph, submission.proposal_uuid, |_| Accept);
    let generation = current(&graph);
    let registry = graph.research_version_retention().unwrap();
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        graph
            .review_research_proposal(&review, &token)
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    let mut incomplete = review.clone();
    incomplete.decisions.clear();
    assert!(
        graph
            .review_research_proposal(&incomplete, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&graph), generation);
    assert_eq!(graph.research_version_retention().unwrap(), registry);
    graph.execute("MATCH (n:Character) SET n.score=2").unwrap();
    let generation = current(&graph);
    assert_eq!(
        graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    assert_eq!(current(&graph), generation);
    let mut changed = submission;
    changed.fields.clear();
    assert_eq!(
        graph
            .submit_research_proposal(&changed, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(current(&graph), generation);
}
