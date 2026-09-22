use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::Accept;

#[test]
fn proposal_publication_fault_helper() {
    let Ok(root) = std::env::var("GF_PROPOSAL_FAULT_ROOT") else {
        return;
    };
    let request: ReviewResearchProposalRequest =
        serde_json::from_slice(&std::fs::read(format!("{root}.review.json")).unwrap()).unwrap();
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let result = graph.review_research_proposal(&request, &CancellationToken::new());
    let post = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT")
        .unwrap()
        .contains("after_current");
    if post {
        // A post-linearization publication error reports committed state; the
        // same owner is reconciled and exact retry exposes the durable receipt.
        let error = result.unwrap_err();
        assert!(error.to_string().contains("committed=true"), "{error}");
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
            1
        );
        let generation = current(&graph);
        graph
            .review_research_proposal(&request, &CancellationToken::new())
            .unwrap();
        assert_eq!(current(&graph), generation);
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .proposals
                .accepted
                .len(),
            1
        );
    } else {
        assert!(result.is_err());
        assert_eq!(current(&graph), request.expected_generation_uuid);
        assert!(
            graph
                .research_version_retention()
                .unwrap()
                .proposals
                .accepted
                .is_empty()
        );
    }
}

#[test]
fn proposal_acceptance_is_atomic_on_both_sides_of_current_and_replays_after_reopen() {
    for phase in ["before", "after"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        graph.execute("CREATE (:Character {score:0})").unwrap();
        let node = node(&mut graph);
        let branch = branch(&mut graph);
        let version = edit(&mut graph, branch, "MATCH (n:Character) SET n.score=1");
        let proposal = submit(&mut graph, branch, version, node, &["property:score"]);
        let request = decision(&graph, proposal.proposal_uuid, |_| Accept);
        std::fs::write(
            format!("{}.review.json", root.display()),
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "research_proposals::tests::recovery::proposal_publication_fault_helper",
                "--nocapture",
            ])
            .env("GF_PROPOSAL_FAULT_ROOT", &root)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                request.operation_uuid.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                format!("project.{phase}_current_replace.error"),
            )
            .status()
            .unwrap();
        assert!(status.success(), "{phase}: {status}");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        let before_retry = current(&graph);
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
            if phase == "after" { 1 } else { 0 }
        );
        let receipt = graph
            .review_research_proposal(&request, &CancellationToken::new())
            .unwrap();
        if phase == "after" {
            assert_eq!(current(&graph), before_retry);
        }
        assert_eq!(
            graph
                .review_research_proposal(&request, &CancellationToken::new())
                .unwrap(),
            receipt
        );
        let generation = current(&graph);
        let mut changed = request;
        changed.explanation.push_str(" changed");
        assert_eq!(
            graph
                .review_research_proposal(&changed, &CancellationToken::new())
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(current(&graph), generation);
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .proposals
                .accepted
                .len(),
            1
        );
    }
}
