//! Actual Proposal history growth, explicit root release and durable replay.
use super::*;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation,
    ResearchProposalDecision::{Accept, Defer, Reject},
};

fn bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn mutation(
    graph: &mut GraphForge,
    mutation: ResearchMutation,
) -> Result<ResearchOperationReceipt, GfError> {
    graph.commit_research_version_operation(
        ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(graph),
            mutation,
        },
        &CancellationToken::new(),
    )
}

fn score(graph: &GraphForge) -> i64 {
    graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap()
        .batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn fixed_parent_proposal_history_releases_obsolete_payloads_but_preserves_accepted_proof_and_replay()
 {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let node_uuid = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let mut history = Vec::new();
    let mut growth = Vec::new();
    let mut destination = None;
    for value in 1..=4 {
        let source = edit(
            &mut graph,
            branch_uuid,
            &format!("MATCH (n:Character) SET n.score={value}"),
        );
        let submission = submit(
            &mut graph,
            branch_uuid,
            source,
            node_uuid,
            &["property:score"],
        );
        let submission_receipt = graph.research_version_retention().unwrap().receipts
            [&submission.operation_uuid]
            .clone();
        let disposition = match value {
            1 => Accept,
            3 => Defer,
            _ => Reject,
        };
        let review = decision(&graph, submission.proposal_uuid, |_| disposition);
        let review_receipt = graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap();
        let registry = graph.research_version_retention().unwrap();
        let payload = registry.proposals.proposals[&submission.proposal_uuid].payload_version_uuid;
        let accepted = registry.proposals.accepted.values().next().unwrap();
        if value == 1 {
            destination = Some(accepted.destination_version_uuid);
        }
        assert_eq!(Some(accepted.destination_version_uuid), destination);
        assert_eq!(registry.proposals.accepted.len(), 1);
        assert_eq!(
            score(&graph),
            1,
            "later proposals must not advance the fixed parent content"
        );
        growth.push(bytes(&root.join("graph-objects/sha256")));
        history.push((
            submission,
            submission_receipt,
            review,
            review_receipt,
            source,
            payload,
        ));
    }
    assert!(growth.windows(2).all(|pair| pair[0] <= pair[1]));
    let before_refusal = graph.research_version_retention().unwrap();
    let generation = current(&graph);
    assert!(
        mutation(
            &mut graph,
            ResearchMutation::DeleteVersion {
                version_uuid: history[2].5
            }
        )
        .is_err(),
        "deferred proposal root must protect its selected payload"
    );
    assert_eq!(current(&graph), generation);
    assert_eq!(graph.research_version_retention().unwrap(), before_refusal);

    // The final source is no longer a head, so every obsolete submitted source
    // can be released independently of its permanent citation and receipts.
    edit(&mut graph, branch_uuid, "MATCH (n:Character) SET n.score=5");
    let accepted = graph
        .research_version_retention()
        .unwrap()
        .proposals
        .accepted
        .values()
        .next()
        .unwrap()
        .clone();
    for (submission, _, _, _, source, payload) in &history {
        graph
            .release_research_proposal(
                &ReleaseResearchProposalRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    proposal_uuid: submission.proposal_uuid,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        assert!(
            graph
                .preview_research_proposal(
                    &PreviewResearchProposalRequest {
                        proposal_uuid: submission.proposal_uuid,
                    },
                    &CancellationToken::new()
                )
                .is_err()
        );
        mutation(
            &mut graph,
            ResearchMutation::DeleteVersion {
                version_uuid: *payload,
            },
        )
        .unwrap();
        mutation(
            &mut graph,
            ResearchMutation::DeleteVersion {
                version_uuid: *source,
            },
        )
        .unwrap();
    }
    let registry = graph.research_version_retention().unwrap();
    mutation(
        &mut graph,
        ResearchMutation::Compact {
            versions: registry.heads.values().copied().collect(),
        },
    )
    .unwrap();
    let before_cleanup = bytes(&root.join("graph-objects/sha256"));
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        graphforge_storage::ProjectRetentionLimits::default(),
    )
    .unwrap();
    let retained = bytes(&root.join("graph-objects/sha256"));
    eprintln!(
        "proposal_retention_bytes growth={growth:?} before_cleanup={before_cleanup} retained={retained}"
    );
    assert!(
        retained > 0 && retained < before_cleanup,
        "explicit release must reclaim obsolete proposal/source bytes"
    );
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(score(&graph), 1);
    assert_eq!(
        score(graph.open_research_branch(branch_uuid).unwrap().graph()),
        5
    );
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(
        registry.proposals.accepted.values().next().unwrap(),
        &accepted
    );
    assert_eq!(registry.proposals.released.len(), 4);
    let proof = crate::research_versions::materialize_version(
        &graph,
        &registry.versions[&accepted.proof_version_uuid],
    )
    .unwrap();
    assert_eq!(score(&proof), 1);
    drop(proof);
    let generation = current(&graph);
    assert!(
        mutation(
            &mut graph,
            ResearchMutation::DeleteVersion {
                version_uuid: accepted.proof_version_uuid
            }
        )
        .is_err(),
        "accepted proof remains independently rooted after frozen proposals are released"
    );
    assert_eq!(current(&graph), generation);
    for (submission, submission_receipt, review, review_receipt, source, payload) in &history {
        assert!(graph.research_version(*source).is_err());
        assert!(graph.research_version(*payload).is_err());
        assert_eq!(
            &graph
                .submit_research_proposal(submission, &CancellationToken::new())
                .unwrap(),
            submission_receipt
        );
        assert_eq!(
            &graph
                .review_research_proposal(review, &CancellationToken::new())
                .unwrap(),
            review_receipt
        );
        assert_eq!(
            current(&graph),
            generation,
            "supported replay cannot recreate released content or republish"
        );
    }
    assert_eq!(score(&graph), 1);
    assert_eq!(
        score(graph.open_research_branch(branch_uuid).unwrap().graph()),
        5
    );
}
