use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer, Reject};

#[test]
fn partial_review_retains_only_accepted_fields_and_deferral_can_be_reviewed_later() {
    let temp = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(temp.path().join("project").to_str()).unwrap();
    graph
        .execute("CREATE (:Character {score:0, deferred:0, rejected:0, private_note:'private'})")
        .unwrap();
    let node = node(&mut graph);
    let branch = branch(&mut graph);
    let version = edit(
        &mut graph,
        branch,
        "MATCH (n:Character) SET n.score=1, n.deferred=2, n.rejected=3",
    );
    let proposal = submit(
        &mut graph,
        branch,
        version,
        node,
        &["property:score", "property:deferred", "property:rejected"],
    );
    let request = decision(&graph, proposal.proposal_uuid, |field| match field {
        "property:score" => Accept,
        "property:deferred" => Defer,
        _ => Reject,
    });
    graph
        .review_research_proposal(&request, &CancellationToken::new())
        .unwrap();
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.proposals.accepted.len(), 1);
    let mapping = registry.proposals.accepted.values().next().unwrap();
    let proof = crate::research_versions::materialize_version(
        &graph,
        &registry.versions[&mapping.proof_version_uuid],
    )
    .unwrap();
    let fields = crate::branches::fields::read(&proof, &CancellationToken::new()).unwrap();
    for field in [
        "property:deferred",
        "property:rejected",
        "property:private_note",
    ] {
        assert!(!fields.contains_key(&("node".into(), node, field.into())));
    }
    let values = graph.execute("MATCH (n:Character) RETURN n.score AS score, n.deferred AS deferred, n.rejected AS rejected").unwrap();
    for (field, expected) in [("score", 1), ("deferred", 0), ("rejected", 0)] {
        assert_eq!(
            values.batches[0]
                .column_by_name(field)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    let second = decision(&graph, proposal.proposal_uuid, |field| {
        if field == "property:rejected" {
            Reject
        } else {
            Accept
        }
    });
    graph
        .review_research_proposal(&second, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .accepted
            .len(),
        2
    );
    let values = graph
        .execute("MATCH (n:Character) RETURN n.deferred AS deferred, n.rejected AS rejected")
        .unwrap();
    for (field, expected) in [("deferred", 2), ("rejected", 0)] {
        assert_eq!(
            values.batches[0]
                .column_by_name(field)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
}
