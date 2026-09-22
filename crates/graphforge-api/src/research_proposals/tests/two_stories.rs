//! Bounded consumer projection: two story Branches share one Character identity.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer};

#[test]
fn two_story_shared_character_journey_preserves_partial_review_and_continued_work() {
    let directory = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(directory.path().join("stories").to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Ada', age:30, ending:'undecided'})")
        .unwrap();
    let ada = node(&mut graph);
    let mystery = branch(&mut graph);
    let voyage = branch(&mut graph);
    let mystery_version = edit(
        &mut graph,
        mystery,
        "MATCH (n:Character) SET n.age=31, n.ending='detective'",
    );
    let voyage_version = edit(
        &mut graph,
        voyage,
        "MATCH (n:Character) SET n.age=32, n.ending='captain'",
    );
    let comparison = ResearchComparisonRequest {
        left: ResearchComparisonEndpoint::Branch {
            branch_uuid: mystery,
        },
        right: ResearchComparisonEndpoint::Project,
        left_authority: None,
        right_authority: None,
        detail: ResearchComparisonDetail::Changes,
        accepted: vec![],
        max_fields: 40000,
        max_bytes: 64 * 1024 * 1024,
        page_size: 100,
        after: None,
    };
    let changes = graph
        .compare_research(&comparison, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        changes.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        2
    );
    let proposal = submit(
        &mut graph,
        mystery,
        mystery_version,
        ada,
        &["property:age", "property:ending"],
    );
    let review = decision(&graph, proposal.proposal_uuid, |field| {
        if field == "property:age" {
            Accept
        } else {
            Defer
        }
    });
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    let parent = graph
        .execute("MATCH (n:Character) RETURN n.age AS age, n.ending AS ending")
        .unwrap();
    assert_eq!(
        parent.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        31
    );
    assert_eq!(
        parent.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "undecided"
    );
    assert_eq!(
        graph.open_research_branch(voyage).unwrap().version_uuid(),
        voyage_version
    );
    let continued = edit(&mut graph, mystery, "MATCH (n:Character) SET n.age=33");
    assert_ne!(continued, mystery_version);
    let history = graph
        .research_proposal_history(
            &ResearchProposalHistoryRequest {
                proposal_uuid: proposal.proposal_uuid,
                detail: ResearchProposalHistoryDetail::Items,
                page_size: 100,
                after: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(history.batches[0].num_rows(), 2);
    let changes = graph
        .compare_research(&comparison, &CancellationToken::new())
        .unwrap();
    let batch = &changes.batches[0];
    let fields = batch
        .column_by_name("field")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let accepted = batch
        .column_by_name("accepted_source_version_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let row = (0..batch.num_rows())
        .find(|r| fields.value(*r) == "property:age")
        .unwrap();
    assert_eq!(
        Uuid::from_slice(accepted.value(row)).unwrap(),
        mystery_version
    );
    let next = submit(&mut graph, mystery, continued, ada, &["property:age"]);
    let review = decision(&graph, next.proposal_uuid, |_| Accept);
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .accepted
            .len(),
        2,
        "same contribution's changed value is a new exact revision"
    );
}
