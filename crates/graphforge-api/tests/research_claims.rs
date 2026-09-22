//! Native contextual authority must survive reopen without filtering the raw graph.
use arrow::array::{FixedSizeBinaryArray, Int64Array};
use graphforge_api::*;
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use uuid::Uuid;
fn current(g: &GraphForge) -> Uuid {
    g.research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn subject(g: &GraphForge) -> Uuid {
    let rows = g
        .execute("MATCH (n:ClaimSubject) RETURN n.node_uuid AS id")
        .unwrap();
    Uuid::from_slice(
        rows.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}
fn count(g: &GraphForge) -> i64 {
    g.execute("MATCH (n) RETURN count(n)").unwrap().batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn request(
    g: &GraphForge,
    context: ResearchContext,
    kind: ResearchDecisionKind,
) -> RecordResearchDecisionsRequest {
    RecordResearchDecisionsRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        context,
        community_uuid: None,
        creator_uuid: Uuid::now_v7(),
        recorded_at: 10,
        decisions: vec![ResearchDecisionInput {
            decision_uuid: Uuid::now_v7(),
            subject_kind: ResearchSubjectKind::Node,
            subject_uuid: subject(g),
            kind,
            source_version_uuid: None,
        }],
    }
}
#[test]
fn canonical_parent_and_branch_decisions_are_explicit_independent_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut g = GraphForge::new(root.to_str()).unwrap();
    g.execute("CREATE (:ClaimSubject {name:'shared evidence'})")
        .unwrap();
    let promote = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    g.record_research_decisions(&promote, &CancellationToken::new())
        .unwrap();
    let published = current(&g);
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.record_research_decisions(&promote, &CancellationToken::new())
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(current(&g), published);
    assert_eq!(count(&g), 1);
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 11,
        label: "alternative interpretation".into(),
    };
    g.create_research_branch(&branch, &CancellationToken::new())
        .unwrap();
    let context = ResearchContext::Branch {
        branch_uuid: branch.branch_uuid,
    };
    let integrate = request(&g, context.clone(), ResearchDecisionKind::Integrate);
    g.record_research_decisions(&integrate, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    let accepted = request(&g, context.clone(), ResearchDecisionKind::Promote);
    g.record_research_decisions(&accepted, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.research_canonical_choices(&context, Some(Uuid::now_v7()))
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        count(g.open_research_branch(branch.branch_uuid).unwrap().graph()),
        1
    );
    drop(g);
    let mut g = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        g.research_decision_history(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );
    let revoke = request(&g, context.clone(), ResearchDecisionKind::Revoke);
    g.record_research_decisions(&revoke, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        g.research_decision_history(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3
    );
    assert_eq!(
        g.record_research_decisions(&integrate, &CancellationToken::new())
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(count(&g), 1);
}
#[test]
fn cancellation_missing_subject_and_changed_retry_leave_current_unchanged() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    let mut write = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    let before = current(&g);
    let token = CancellationToken::new();
    token.cancel();
    assert!(g.record_research_decisions(&write, &token).is_err());
    assert_eq!(current(&g), before);
    let subject = write.decisions[0].subject_uuid;
    write.decisions[0].subject_uuid = Uuid::now_v7();
    assert!(
        g.record_research_decisions(&write, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), before);
    write.decisions[0].subject_uuid = subject;
    g.record_research_decisions(&write, &CancellationToken::new())
        .unwrap();
    let published = current(&g);
    write.decisions[0].kind = ResearchDecisionKind::Revoke;
    assert!(
        g.record_research_decisions(&write, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), published);
}
