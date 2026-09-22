use super::*;
use crate::{
    Assertion, AssertionLedger, AssertionSupersession, AssertionSupersessionLedger, KnowledgeError,
};
use uuid::Uuid;

fn claim() -> (Assertion, ResearchClaimRecord) {
    let id = Uuid::now_v7();
    let provenance = Uuid::now_v7();
    (
        Assertion::new(
            id,
            "Interpretation of the same evidence".into(),
            provenance,
            10,
        )
        .unwrap(),
        ResearchClaimRecord {
            assertion_uuid: id,
            conceptual_uuid: id,
            category: ResearchCategory::Interpretation,
            creator_uuid: Uuid::now_v7(),
            run_uuid: None,
            origin_branch_uuid: None,
            origin_version_uuid: None,
            provenance_uuid: provenance,
            recorded_at: 10,
        },
    )
}
fn relation(source: Uuid, target: Uuid, kind: ClaimRelationKind) -> ClaimRelationRecord {
    ClaimRelationRecord {
        relation_uuid: Uuid::now_v7(),
        source_assertion_uuid: source,
        target_assertion_uuid: target,
        kind,
        creator_uuid: Uuid::now_v7(),
        provenance_uuid: Uuid::now_v7(),
        recorded_at: 20,
    }
}
#[test]
fn alternatives_coexist_without_rewriting_assertions_or_choosing_a_winner() {
    let (a, ca) = claim();
    let (b, cb) = claim();
    let assertions = assertions(vec![a.clone(), b.clone()]);
    let before = assertions.clone();
    let alternatives = relation(
        a.assertion_uuid,
        b.assertion_uuid,
        ClaimRelationKind::AlternativeTo,
    );
    let ledger = ResearchClaimLedger::new(vec![ca, cb], vec![alternatives]).unwrap();
    ledger
        .validate_references(&assertions, &AssertionSupersessionLedger::default())
        .unwrap();
    assert_eq!(assertions, before);
    assert_eq!(ledger.claims().len(), 2);
    assert_eq!(ledger.relations().len(), 1);
    assert_eq!(ledger.merge(&ledger).unwrap(), ledger);
    let mut changed = ledger.claims()[0].clone();
    changed.category = ResearchCategory::Theory;
    let staged = ResearchClaimLedger::new(vec![changed], vec![]).unwrap();
    assert!(matches!(
        ledger.merge(&staged),
        Err(KnowledgeError::Conflict(_))
    ));
}
#[test]
fn revision_preserves_concept_and_requires_existing_supersession_owner() {
    let (a, ca) = claim();
    let (b, mut cb) = claim();
    let assertions = assertions(vec![a.clone(), b.clone()]);
    let successor = relation(
        b.assertion_uuid,
        a.assertion_uuid,
        ClaimRelationKind::Supersedes,
    );
    let ledger =
        ResearchClaimLedger::new(vec![ca.clone(), cb.clone()], vec![successor.clone()]).unwrap();
    assert!(
        ledger
            .validate_references(&assertions, &AssertionSupersessionLedger::default())
            .is_err()
    );
    let owner = AssertionSupersessionLedger::new(vec![
        AssertionSupersession::new(
            Uuid::now_v7(),
            a.assertion_uuid,
            b.assertion_uuid,
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            20,
        )
        .unwrap(),
    ])
    .unwrap();
    assert!(ledger.validate_references(&assertions, &owner).is_err());
    cb.conceptual_uuid = ca.conceptual_uuid;
    let valid = ResearchClaimLedger::new(vec![ca, cb.clone()], vec![successor]).unwrap();
    valid.validate_references(&assertions, &owner).unwrap();
    // A legacy unclassified prior still anchors its successor's conceptual identity.
    ResearchClaimLedger::new(vec![cb], vec![])
        .unwrap()
        .validate_references(&assertions, &owner)
        .unwrap();
    assert_eq!(assertions.assertions.len(), 2);
}
#[test]
fn missing_producer_and_forged_origin_fail_closed_but_legacy_is_unclassified() {
    let (a, mut row) = claim();
    let assertions = assertions(vec![a]);
    ResearchClaimLedger::default()
        .validate_references(&assertions, &AssertionSupersessionLedger::default())
        .unwrap();
    row.category = ResearchCategory::MachineExtraction;
    assert!(ResearchClaimLedger::new(vec![row.clone()], vec![]).is_err());
    row.run_uuid = Some(Uuid::now_v7());
    row.provenance_uuid = Uuid::now_v7();
    assert!(
        ResearchClaimLedger::new(vec![row], vec![])
            .unwrap()
            .validate_references(&assertions, &AssertionSupersessionLedger::default())
            .is_err()
    );
}

fn assertions(rows: Vec<Assertion>) -> AssertionLedger {
    let shared_subject = Uuid::now_v7();
    let refs = rows
        .iter()
        .map(|row| {
            crate::AssertionGraphRef::new(
                row.assertion_uuid,
                shared_subject,
                crate::GraphObjectKind::Node,
                crate::AssertionGraphRole::Subject,
                0,
            )
            .unwrap()
        })
        .collect();
    AssertionLedger::new(rows, refs).unwrap()
}

#[test]
fn typed_arrow_round_trip_preserves_optional_origin_and_closed_categories() {
    let (_, mut first) = claim();
    let (_, second) = claim();
    first.category = ResearchCategory::MachineExtraction;
    first.run_uuid = Some(Uuid::now_v7());
    first.origin_branch_uuid = Some(Uuid::now_v7());
    first.origin_version_uuid = Some(Uuid::now_v7());
    let relation = relation(
        first.assertion_uuid,
        second.assertion_uuid,
        ClaimRelationKind::Disputes,
    );
    let ledger = ResearchClaimLedger::new(vec![first, second], vec![relation]).unwrap();
    let batch = ledger.claim_batch().unwrap();
    assert_eq!(
        ResearchClaimLedger::from_batches(
            std::slice::from_ref(&batch),
            &[ledger.relation_batch().unwrap()]
        )
        .unwrap(),
        ledger
    );
    let mut columns = batch.columns().to_vec();
    let category = batch.schema().index_of("category").unwrap();
    columns[category] = std::sync::Arc::new(arrow::array::StringArray::from(vec![
        "invented_truth";
        batch.num_rows()
    ]));
    let corrupt = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).unwrap();
    assert!(ResearchClaimLedger::from_batches(&[corrupt], &[]).is_err());
    let duplicates = [batch.clone(), batch];
    assert!(matches!(
        ResearchClaimLedger::from_batches(&duplicates, &[]),
        Err(KnowledgeError::Duplicate(_))
    ));
}

#[test]
fn decision_arrow_round_trip_preserves_order_and_context() {
    let event = ResearchDecisionRecord {
        sequence: 1,
        decision_uuid: Uuid::now_v7(),
        operation_uuid: Uuid::now_v7(),
        request_sha256: [7; 32],
        authority: ResearchAuthority {
            project_uuid: Uuid::now_v7(),
            community_uuid: Some(Uuid::now_v7()),
            context_uuid: Uuid::now_v7(),
        },
        subject_kind: ResearchSubjectKind::Node,
        subject_uuid: Uuid::now_v7(),
        kind: ResearchDecisionKind::Promote,
        creator_uuid: Uuid::now_v7(),
        source_version_uuid: Some(Uuid::now_v7()),
        recorded_at: -100,
    };
    let ledger = ResearchDecisionLedger::new(vec![event]).unwrap();
    assert_eq!(
        ResearchDecisionLedger::from_batches(&[ledger.batch().unwrap()]).unwrap(),
        ledger
    );
    let empty = ResearchDecisionLedger::default();
    assert_eq!(
        ResearchDecisionLedger::from_batches(&[empty.batch().unwrap()]).unwrap(),
        empty
    );
}

#[test]
fn conceptual_origin_crosses_unclassified_intermediate_revisions() {
    let (a, mut ca) = claim();
    let (b, _) = claim();
    let (c, mut cc) = claim();
    let assertions = assertions(vec![a.clone(), b.clone(), c.clone()]);
    let edges = [
        (a.assertion_uuid, b.assertion_uuid),
        (b.assertion_uuid, c.assertion_uuid),
    ];
    let owner = AssertionSupersessionLedger::new(
        edges
            .into_iter()
            .map(|(prior, next)| {
                AssertionSupersession::new(
                    Uuid::now_v7(),
                    prior,
                    next,
                    Uuid::now_v7(),
                    Uuid::now_v7(),
                    Uuid::now_v7(),
                    20,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    cc.conceptual_uuid = a.assertion_uuid;
    ResearchClaimLedger::new(vec![cc.clone()], vec![])
        .unwrap()
        .validate_references(&assertions, &owner)
        .unwrap();
    cc.conceptual_uuid = b.assertion_uuid;
    assert!(
        ResearchClaimLedger::new(vec![cc.clone()], vec![])
            .unwrap()
            .validate_references(&assertions, &owner)
            .is_err()
    );
    ca.conceptual_uuid = Uuid::now_v7();
    cc.conceptual_uuid = ca.conceptual_uuid;
    ResearchClaimLedger::new(vec![ca.clone(), cc.clone()], vec![])
        .unwrap()
        .validate_references(&assertions, &owner)
        .unwrap();
    cc.conceptual_uuid = b.assertion_uuid;
    assert!(
        ResearchClaimLedger::new(vec![ca, cc], vec![])
            .unwrap()
            .validate_references(&assertions, &owner)
            .is_err()
    );
}
