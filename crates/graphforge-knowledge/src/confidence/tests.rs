use super::*;
use crate::tests::uuid7;

#[test]
fn explicit_confidence_round_trips_and_normalizes_negative_zero() {
    let ledger = ConfidenceLedger::explicit(uuid7(10), uuid7(1), -0.0, uuid7(2), 20).unwrap();
    assert_eq!(
        ledger.assessments[0].value.unwrap().to_bits(),
        0.0f64.to_bits()
    );
    assert!(ledger.inputs.is_empty());
    let assessments = ledger.assessment_batch().unwrap();
    let inputs = ledger.input_batch().unwrap();
    assert_eq!(
        ConfidenceLedger::from_batches(&[assessments], &[inputs]).unwrap(),
        ledger
    );
}

#[test]
fn conservative_min_is_uuid_normalized_and_snapshots_missing_values() {
    let first = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 10).unwrap();
    let second = ConfidenceLedger::explicit(uuid7(11), uuid7(1), 0.3, uuid7(3), 11).unwrap();
    let existing = first.merge(&second).unwrap();
    let staged = existing
        .conservative_min(
            uuid7(20),
            uuid7(1),
            vec![uuid7(12), uuid7(11), uuid7(10)],
            uuid7(4),
            20,
        )
        .unwrap();
    assert_eq!(staged.assessments[0].value, None);
    assert_eq!(
        staged
            .inputs
            .iter()
            .map(|row| row.input_confidence_uuid)
            .collect::<Vec<_>>(),
        vec![uuid7(10), uuid7(11), uuid7(12)]
    );
    assert_eq!(
        staged
            .inputs
            .iter()
            .map(|row| row.input_value)
            .collect::<Vec<_>>(),
        vec![Some(0.8), Some(0.3), None]
    );

    let complete = existing
        .conservative_min(
            uuid7(21),
            uuid7(1),
            vec![uuid7(11), uuid7(10)],
            uuid7(5),
            21,
        )
        .unwrap();
    assert_eq!(complete.assessments[0].value, Some(0.3));
    assert_eq!(
        staged.assessment_fingerprint(uuid7(20)).unwrap(),
        existing
            .conservative_min(
                uuid7(20),
                uuid7(1),
                vec![uuid7(10), uuid7(12), uuid7(11)],
                uuid7(4),
                20,
            )
            .unwrap()
            .assessment_fingerprint(uuid7(20))
            .unwrap()
    );
    let encoded = staged
        .assessment_fingerprint(uuid7(20))
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    // Locks `graphforge/confidence-assessment` plus GFCA framing.
    assert_eq!(
        encoded,
        "052104e7e9573852f29255e5f4d7942bd0f409a00b8cda80aba2d09112e40bbb"
    );
}

#[test]
fn confidence_idempotency_and_validation_are_fail_closed() {
    for invalid_value in [f64::NAN, f64::INFINITY, -0.01, 1.01] {
        assert!(matches!(
            ConfidenceLedger::explicit(uuid7(10), uuid7(1), invalid_value, uuid7(2), 20),
            Err(KnowledgeError::Invalid { field: "value", .. })
        ));
    }
    let existing = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 20).unwrap();
    assert_eq!(
        existing.merge(&existing).unwrap().assessments.len(),
        existing.assessments.len()
    );
    let conflict = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.7, uuid7(2), 20).unwrap();
    assert!(matches!(
        existing.merge(&conflict),
        Err(KnowledgeError::Conflict("confidence_uuid"))
    ));
    assert!(matches!(
        existing.conservative_min(
            uuid7(20),
            uuid7(1),
            vec![uuid7(10), uuid7(10)],
            uuid7(2),
            20,
        ),
        Err(KnowledgeError::Duplicate("input_confidence_uuid"))
    ));
}

#[test]
fn confidence_arrow_decode_rejects_schema_and_closed_policy_drift() {
    let ledger = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 20).unwrap();
    let batch = ledger.assessment_batch().unwrap();
    let bad = RecordBatch::try_new(
        Arc::clone(&CONFIDENCE_ASSESSMENT_SCHEMA),
        vec![
            Arc::clone(batch.column(0)),
            Arc::clone(batch.column(1)),
            Arc::new(StringArray::from(vec!["average"])),
            Arc::clone(batch.column(3)),
            Arc::clone(batch.column(4)),
            Arc::clone(batch.column(5)),
            Arc::clone(batch.column(6)),
            Arc::clone(batch.column(7)),
        ],
    )
    .unwrap();
    assert!(matches!(
        ConfidenceLedger::from_batches(&[bad], &[]),
        Err(KnowledgeError::Invalid {
            field: "policy",
            ..
        })
    ));
}
