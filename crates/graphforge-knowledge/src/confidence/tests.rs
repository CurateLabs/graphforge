use super::*;
use crate::tests::uuid7;

fn conservative_assessment(id: u8, value: Option<f64>) -> ConfidenceAssessment {
    ConfidenceAssessment::new(
        uuid7(id),
        uuid7(id + 1),
        ConfidencePolicy::ConservativeMin,
        value,
        uuid7(id + 2),
        i64::from(id),
    )
    .unwrap()
}

fn confidence_input(owner: Uuid, id: u8, value: Option<f64>, ordinal: u32) -> ConfidenceInput {
    ConfidenceInput::new(owner, uuid7(id), value, ordinal).unwrap()
}

#[test]
fn explicit_confidence_round_trips_and_normalizes_negative_zero() {
    let ledger = ConfidenceLedger::explicit(uuid7(10), uuid7(1), -0.0, uuid7(2), 20).unwrap();
    assert_eq!(
        ledger.assessments()[0].value.unwrap().to_bits(),
        0.0f64.to_bits()
    );
    assert!(ledger.inputs().is_empty());
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
    assert_eq!(staged.assessments()[0].value, None);
    assert_eq!(
        staged
            .inputs()
            .iter()
            .map(|row| row.input_confidence_uuid)
            .collect::<Vec<_>>(),
        vec![uuid7(10), uuid7(11), uuid7(12)]
    );
    assert_eq!(
        staged
            .inputs()
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
    assert_eq!(complete.assessments()[0].value, Some(0.3));
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
        existing.merge(&existing).unwrap().assessments().len(),
        existing.assessments().len()
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
fn confidence_merge_does_not_revalidate_validated_rows() {
    let existing = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 20).unwrap();
    let staged = ConfidenceLedger::explicit(uuid7(11), uuid7(1), 0.3, uuid7(3), 21).unwrap();

    reset_confidence_row_validation_count();
    let merged = existing.merge(&staged).unwrap();

    assert_eq!(confidence_row_validation_count(), 0);
    assert_eq!(merged.assessments().len(), 2);
    assert_eq!(merged.assessments()[0].confidence_uuid, uuid7(10));
    assert_eq!(merged.assessments()[1].confidence_uuid, uuid7(11));
}

#[test]
fn confidence_merge_enforces_combined_row_limits_without_full_validation() {
    let existing = ConfidenceLedger::new(
        vec![
            ConfidenceAssessment::new(
                uuid7(10),
                uuid7(1),
                ConfidencePolicy::Explicit,
                Some(0.8),
                uuid7(2),
                20,
            )
            .unwrap(),
            ConfidenceAssessment::new(
                uuid7(11),
                uuid7(1),
                ConfidencePolicy::Explicit,
                Some(0.3),
                uuid7(3),
                21,
            )
            .unwrap(),
        ],
        vec![],
    )
    .unwrap();
    let staged = ConfidenceLedger::explicit(uuid7(12), uuid7(1), 0.6, uuid7(4), 22).unwrap();

    assert!(matches!(
        existing.merge_with_limits(&staged, 2, crate::MAX_KNOWLEDGE_ROWS),
        Err(KnowledgeError::Limit {
            participant: "confidence_assessments",
            observed: 3,
            limit: 2,
        })
    ));
}

#[test]
fn confidence_ledger_validates_each_assessment_row_invariant() {
    let valid = ConfidenceAssessment::new(
        uuid7(30),
        uuid7(31),
        ConfidencePolicy::Explicit,
        Some(0.5),
        uuid7(32),
        33,
    )
    .unwrap();
    let mut invalid = valid.clone();
    invalid.confidence_uuid = uuid7(40);
    invalid.contract_version += 1;
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "confidence.contract_version",
            ..
        })
    ));

    invalid = valid.clone();
    invalid.confidence_uuid = Uuid::nil();
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "confidence_uuid",
            ..
        })
    ));
    invalid = valid.clone();
    invalid.confidence_uuid = non_v7_uuid(40);
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "confidence_uuid",
            message: "must be UUIDv7",
        })
    ));

    invalid = valid.clone();
    invalid.assertion_uuid = non_v7_uuid(41);
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "assertion_uuid",
            message: "must be UUIDv7",
        })
    ));
    invalid = valid.clone();
    invalid.provenance_uuid = Uuid::nil();
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "provenance_uuid",
            ..
        })
    ));
    invalid = valid.clone();
    for invalid_value in [f64::NAN, 1.5] {
        invalid.value = Some(invalid_value);
        assert!(matches!(
            ConfidenceLedger::new(vec![invalid.clone()], vec![]),
            Err(KnowledgeError::Invalid { field: "value", .. })
        ));
    }
    invalid = valid.clone();
    invalid.policy_version += 1;
    assert!(matches!(
        ConfidenceLedger::new(vec![invalid], vec![]),
        Err(KnowledgeError::Invalid {
            field: "policy_version",
            ..
        })
    ));
    assert!(matches!(
        ConfidenceLedger::new(vec![valid.clone(), valid], vec![]),
        Err(KnowledgeError::Duplicate("confidence_uuid"))
    ));
}

#[test]
fn confidence_ledger_validates_each_input_row_and_cross_row_invariant() {
    let assessment = conservative_assessment(30, Some(0.5));
    let owner = assessment.confidence_uuid;
    let input = confidence_input(owner, 40, Some(0.5), 0);
    let invalid_error = |row: ConfidenceInput| {
        ConfidenceLedger::new(vec![assessment.clone()], vec![row]).unwrap_err()
    };

    let mut invalid = input.clone();
    invalid.confidence_uuid = non_v7_uuid(41);
    assert!(matches!(
        invalid_error(invalid),
        KnowledgeError::Invalid {
            field: "confidence_uuid",
            message: "must be UUIDv7",
        }
    ));
    invalid = input.clone();
    invalid.input_confidence_uuid = non_v7_uuid(42);
    assert!(matches!(
        invalid_error(invalid),
        KnowledgeError::Invalid {
            field: "input_confidence_uuid",
            message: "must be UUIDv7",
        }
    ));
    invalid = input.clone();
    for invalid_value in [f64::INFINITY, 1.5] {
        invalid.input_value = Some(invalid_value);
        assert!(matches!(
            invalid_error(invalid.clone()),
            KnowledgeError::Invalid {
                field: "input_value",
                ..
            }
        ));
    }
    invalid = input.clone();
    invalid.contract_version += 1;
    assert!(matches!(
        invalid_error(invalid),
        KnowledgeError::Invalid {
            field: "confidence_input.contract_version",
            ..
        }
    ));
    invalid = input.clone();
    invalid.confidence_uuid = uuid7(60);
    assert!(matches!(
        invalid_error(invalid),
        KnowledgeError::Dangling("confidence_uuid")
    ));
    assert!(matches!(
        ConfidenceLedger::new(vec![assessment.clone()], vec![input.clone(), input.clone()]),
        Err(KnowledgeError::Duplicate("input_confidence_uuid"))
    ));

    let gap = ConfidenceInput::new(owner, uuid7(41), Some(0.5), 1).unwrap();
    assert!(matches!(
        ConfidenceLedger::new(vec![assessment.clone()], vec![gap]),
        Err(KnowledgeError::Invalid {
            field: "ordinal",
            ..
        })
    ));

    let reversed = vec![
        ConfidenceInput::new(owner, uuid7(42), Some(0.5), 0).unwrap(),
        ConfidenceInput::new(owner, uuid7(41), Some(0.5), 1).unwrap(),
    ];
    let multi = conservative_assessment(30, Some(0.5));
    assert!(matches!(
        ConfidenceLedger::new(vec![multi], reversed),
        Err(KnowledgeError::Invalid {
            field: "input_confidence_uuid",
            ..
        })
    ));
}

#[test]
fn confidence_ledger_enforces_policy_input_snapshots() {
    let explicit = ConfidenceAssessment::new(
        uuid7(30),
        uuid7(31),
        ConfidencePolicy::Explicit,
        Some(0.5),
        uuid7(32),
        33,
    )
    .unwrap();
    let explicit_input = confidence_input(explicit.confidence_uuid, 40, Some(0.5), 0);
    assert!(matches!(
        ConfidenceLedger::new(vec![explicit], vec![explicit_input]),
        Err(KnowledgeError::Invalid {
            field: "confidence_inputs",
            ..
        })
    ));

    let missing_explicit = ConfidenceAssessment::new(
        uuid7(50),
        uuid7(51),
        ConfidencePolicy::Explicit,
        None,
        uuid7(52),
        53,
    )
    .unwrap();
    assert!(matches!(
        ConfidenceLedger::new(vec![missing_explicit], vec![]),
        Err(KnowledgeError::Invalid { field: "value", .. })
    ));

    let mismatch = conservative_assessment(60, Some(0.8));
    let mismatch_input = confidence_input(mismatch.confidence_uuid, 70, Some(0.5), 0);
    assert!(matches!(
        ConfidenceLedger::new(vec![mismatch], vec![mismatch_input]),
        Err(KnowledgeError::Invalid { field: "value", .. })
    ));
    assert!(ConfidenceLedger::new(vec![conservative_assessment(80, None)], vec![]).is_ok());
}

#[test]
fn confidence_ledger_row_limit_checks_cover_both_participants() {
    let assessment = ConfidenceAssessment::new(
        uuid7(10),
        uuid7(11),
        ConfidencePolicy::Explicit,
        Some(0.5),
        uuid7(12),
        13,
    )
    .unwrap();
    let input = ConfidenceInput::new(uuid7(20), uuid7(21), Some(0.5), 0).unwrap();
    assert!(validate_confidence_rows_with_limits(&[assessment.clone()], &[], 1, 1).is_ok());
    assert!(matches!(
        validate_confidence_rows_with_limits(&[assessment], &[], 0, 1),
        Err(KnowledgeError::Limit {
            participant: "confidence_assessments",
            observed: 1,
            limit: 0,
        })
    ));
    assert!(matches!(
        validate_confidence_rows_with_limits(&[], &[input], 1, 0),
        Err(KnowledgeError::Limit {
            participant: "confidence_inputs",
            observed: 1,
            limit: 0,
        })
    ));
}

#[test]
fn confidence_merge_enforces_combined_input_limit() {
    let existing_assessment = conservative_assessment(10, Some(0.8));
    let existing = ConfidenceLedger::new(
        vec![existing_assessment.clone()],
        vec![confidence_input(
            existing_assessment.confidence_uuid,
            20,
            Some(0.8),
            0,
        )],
    )
    .unwrap();
    let staged_assessment = conservative_assessment(30, Some(0.3));
    let staged = ConfidenceLedger::new(
        vec![staged_assessment.clone()],
        vec![confidence_input(
            staged_assessment.confidence_uuid,
            40,
            Some(0.3),
            0,
        )],
    )
    .unwrap();

    assert!(matches!(
        existing.merge_with_limits(&staged, 2, 1),
        Err(KnowledgeError::Limit {
            participant: "confidence_inputs",
            observed: 2,
            limit: 1,
        })
    ));
}

fn non_v7_uuid(seed: u8) -> Uuid {
    let mut bytes = [seed; 16];
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
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

fn measurement_uuid(seed: u64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    bytes[6] = 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn merge_with_full_revalidation(
    existing: &ConfidenceLedger,
    staged: &ConfidenceLedger,
) -> Result<ConfidenceLedger, KnowledgeError> {
    let mut assessments = existing.assessments.clone();
    let mut inputs = existing.inputs.clone();
    for row in &staged.assessments {
        if let Some(previous) = assessments
            .iter()
            .find(|previous| previous.confidence_uuid == row.confidence_uuid)
        {
            if previous != row
                || inputs_for(&inputs, row.confidence_uuid)
                    != inputs_for(&staged.inputs, row.confidence_uuid)
            {
                return Err(KnowledgeError::Conflict("confidence_uuid"));
            }
        } else {
            assessments.push(row.clone());
            inputs.extend(
                staged
                    .inputs
                    .iter()
                    .filter(|input| input.confidence_uuid == row.confidence_uuid)
                    .cloned(),
            );
        }
    }
    ConfidenceLedger::new(assessments, inputs)
}

fn confidence_measurement_digest(existing: &ConfidenceLedger, staged: &ConfidenceLedger) -> String {
    let mut writer = graphforge_core::canonical::CanonicalWriter::new();
    for ledger in [existing, staged] {
        writer.u64(ledger.assessments.len() as u64).unwrap();
        for row in &ledger.assessments {
            writer.raw(row.confidence_uuid.as_bytes()).unwrap();
            writer.raw(row.assertion_uuid.as_bytes()).unwrap();
            writer.text(row.policy.as_str()).unwrap();
            writer.u32(row.policy_version).unwrap();
            canonical_optional_f64(&mut writer, row.value).unwrap();
            writer.raw(row.provenance_uuid.as_bytes()).unwrap();
            writer.i64(row.recorded_at_micros).unwrap();
            writer.u32(row.contract_version).unwrap();
        }
        writer.u64(ledger.inputs.len() as u64).unwrap();
        for row in &ledger.inputs {
            writer.raw(row.confidence_uuid.as_bytes()).unwrap();
            writer.raw(row.input_confidence_uuid.as_bytes()).unwrap();
            canonical_optional_f64(&mut writer, row.input_value).unwrap();
            writer.u32(row.ordinal).unwrap();
            writer.u32(row.contract_version).unwrap();
        }
    }
    let digest = fingerprint(
        CanonicalDomain::ConfidenceAssessment,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .unwrap();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn median_duration(mut durations: Vec<std::time::Duration>) -> std::time::Duration {
    durations.sort_unstable();
    durations[durations.len() / 2]
}

#[test]
// Reproduce on an otherwise idle host:
// source /home/ubuntu/.claude/gf-quiet-host.sh && require_quiet_host && \
// CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1825-measurement cargo test --release --locked -p graphforge-knowledge confidence::tests::quiet_host_confidence_merge_cost_measurement -- --ignored --nocapture --test-threads=1
#[ignore = "manual quiet-host before/after merge-cost measurement"]
fn quiet_host_confidence_merge_cost_measurement() {
    use std::time::Instant;

    for existing_count in [10_000_u64, 100_000] {
        let existing = ConfidenceLedger::new(
            (0..existing_count)
                .map(|seed| {
                    ConfidenceAssessment::new(
                        measurement_uuid(seed + 1),
                        measurement_uuid(4_000_000),
                        ConfidencePolicy::ConservativeMin,
                        Some(0.5),
                        measurement_uuid(seed + 2_000_000),
                        i64::try_from(seed).unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            (0..existing_count)
                .map(|seed| {
                    ConfidenceInput::new(
                        measurement_uuid(seed + 1),
                        measurement_uuid(seed + 3_000_000),
                        Some(0.5),
                        0,
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap();
        let staged = ConfidenceLedger::new(
            vec![
                ConfidenceAssessment::new(
                    measurement_uuid(existing_count + 1),
                    measurement_uuid(4_000_000),
                    ConfidencePolicy::ConservativeMin,
                    Some(0.5),
                    measurement_uuid(existing_count + 2_000_000),
                    i64::try_from(existing_count).unwrap(),
                )
                .unwrap(),
            ],
            vec![
                ConfidenceInput::new(
                    measurement_uuid(existing_count + 1),
                    measurement_uuid(existing_count + 3_000_000),
                    Some(0.5),
                    0,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let input_digest = confidence_measurement_digest(&existing, &staged);
        assert_eq!(
            merge_with_full_revalidation(&existing, &staged).unwrap(),
            existing.merge(&staged).unwrap()
        );

        let repetitions = 7;
        let mut baseline = Vec::with_capacity(repetitions);
        let mut optimized = Vec::with_capacity(repetitions);
        for repetition in 0..repetitions {
            if repetition % 2 == 0 {
                let start = Instant::now();
                std::hint::black_box(merge_with_full_revalidation(&existing, &staged).unwrap());
                baseline.push(start.elapsed());

                let start = Instant::now();
                std::hint::black_box(existing.merge(&staged).unwrap());
                optimized.push(start.elapsed());
            } else {
                let start = Instant::now();
                std::hint::black_box(existing.merge(&staged).unwrap());
                optimized.push(start.elapsed());

                let start = Instant::now();
                std::hint::black_box(merge_with_full_revalidation(&existing, &staged).unwrap());
                baseline.push(start.elapsed());
            }
        }
        let baseline_median = median_duration(baseline);
        let optimized_median = median_duration(optimized);
        let divisor = existing_count as f64;
        eprintln!(
            "existing_assessments={existing_count} existing_inputs={existing_count} staged_assessments=1 staged_inputs=1 repetitions={repetitions} input_sha256={input_digest} baseline_ns={} baseline_ns_per_existing_assessment={:.3} optimized_ns={} optimized_ns_per_existing_assessment={:.3}",
            baseline_median.as_nanos(),
            baseline_median.as_nanos() as f64 / divisor,
            optimized_median.as_nanos(),
            optimized_median.as_nanos() as f64 / divisor,
        );
    }
}
