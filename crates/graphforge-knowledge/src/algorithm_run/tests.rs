use super::*;
use crate::{ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT, ALGORITHM_RUN_SCHEMA_FINGERPRINT};
use graphforge_core::canonical::CANONICAL_CONTRACT_VERSION;
use graphforge_core::canonical::CanonicalDomain;
use graphforge_core::canonical::CanonicalWriter;
use graphforge_core::canonical::fingerprint;

fn uuid7(seed: u128) -> Uuid {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn uuid4(seed: u128) -> Uuid {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn run(seed: u128, started_at_micros: i64) -> AlgorithmRun {
    AlgorithmRun::new(
        uuid7(seed),
        "rank.degree".into(),
        1,
        1,
        b"canonical descriptor".to_vec(),
        [7; 32],
        uuid7(seed + 1),
        started_at_micros,
    )
    .unwrap()
}

fn started(seed: u128, run: &AlgorithmRun) -> AlgorithmRunEvent {
    AlgorithmRunEvent::new(
        uuid7(seed),
        run.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        run.started_at_micros,
        run.provenance_uuid,
    )
    .unwrap()
}

fn completed(seed: u128, run: &AlgorithmRun, recorded_at_micros: i64) -> AlgorithmRunEvent {
    AlgorithmRunEvent::new(
        uuid7(seed),
        run.run_uuid,
        AlgorithmRunState::Completed,
        Some([8; 32]),
        None,
        recorded_at_micros,
        uuid7(seed + 1),
    )
    .unwrap()
}

fn assert_invalid_run(row: &AlgorithmRun) {
    assert!(matches!(
        validate_algorithm_run(row),
        Err(KnowledgeError::Invalid { .. })
    ));
}

fn assert_invalid_event(row: &AlgorithmRunEvent) {
    assert!(matches!(
        validate_algorithm_run_event(row),
        Err(KnowledgeError::Invalid { .. })
    ));
}

fn assert_invalid_ledger(runs: Vec<AlgorithmRun>, events: Vec<AlgorithmRunEvent>) {
    assert!(matches!(
        AlgorithmRunLedger::new(runs, events),
        Err(KnowledgeError::Invalid { .. })
    ));
}

fn full_validation_merge(
    existing: &AlgorithmRunLedger,
    staged: &AlgorithmRunLedger,
) -> Result<AlgorithmRunLedger, KnowledgeError> {
    let mut runs = existing.runs.clone();
    for row in &staged.runs {
        match runs.iter().find(|current| current.run_uuid == row.run_uuid) {
            Some(current) if current == row => {}
            Some(_) => return Err(KnowledgeError::Conflict("run_uuid")),
            None => runs.push(row.clone()),
        }
    }
    let mut events = existing.events.clone();
    for row in &staged.events {
        match events
            .iter()
            .find(|current| current.event_uuid == row.event_uuid)
        {
            Some(current) if current == row => {}
            Some(_) => return Err(KnowledgeError::Conflict("event_uuid")),
            None => events.push(row.clone()),
        }
    }
    AlgorithmRunLedger::new(runs, events)
}

fn algorithm_run_input_digest(
    existing: &AlgorithmRunLedger,
    staged: &AlgorithmRunLedger,
) -> String {
    let mut writer = CanonicalWriter::new();
    for ledger in [existing, staged] {
        writer.u64(ledger.runs.len() as u64).unwrap();
        for row in &ledger.runs {
            writer.raw(row.run_uuid.as_bytes()).unwrap();
            writer.text(&row.algorithm).unwrap();
            writer.u32(row.algorithm_version).unwrap();
            writer.u32(row.descriptor_version).unwrap();
            writer.binary(&row.descriptor).unwrap();
            writer.raw(&row.projection_fingerprint).unwrap();
            writer.raw(row.provenance_uuid.as_bytes()).unwrap();
            writer.i64(row.started_at_micros).unwrap();
            writer.u32(row.contract_version).unwrap();
        }
        writer.u64(ledger.events.len() as u64).unwrap();
        for row in &ledger.events {
            writer.raw(row.event_uuid.as_bytes()).unwrap();
            writer.raw(row.run_uuid.as_bytes()).unwrap();
            writer.text(row.state.as_str()).unwrap();
            match row.result_fingerprint {
                Some(value) => {
                    writer.u8(1).unwrap();
                    writer.raw(&value).unwrap();
                }
                None => writer.u8(0).unwrap(),
            }
            match &row.error_code {
                Some(value) => {
                    writer.u8(1).unwrap();
                    writer.text(value).unwrap();
                }
                None => writer.u8(0).unwrap(),
            }
            writer.i64(row.recorded_at_micros).unwrap();
            writer.raw(row.provenance_uuid.as_bytes()).unwrap();
            writer.u32(row.contract_version).unwrap();
        }
    }
    fingerprint(
        CanonicalDomain::InvocationDescriptor,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .unwrap()
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect()
}

fn reset_row_validation_counts() {
    RUN_ROW_VALIDATIONS.with(|count| count.set(Some(0)));
    EVENT_ROW_VALIDATIONS.with(|count| count.set(Some(0)));
}

fn row_validation_counts() -> (usize, usize) {
    (
        RUN_ROW_VALIDATIONS.with(|count| count.get().unwrap_or_default()),
        EVENT_ROW_VALIDATIONS.with(|count| count.get().unwrap_or_default()),
    )
}

fn stop_row_validation_counts() {
    RUN_ROW_VALIDATIONS.with(|count| count.set(None));
    EVENT_ROW_VALIDATIONS.with(|count| count.set(None));
}

#[test]
fn merge_does_not_repeat_validated_row_checks_and_preserves_append_results() {
    let existing_run = run(1, 10);
    let existing_start = started(10, &existing_run);
    let existing =
        AlgorithmRunLedger::new(vec![existing_run.clone()], vec![existing_start.clone()]).unwrap();

    let appended_run = run(20, 20);
    let appended_start = started(30, &appended_run);
    reset_row_validation_counts();
    let staged = AlgorithmRunLedger::new(
        vec![existing_run, appended_run.clone()],
        vec![existing_start, appended_start.clone()],
    )
    .unwrap();
    assert_eq!(row_validation_counts(), (2, 2));

    reset_row_validation_counts();
    let expected = full_validation_merge(&existing, &staged).unwrap();
    reset_row_validation_counts();
    let merged = existing.merge(&staged).unwrap();
    assert_eq!(row_validation_counts(), (0, 0));
    stop_row_validation_counts();
    assert_eq!(merged, expected);
    assert_eq!(merged.runs(), &[run(1, 10), appended_run.clone()]);
    assert_eq!(merged.events(), &[started(10, &run(1, 10)), appended_start]);
    assert_eq!(merged.merge(&merged).unwrap(), merged);
}

#[test]
fn append_events_validates_only_incoming_events_and_preserves_full_validation_result() {
    let first = run(1, 10);
    let second = run(20, 20);
    let first_start = started(10, &first);
    let second_start = started(30, &second);
    let existing = AlgorithmRunLedger::new(
        vec![first.clone(), second.clone()],
        vec![first_start.clone(), second_start.clone()],
    )
    .unwrap();
    let terminal = completed(40, &first, 15);

    reset_row_validation_counts();
    let appended = existing.append_events(vec![terminal.clone()]).unwrap();
    assert_eq!(row_validation_counts(), (0, 1));
    stop_row_validation_counts();
    assert_eq!(existing.events().len(), 2);
    assert_eq!(appended.events().len(), 3);
    assert_eq!(
        appended.events(),
        &[first_start.clone(), terminal.clone(), second_start.clone()]
    );

    reset_row_validation_counts();
    let expected =
        AlgorithmRunLedger::new(existing.runs().to_vec(), appended.events().to_vec()).unwrap();
    assert_eq!(appended, expected);
    stop_row_validation_counts();
}

#[test]
fn append_events_rechecks_combined_identity_state_time_ownership_and_caps() {
    let first = run(1, 10);
    let second = run(20, 20);
    let first_start = started(10, &first);
    let base = AlgorithmRunLedger::new(
        vec![first.clone(), second.clone()],
        vec![first_start.clone(), started(30, &second)],
    )
    .unwrap();
    let terminal = completed(40, &first, 30);

    assert!(matches!(
        base.append_events(vec![first_start.clone()]),
        Err(KnowledgeError::Duplicate("event_uuid"))
    ));
    assert!(matches!(
        base.append_events(vec![terminal.clone(), terminal.clone()]),
        Err(KnowledgeError::Duplicate("event_uuid"))
    ));
    assert!(matches!(
        base.append_events(vec![AlgorithmRunEvent {
            run_uuid: uuid7(90),
            ..terminal.clone()
        }]),
        Err(KnowledgeError::Dangling("run_uuid"))
    ));
    assert!(matches!(
        base.append_events(vec![AlgorithmRunEvent {
            recorded_at_micros: 9,
            ..terminal.clone()
        }]),
        Err(KnowledgeError::Invalid {
            field: "recorded_at",
            ..
        })
    ));
    assert!(matches!(
        base.append_events(vec![started(50, &first)]),
        Err(KnowledgeError::Invalid {
            field: "started",
            ..
        })
    ));
    assert!(matches!(
        base.append_events(vec![terminal.clone(), completed(41, &first, 31)]),
        Err(KnowledgeError::Invalid {
            field: "terminal",
            ..
        })
    ));
    assert!(matches!(
        base.append_events_with_limits(vec![terminal], usize::MAX, 2),
        Err(KnowledgeError::Limit {
            participant: "algorithm_run_events",
            observed: 3,
            limit: 2,
        })
    ));
    assert!(matches!(
        base.append_events_with_limits(Vec::new(), 1, usize::MAX),
        Err(KnowledgeError::Limit {
            participant: "algorithm_runs",
            observed: 2,
            limit: 1,
        })
    ));
}

#[test]
fn merge_enforces_combined_run_and_event_caps_after_replays() {
    let first = run(1, 10);
    let second = run(20, 20);
    let first_start = started(10, &first);
    let second_start = started(30, &second);
    let base = AlgorithmRunLedger::new(vec![first.clone()], vec![first_start.clone()]).unwrap();
    let staged =
        AlgorithmRunLedger::new(vec![first, second], vec![first_start, second_start]).unwrap();

    assert!(matches!(
        base.merge_with_limits(&staged, 1, usize::MAX),
        Err(KnowledgeError::Limit {
            participant: "algorithm_runs",
            observed: 2,
            limit: 1,
        })
    ));
    assert!(matches!(
        base.merge_with_limits(&staged, usize::MAX, 1),
        Err(KnowledgeError::Limit {
            participant: "algorithm_run_events",
            observed: 2,
            limit: 1,
        })
    ));
}

#[test]
fn merge_is_idempotent_and_rejects_conflicting_identity_reuse() {
    let original = run(1, 10);
    let original_start = started(10, &original);
    let base =
        AlgorithmRunLedger::new(vec![original.clone()], vec![original_start.clone()]).unwrap();
    assert_eq!(base.merge(&base).unwrap(), base);

    let conflicting_run = AlgorithmRun {
        algorithm: "rank.other".into(),
        ..original.clone()
    };
    let staged_run = AlgorithmRunLedger::new(
        vec![conflicting_run.clone()],
        vec![started(11, &conflicting_run)],
    )
    .unwrap();
    assert!(matches!(
        base.merge(&staged_run),
        Err(KnowledgeError::Conflict("run_uuid"))
    ));

    let staged_event = AlgorithmRunLedger::new(
        vec![original.clone()],
        vec![started(11, &original), completed(10, &original, 11)],
    )
    .unwrap();
    assert!(matches!(
        base.merge(&staged_event),
        Err(KnowledgeError::Conflict("event_uuid"))
    ));
}

#[test]
fn run_uuid_must_not_be_nil() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        run_uuid: Uuid::nil(),
        ..valid
    });
}

#[test]
fn run_uuid_must_be_uuidv7() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        run_uuid: uuid4(102),
        ..valid
    });
}

#[test]
fn run_provenance_uuid_must_not_be_nil() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        provenance_uuid: Uuid::nil(),
        ..valid
    });
}

#[test]
fn run_algorithm_must_be_nonempty() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        algorithm: String::new(),
        ..valid
    });
}

#[test]
fn run_algorithm_text_must_respect_the_canonical_byte_limit() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        algorithm: "a".repeat(graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES as usize + 1),
        ..valid
    });
}

#[test]
fn run_algorithm_version_must_be_supported() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        algorithm_version: 2,
        ..valid
    });
}

#[test]
fn run_descriptor_version_must_be_supported() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        descriptor_version: 2,
        ..valid
    });
}

#[test]
fn run_descriptor_must_be_nonempty() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        descriptor: Vec::new(),
        ..valid
    });
}

#[test]
fn run_descriptor_must_respect_the_canonical_byte_limit() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        descriptor: vec![1; graphforge_core::canonical::MAX_CANONICAL_BINARY_BYTES as usize + 1],
        ..valid
    });
}

#[test]
fn run_contract_version_must_be_supported() {
    let valid = run(100, 10);
    assert_invalid_run(&AlgorithmRun {
        contract_version: 2,
        ..valid
    });
}

#[test]
fn event_uuid_must_not_be_nil() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        event_uuid: Uuid::nil(),
        ..valid
    });
}

#[test]
fn event_run_uuid_must_not_be_nil() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        run_uuid: Uuid::nil(),
        ..valid
    });
}

#[test]
fn event_run_uuid_must_be_uuidv7() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        run_uuid: uuid4(102),
        ..valid
    });
}

#[test]
fn event_provenance_uuid_must_not_be_nil() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        provenance_uuid: Uuid::nil(),
        ..valid
    });
}

#[test]
fn event_contract_version_must_be_supported() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        contract_version: 2,
        ..valid
    });
}

#[test]
fn event_error_code_must_be_nonempty_when_present() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Failed,
        result_fingerprint: None,
        error_code: Some(String::new()),
        ..valid
    });
}

#[test]
fn event_error_code_must_respect_the_canonical_byte_limit() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Failed,
        result_fingerprint: None,
        error_code: Some(format!(
            "GF_{}",
            "x".repeat(graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES as usize,)
        )),
        ..valid
    });
}

#[test]
fn event_error_code_must_use_the_stable_gf_prefix() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Failed,
        result_fingerprint: None,
        error_code: Some("EXECUTION".into()),
        ..valid
    });
}

#[test]
fn started_event_must_not_have_a_result_fingerprint() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        result_fingerprint: Some([9; 32]),
        ..valid
    });
}

#[test]
fn started_event_must_not_have_an_error_code() {
    let valid_run = run(100, 10);
    let valid = started(101, &valid_run);
    assert_invalid_event(&AlgorithmRunEvent {
        error_code: Some("GF_EXECUTION".into()),
        ..valid
    });
}

#[test]
fn completed_event_requires_a_result_fingerprint() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        result_fingerprint: None,
        ..valid
    });
}

#[test]
fn completed_event_must_not_have_an_error_code() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        error_code: Some("GF_EXECUTION".into()),
        ..valid
    });
}

#[test]
fn failed_event_requires_an_error_code_and_no_result_fingerprint() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Failed,
        result_fingerprint: None,
        error_code: None,
        ..valid
    });
}

#[test]
fn cancelled_event_requires_an_error_code_and_no_result_fingerprint() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Cancelled,
        result_fingerprint: None,
        error_code: None,
        ..valid
    });
}

#[test]
fn interrupted_event_requires_an_error_code_and_no_result_fingerprint() {
    let valid_run = run(100, 10);
    let valid = completed(101, &valid_run, 11);
    assert_invalid_event(&AlgorithmRunEvent {
        state: AlgorithmRunState::Interrupted,
        result_fingerprint: None,
        error_code: None,
        ..valid
    });
}

#[test]
fn non_success_terminal_must_not_have_a_result_fingerprint() {
    let valid_run = run(100, 10);
    for state in [
        AlgorithmRunState::Failed,
        AlgorithmRunState::Cancelled,
        AlgorithmRunState::Interrupted,
    ] {
        let valid = AlgorithmRunEvent {
            state,
            error_code: Some("GF_TEST".into()),
            ..completed(101, &valid_run, 11)
        };
        assert_invalid_event(&valid);
    }
}

#[test]
fn run_uuid_must_be_unique() {
    let valid_run = run(100, 10);
    assert!(matches!(
        AlgorithmRunLedger::new(
            vec![valid_run.clone(), valid_run.clone()],
            vec![started(101, &valid_run)],
        ),
        Err(KnowledgeError::Duplicate("run_uuid"))
    ));
}

#[test]
fn event_uuid_must_be_unique() {
    let first = run(100, 10);
    let second = run(200, 20);
    let first_start = started(101, &first);
    let second_start = AlgorithmRunEvent {
        event_uuid: first_start.event_uuid,
        ..started(201, &second)
    };
    assert!(matches!(
        AlgorithmRunLedger::new(vec![first, second], vec![first_start, second_start]),
        Err(KnowledgeError::Duplicate("event_uuid"))
    ));
}

#[test]
fn event_must_reference_an_existing_run() {
    let valid_run = run(100, 10);
    let start = started(101, &valid_run);
    let dangling = AlgorithmRunEvent {
        run_uuid: uuid7(200),
        ..completed(102, &valid_run, 11)
    };
    assert!(matches!(
        AlgorithmRunLedger::new(vec![valid_run], vec![start, dangling]),
        Err(KnowledgeError::Dangling("run_uuid"))
    ));
}

#[test]
fn run_event_must_not_precede_its_start() {
    let valid_run = run(100, 10);
    let start = started(101, &valid_run);
    let early = completed(102, &valid_run, 9);
    assert_invalid_ledger(vec![valid_run], vec![start, early]);
}

#[test]
fn every_run_requires_exactly_one_start_event() {
    assert_invalid_ledger(vec![run(100, 10)], Vec::new());
}

#[test]
fn start_event_must_match_run_start_time() {
    let valid_run = run(100, 10);
    let mismatched = AlgorithmRunEvent::new(
        uuid7(101),
        valid_run.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        11,
        valid_run.provenance_uuid,
    )
    .unwrap();
    assert_invalid_ledger(vec![valid_run], vec![mismatched]);
}

#[test]
fn start_event_must_match_run_provenance() {
    let valid_run = run(100, 10);
    let mismatched = AlgorithmRunEvent::new(
        uuid7(101),
        valid_run.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        10,
        uuid7(102),
    )
    .unwrap();
    assert_invalid_ledger(vec![valid_run], vec![mismatched]);
}

#[test]
fn each_run_allows_at_most_one_terminal_event() {
    let valid_run = run(100, 10);
    assert_invalid_ledger(
        vec![valid_run.clone()],
        vec![
            started(101, &valid_run),
            completed(102, &valid_run, 11),
            completed(103, &valid_run, 12),
        ],
    );
}

#[test]
fn constructor_run_limit_is_enforced() {
    let valid_run = run(100, 10);
    assert!(matches!(
        AlgorithmRunLedger::new_with_limits(
            vec![valid_run.clone()],
            vec![started(101, &valid_run)],
            0,
            1
        ),
        Err(KnowledgeError::Limit {
            participant: "algorithm_runs",
            observed: 1,
            limit: 0
        })
    ));
}

#[test]
fn constructor_event_limit_is_enforced() {
    let valid_run = run(100, 10);
    assert!(matches!(
        AlgorithmRunLedger::new_with_limits(
            vec![valid_run.clone()],
            vec![started(101, &valid_run)],
            1,
            0
        ),
        Err(KnowledgeError::Limit {
            participant: "algorithm_run_events",
            observed: 1,
            limit: 0
        })
    ));
}

#[test]
fn run_and_event_rows_are_sorted_by_timestamp_then_uuid() {
    let later_run = run(200, 20);
    let earlier_run = run(100, 10);
    let later_start = started(201, &later_run);
    let earlier_start = started(101, &earlier_run);
    let ledger = AlgorithmRunLedger::new(
        vec![later_run.clone(), earlier_run.clone()],
        vec![later_start.clone(), earlier_start.clone()],
    )
    .unwrap();
    assert_eq!(ledger.runs(), &[earlier_run.clone(), later_run.clone()]);
    assert_eq!(
        ledger.events(),
        &[earlier_start.clone(), later_start.clone()]
    );

    let appended_run = run(300, 15);
    let appended_start = started(301, &appended_run);
    let staged = AlgorithmRunLedger::new(vec![appended_run], vec![appended_start]).unwrap();
    let merged = ledger.merge(&staged).unwrap();
    assert_eq!(merged.runs(), &[earlier_run, run(300, 15), later_run]);
    assert_eq!(
        merged.events(),
        &[earlier_start, started(301, &run(300, 15)), later_start]
    );
}

#[test]
fn equal_timestamp_run_and_event_rows_sort_by_uuid() {
    let run_a = run(100, 10);
    let run_b = run(200, 10);
    let start_a = started(101, &run_a);
    let start_b = started(201, &run_b);
    let ledger = AlgorithmRunLedger::new(
        vec![run_b.clone(), run_a.clone()],
        vec![start_b.clone(), start_a.clone()],
    )
    .unwrap();
    assert_eq!(ledger.runs(), &[run_a.clone(), run_b.clone()]);
    assert_eq!(ledger.events(), &[start_a, start_b]);
}

#[test]
#[ignore = "requires a quiet host; records comparable baseline and incremental timings"]
fn quiet_host_algorithm_run_merge_cost_measurement() {
    use std::time::Instant;

    for existing_count in [1_000_u128, 10_000_u128] {
        let runs = (0..existing_count)
            .map(|index| run(10_000 + index * 3, index as i64))
            .collect::<Vec<_>>();
        let mut events = Vec::with_capacity(runs.len());
        for (index, row) in runs.iter().enumerate() {
            events.push(started(20_000 + index as u128 * 3, row));
        }
        let existing = AlgorithmRunLedger::new(runs, events).unwrap();
        let staged_run = run(10_000 + existing_count * 3, existing_count as i64);
        let staged = AlgorithmRunLedger::new(
            vec![staged_run.clone()],
            vec![started(20_000 + existing_count * 3, &staged_run)],
        )
        .unwrap();
        let input_digest = algorithm_run_input_digest(&existing, &staged);
        let baseline = full_validation_merge(&existing, &staged).unwrap();
        let incremental = existing.merge(&staged).unwrap();
        assert_eq!(incremental, baseline, "output mismatch at {existing_count}");

        let mut baseline_samples = Vec::with_capacity(9);
        let mut incremental_samples = Vec::with_capacity(9);
        for repetition in 0..9 {
            let baseline_first = repetition % 2 == 0;
            if baseline_first {
                let started = Instant::now();
                std::hint::black_box(full_validation_merge(&existing, &staged).unwrap());
                baseline_samples.push(started.elapsed().as_nanos());
                let started = Instant::now();
                std::hint::black_box(existing.merge(&staged).unwrap());
                incremental_samples.push(started.elapsed().as_nanos());
            } else {
                let started = Instant::now();
                std::hint::black_box(existing.merge(&staged).unwrap());
                incremental_samples.push(started.elapsed().as_nanos());
                let started = Instant::now();
                std::hint::black_box(full_validation_merge(&existing, &staged).unwrap());
                baseline_samples.push(started.elapsed().as_nanos());
            }
        }
        let mut baseline_sorted = baseline_samples.clone();
        baseline_sorted.sort_unstable();
        let mut incremental_sorted = incremental_samples.clone();
        incremental_sorted.sort_unstable();
        let existing_rows = existing.runs.len() + existing.events.len();
        eprintln!(
            "algorithm_run_merge existing_runs={} existing_events={} staged_runs=1 staged_events=1 digest={} baseline_ns={:?} baseline_median_ns={} incremental_ns={:?} incremental_median_ns={} baseline_ns_per_existing_row={} incremental_ns_per_existing_row={}",
            existing.runs.len(),
            existing.events.len(),
            input_digest,
            baseline_samples,
            baseline_sorted[baseline_sorted.len() / 2],
            incremental_samples,
            incremental_sorted[incremental_sorted.len() / 2],
            baseline_sorted[baseline_sorted.len() / 2] / existing_rows as u128,
            incremental_sorted[incremental_sorted.len() / 2] / existing_rows as u128,
        );
    }
}

#[test]
fn algorithm_run_lifecycle_round_trips_and_rejects_second_terminal() {
    assert_eq!(
        ALGORITHM_RUN_SCHEMA_FINGERPRINT
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "ff52080371c5956aa9bc8b0cf9c1022e2c3271d576d43ab57f4b75eb33cf64ed"
    );
    assert_eq!(
        ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "f055828f73440cca1bd2ea746cb4599d52a736229d2a4df8ddec70972ceef0aa"
    );
    let run = AlgorithmRun::new(
        uuid7(40),
        "rank.degree".into(),
        1,
        1,
        b"canonical descriptor".to_vec(),
        [7; 32],
        uuid7(41),
        10,
    )
    .unwrap();
    let started = AlgorithmRunEvent::new(
        uuid7(42),
        run.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        10,
        run.provenance_uuid,
    )
    .unwrap();
    let completed = AlgorithmRunEvent::new(
        uuid7(43),
        run.run_uuid,
        AlgorithmRunState::Completed,
        Some([8; 32]),
        None,
        11,
        uuid7(44),
    )
    .unwrap();
    let ledger = AlgorithmRunLedger::new(vec![run.clone()], vec![completed, started]).unwrap();
    assert_eq!(
        AlgorithmRunLedger::from_batches(
            &[ledger.run_batch().unwrap()],
            &[ledger.event_batch().unwrap()],
        )
        .unwrap(),
        ledger
    );
    let failed = AlgorithmRunEvent::new(
        uuid7(45),
        run.run_uuid,
        AlgorithmRunState::Failed,
        None,
        Some("GF_EXECUTION".into()),
        12,
        uuid7(46),
    )
    .unwrap();
    let mut events = ledger.events.clone();
    events.push(failed);
    assert!(matches!(
        AlgorithmRunLedger::new(ledger.runs.clone(), events),
        Err(KnowledgeError::Invalid {
            field: "terminal",
            ..
        })
    ));
}

#[test]
fn algorithm_run_validation_rejects_every_malformed_identity_and_transition() {
    let run = AlgorithmRun::new(
        uuid7(40),
        "pagerank".into(),
        1,
        1,
        vec![1],
        [2; 32],
        uuid7(41),
        100,
    )
    .unwrap();
    for invalid_run in [
        AlgorithmRun {
            algorithm: String::new(),
            ..run.clone()
        },
        AlgorithmRun {
            algorithm_version: 2,
            ..run.clone()
        },
        AlgorithmRun {
            descriptor_version: 2,
            ..run.clone()
        },
        AlgorithmRun {
            descriptor: Vec::new(),
            ..run.clone()
        },
        AlgorithmRun {
            contract_version: 2,
            ..run.clone()
        },
    ] {
        assert!(matches!(
            validate_algorithm_run(&invalid_run),
            Err(KnowledgeError::Invalid { .. })
        ));
    }

    let start = AlgorithmRunEvent::new(
        uuid7(42),
        run.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        100,
        run.provenance_uuid,
    )
    .unwrap();
    let completed = AlgorithmRunEvent::new(
        uuid7(43),
        run.run_uuid,
        AlgorithmRunState::Completed,
        Some([3; 32]),
        None,
        101,
        uuid7(44),
    )
    .unwrap();
    assert_eq!(
        AlgorithmRunLedger::new(vec![run.clone()], vec![start.clone(), completed.clone()])
            .unwrap()
            .events_for(run.run_uuid)
            .len(),
        2
    );
    assert!(matches!(
        validate_algorithm_run_event(&AlgorithmRunEvent {
            contract_version: 2,
            ..start.clone()
        }),
        Err(KnowledgeError::Invalid { .. })
    ));
    for event in [
        AlgorithmRunEvent {
            result_fingerprint: Some([1; 32]),
            ..start.clone()
        },
        AlgorithmRunEvent {
            result_fingerprint: None,
            ..completed.clone()
        },
        AlgorithmRunEvent {
            state: AlgorithmRunState::Failed,
            result_fingerprint: None,
            error_code: None,
            ..completed.clone()
        },
        AlgorithmRunEvent {
            state: AlgorithmRunState::Failed,
            result_fingerprint: None,
            error_code: Some("bad".into()),
            ..completed.clone()
        },
    ] {
        assert!(matches!(
            validate_algorithm_run_event(&event),
            Err(KnowledgeError::Invalid { .. })
        ));
    }

    assert!(matches!(
        AlgorithmRunLedger::new(vec![run.clone(), run.clone()], vec![start.clone()]),
        Err(KnowledgeError::Duplicate("run_uuid"))
    ));
    assert!(matches!(
        AlgorithmRunLedger::new(vec![run.clone()], vec![start.clone(), start.clone()]),
        Err(KnowledgeError::Duplicate("event_uuid"))
    ));
    assert!(matches!(
        AlgorithmRunLedger::new(
            vec![run.clone()],
            vec![AlgorithmRunEvent {
                run_uuid: uuid7(50),
                ..start.clone()
            }]
        ),
        Err(KnowledgeError::Dangling("run_uuid"))
    ));
    assert!(matches!(
        AlgorithmRunLedger::new(
            vec![run.clone()],
            vec![AlgorithmRunEvent {
                recorded_at_micros: 99,
                ..start.clone()
            }]
        ),
        Err(KnowledgeError::Invalid {
            field: "recorded_at",
            ..
        })
    ));
    assert!(matches!(
        AlgorithmRunLedger::new(vec![run.clone()], Vec::new()),
        Err(KnowledgeError::Invalid {
            field: "started",
            ..
        })
    ));
    assert!(matches!(
        AlgorithmRunLedger::new(
            vec![run],
            vec![
                start,
                completed.clone(),
                AlgorithmRunEvent {
                    event_uuid: uuid7(51),
                    ..completed
                },
            ]
        ),
        Err(KnowledgeError::Invalid {
            field: "terminal",
            ..
        })
    ));
}
