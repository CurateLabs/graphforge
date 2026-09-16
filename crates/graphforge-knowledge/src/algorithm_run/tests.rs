use super::*;
use crate::tests::uuid7;
use crate::{ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT, ALGORITHM_RUN_SCHEMA_FINGERPRINT};

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
