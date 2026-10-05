//! Public-facade evidence for transaction-owned workspace rollback snapshots.

use std::collections::HashMap;
use std::sync::Mutex;

use arrow::array::Int64Array;
use graphforge_api::{GraphForge, OperationId, WriteContext};
use graphforge_storage::IoSnapshot;
use uuid::Uuid;

static IO_STATS_LOCK: Mutex<()> = Mutex::new(());

fn context(seed: u128) -> WriteContext {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    WriteContext {
        operation_uuid: OperationId(Uuid::from_bytes(bytes)),
        actor_uuid: None,
    }
}

fn seed_query(nodes: usize) -> String {
    (0..nodes)
        .map(|value| format!("CREATE (:Noise {{value: {value}}})"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_nodes(graph: &GraphForge) -> i64 {
    graph.execute("MATCH (n) RETURN count(*)").unwrap().batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn measure(unrelated_nodes: usize, statements: usize) -> (IoSnapshot, i64) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute(&seed_query(unrelated_nodes)).unwrap();
    graph.execute("CREATE (:Written {value: -1})").unwrap();

    let _capture = graphforge_storage::io_stats::CaptureScope::install();
    graphforge_storage::io_stats::reset();
    let transaction = graph
        .begin_transaction(context(0x1807_0000 + statements as u128))
        .unwrap();
    for value in 0..statements {
        let query = if value == 1 {
            "MATCH (n:Written {value: 0}) CREATE (:Written {value: 1}) SET n.seen = 1".to_owned()
        } else {
            format!("CREATE (:Written {{value: {value}}})")
        };
        transaction.stage_cypher(&query, HashMap::new()).unwrap();
    }
    transaction.commit(&graph).unwrap();
    let io = graphforge_storage::io_stats::snapshot().unwrap();
    let expected = i64::try_from(unrelated_nodes + statements + 1).unwrap();
    assert_eq!(count_nodes(&graph), expected);
    if statements > 1 {
        let seen = graph
            .execute("MATCH (n:Written) WHERE n.seen = 1 RETURN count(*)")
            .unwrap();
        assert_eq!(
            seen.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(count_nodes(&reopened), expected);
    (io, expected)
}

fn measure_split_transactions(unrelated_nodes: usize, statements: usize) -> IoSnapshot {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute(&seed_query(unrelated_nodes)).unwrap();
    graph.execute("CREATE (:Written {value: -1})").unwrap();

    let _capture = graphforge_storage::io_stats::CaptureScope::install();
    graphforge_storage::io_stats::reset();
    for value in 0..statements {
        let transaction = graph
            .begin_transaction(context(0x1817_0000 + value as u128))
            .unwrap();
        transaction
            .stage_cypher(
                &format!("CREATE (:Written {{value: {value}}})"),
                HashMap::new(),
            )
            .unwrap();
        transaction.commit(&graph).unwrap();
    }
    graphforge_storage::io_stats::snapshot().unwrap()
}

#[test]
fn transaction_checkpoint_work_is_once_per_transaction_and_tracks_workspace_size() {
    let _guard = IO_STATS_LOCK.lock().unwrap();
    let small_one = measure(8, 1).0;
    let small_many = measure(8, 4).0;
    let large_one = measure(64, 1).0;
    let large_many = measure(64, 4).0;
    let split_baseline = measure_split_transactions(8, 4);

    for (label, work) in [
        ("small_1", &small_one),
        ("small_4", &small_many),
        ("large_1", &large_one),
        ("large_4", &large_many),
        ("split_4", &split_baseline),
    ] {
        eprintln!(
            "transaction checkpoint evidence {label}: captures={} source_bytes={} copied_bytes={} files={} flushes={}",
            work.workspace_checkpoints,
            work.workspace_checkpoint_source_bytes,
            work.workspace_checkpoint_bytes,
            work.workspace_checkpoint_files,
            work.workspace_checkpoint_flushes
        );
    }

    for work in [&small_one, &small_many, &large_one, &large_many] {
        assert!(work.workspace_checkpoint_files > 0, "{work:?}");
        assert!(work.workspace_checkpoint_bytes > 0, "{work:?}");
        assert!(work.workspace_checkpoint_flushes > 0, "{work:?}");
        assert_eq!(
            work.workspace_checkpoint_source_bytes, work.workspace_checkpoint_bytes,
            "copied bytes must reconcile with the source inventory: {work:?}"
        );
    }
    assert_eq!(
        small_one.workspace_checkpoint_files,
        small_many.workspace_checkpoint_files
    );
    assert_eq!(small_one.workspace_checkpoints, 1, "{small_one:?}");
    assert_eq!(small_many.workspace_checkpoints, 1, "{small_many:?}");
    assert_eq!(
        small_one.workspace_checkpoint_flushes,
        small_many.workspace_checkpoint_flushes
    );
    assert_eq!(
        large_one.workspace_checkpoint_files,
        large_many.workspace_checkpoint_files
    );
    assert_eq!(large_one.workspace_checkpoints, 1, "{large_one:?}");
    assert_eq!(large_many.workspace_checkpoints, 1, "{large_many:?}");
    assert_eq!(
        large_one.workspace_checkpoint_flushes,
        large_many.workspace_checkpoint_flushes
    );
    assert!(
        split_baseline.workspace_checkpoint_files > small_many.workspace_checkpoint_files,
        "one-statement transaction baseline should copy more files: {split_baseline:?} {small_many:?}"
    );
    assert!(
        split_baseline.workspace_checkpoint_flushes > small_many.workspace_checkpoint_flushes,
        "one-statement transaction baseline should flush more files: {split_baseline:?} {small_many:?}"
    );
    assert_eq!(
        split_baseline.workspace_checkpoints, 4,
        "one-statement transaction baseline should capture four checkpoints: {split_baseline:?}"
    );
    assert!(
        split_baseline.workspace_checkpoint_bytes > small_many.workspace_checkpoint_bytes,
        "baseline should expose repeated checkpoint copies: {split_baseline:?} {small_many:?}"
    );
    assert!(
        large_one.workspace_checkpoint_bytes > small_one.workspace_checkpoint_bytes,
        "workspace-size dimension was not observed: {small_one:?} {large_one:?}"
    );
}

#[test]
fn later_statement_failure_restores_prior_workspace_across_reopen() {
    let _guard = IO_STATS_LOCK.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value: 7})").unwrap();

    let transaction = graph.begin_transaction(context(0x1807_fa11)).unwrap();
    transaction
        .stage_cypher("CREATE (:Written {value: 9})", HashMap::new())
        .unwrap();
    transaction
        .stage_cypher("THIS IS NOT CYPHER", HashMap::new())
        .unwrap();
    assert!(transaction.commit(&graph).is_err());
    assert_eq!(count_nodes(&graph), 1);
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(count_nodes(&reopened), 1);
    assert_eq!(
        reopened
            .execute("MATCH (n:Retained) RETURN n.value")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert_eq!(
        reopened
            .execute("MATCH (n:Written) RETURN n")
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
}
