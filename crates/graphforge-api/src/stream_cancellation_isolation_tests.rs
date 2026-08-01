use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use crate::{
    CancellationToken, GfError, GraphForge, ListCheckpointsRequest, PageRequest,
    SendableRecordBatchStream,
};

const CASES: usize = 4;
const DEADLINE: Duration = Duration::from_secs(10);
const QUERY: &str = "MATCH (n:StreamRow) RETURN n.ordinal AS ordinal ORDER BY ordinal";

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn recv<T>(receiver: &mpsc::Receiver<T>, deadline: Instant, phase: &str) -> T {
    receiver
        .recv_timeout(remaining(deadline))
        .unwrap_or_else(|error| panic!("phase={phase} timeout={DEADLINE:?} channel={error}"))
}

fn logical_fingerprint(batches: &[RecordBatch]) -> Result<[u8; 32], GfError> {
    let logical = batches
        .iter()
        .map(|batch| {
            let schema = Arc::new(Schema::new(batch.schema().fields().clone()));
            RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|error| {
                GfError::Execution(format!("logical Arrow normalization failed: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::canonical_arrow::result_fingerprint(&logical)
        .map_err(|error| GfError::Execution(format!("canonical Arrow result failed: {error}")))
}

fn collect_stream(mut stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("build stream runtime: {error}"))?;
    runtime.block_on(async move {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.map_err(|error| format!("consume stream: {error}"))?);
        }
        Ok(batches)
    })
}

fn fixture() -> (tempfile::TempDir, Arc<GraphForge>, [u8; 32]) {
    let project = tempfile::tempdir().expect("phase=fixture tempdir");
    let graph = Arc::new(
        GraphForge::new(project.path().to_str()).expect("phase=fixture open persistent project"),
    );
    graph
        .execute("UNWIND range(1, 9000) AS i CREATE (:StreamRow {ordinal: i})")
        .expect("phase=fixture seed more than one execution batch");
    let expected = graph.execute(QUERY).expect("phase=fixture collected query");
    let expected = logical_fingerprint(&expected.batches).expect("phase=fixture fingerprint");

    let stream = graph
        .execute_stream(QUERY)
        .expect("phase=fixture create verification stream");
    let (sender, receiver) = mpsc::sync_channel(1);
    let handle = thread::Builder::new()
        .name("gf-multi-batch-proof".into())
        .spawn(move || {
            let _ = sender.send(collect_stream(stream));
        })
        .expect("phase=fixture spawn verification worker");
    let batches = recv(&receiver, Instant::now() + DEADLINE, "fixture-multi-batch")
        .unwrap_or_else(|error| panic!("phase=fixture stream failed: {error}"));
    handle
        .join()
        .expect("phase=fixture verification worker panicked");
    assert!(
        batches.len() >= 2,
        "phase=fixture expected deterministic multi-batch stream, got {} batch(es)",
        batches.len()
    );
    assert_eq!(
        logical_fingerprint(&batches).expect("phase=fixture stream fingerprint"),
        expected,
        "phase=fixture streamed canonical Arrow differs from collected result"
    );
    (project, graph, expected)
}

#[test]
fn early_stream_drop_does_not_truncate_concurrent_peer() {
    let (_project, graph, expected) = fixture();
    for case in 0..CASES {
        let deadline = Instant::now() + DEADLINE;
        let drop_stream = graph
            .execute_stream(QUERY)
            .unwrap_or_else(|error| panic!("case={case} phase=drop create stream: {error}"));
        let peer_stream = graph
            .execute_stream(QUERY)
            .unwrap_or_else(|error| panic!("case={case} phase=peer create stream: {error}"));
        let (start_drop_tx, start_drop_rx) = mpsc::sync_channel(1);
        let (start_peer_tx, start_peer_rx) = mpsc::sync_channel(1);
        let (drop_ready_tx, drop_ready_rx) = mpsc::sync_channel(1);
        let (peer_ready_tx, peer_ready_rx) = mpsc::sync_channel(1);
        let (peer_first_tx, peer_first_rx) = mpsc::sync_channel(1);
        let (drop_done_tx, drop_done_rx) = mpsc::sync_channel(1);
        let (drop_result_tx, drop_result_rx) = mpsc::sync_channel(1);
        let (peer_result_tx, peer_result_rx) = mpsc::sync_channel(1);

        let drop_handle = thread::Builder::new()
            .name(format!("gf-stream-drop-{case}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("phase=drop build runtime");
                drop_ready_tx.send(()).expect("phase=drop ready");
                recv(&start_drop_rx, deadline, "drop-start");
                let mut stream = drop_stream;
                let first = runtime
                    .block_on(stream.next())
                    .expect("phase=drop stream ended before first batch")
                    .map_err(|error| format!("phase=drop first batch: {error}"));
                recv(&peer_first_rx, deadline, "drop-await-peer-first");
                drop(stream);
                drop_done_tx.send(()).expect("phase=drop publish dropped");
                let _ = drop_result_tx.send(first.map(|batch| batch.num_rows()));
            })
            .unwrap_or_else(|error| panic!("case={case} phase=drop spawn: {error}"));

        let peer_handle = thread::Builder::new()
            .name(format!("gf-stream-peer-{case}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("phase=peer build runtime");
                peer_ready_tx.send(()).expect("phase=peer ready");
                recv(&start_peer_rx, deadline, "peer-start");
                let mut stream = peer_stream;
                let outcome = runtime.block_on(async move {
                    let first = stream
                        .next()
                        .await
                        .ok_or_else(|| "peer stream ended before first batch".to_owned())?
                        .map_err(|error| format!("peer first batch: {error}"))?;
                    peer_first_tx.send(()).map_err(|error| error.to_string())?;
                    recv(&drop_done_rx, deadline, "peer-await-drop");
                    let mut batches = vec![first];
                    while let Some(batch) = stream.next().await {
                        batches.push(batch.map_err(|error| format!("peer batch: {error}"))?);
                    }
                    logical_fingerprint(&batches).map_err(|error| error.to_string())
                });
                let _ = peer_result_tx.send(outcome);
            })
            .unwrap_or_else(|error| panic!("case={case} phase=peer spawn: {error}"));

        recv(&drop_ready_rx, deadline, "main-drop-ready");
        recv(&peer_ready_rx, deadline, "main-peer-ready");
        start_drop_tx.send(()).expect("phase=main start drop");
        start_peer_tx.send(()).expect("phase=main start peer");
        let dropped_rows = recv(&drop_result_rx, deadline, "main-drop-result")
            .unwrap_or_else(|error| panic!("case={case} drop failed: {error}"));
        assert!(dropped_rows > 0, "case={case} dropped batch was empty");
        let actual = recv(&peer_result_rx, deadline, "main-peer-result")
            .unwrap_or_else(|error| panic!("case={case} peer failed: {error}"));
        assert_eq!(actual, expected, "case={case} peer canonical Arrow changed");
        drop_handle
            .join()
            .unwrap_or_else(|_| panic!("case={case} drop worker panicked"));
        peer_handle
            .join()
            .unwrap_or_else(|_| panic!("case={case} peer worker panicked"));
    }
}

#[test]
fn cooperative_token_cancellation_does_not_cancel_concurrent_peer() {
    let (_project, graph, expected) = fixture();
    for case in 0..CASES {
        let deadline = Instant::now() + DEADLINE;
        let peer_stream = graph
            .execute_stream(QUERY)
            .unwrap_or_else(|error| panic!("case={case} phase=peer create stream: {error}"));
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let worker_graph = Arc::clone(&graph);
        let (start_cancel_tx, start_cancel_rx) = mpsc::sync_channel(1);
        let (start_peer_tx, start_peer_rx) = mpsc::sync_channel(1);
        let (cancel_ready_tx, cancel_ready_rx) = mpsc::sync_channel(1);
        let (peer_ready_tx, peer_ready_rx) = mpsc::sync_channel(1);
        let (peer_first_tx, peer_first_rx) = mpsc::sync_channel(1);
        let (cancel_done_tx, cancel_done_rx) = mpsc::sync_channel(1);
        let (cancel_result_tx, cancel_result_rx) = mpsc::sync_channel(1);
        let (peer_result_tx, peer_result_rx) = mpsc::sync_channel(1);

        let cancel_handle = thread::Builder::new()
            .name(format!("gf-token-cancel-{case}"))
            .spawn(move || {
                cancel_ready_tx.send(()).expect("phase=cancel ready");
                recv(&start_cancel_rx, deadline, "cancel-start");
                recv(&peer_first_rx, deadline, "cancel-await-peer-first");
                worker_token.cancel();
                let outcome = worker_graph
                    .list_checkpoints(ListCheckpointsRequest {
                        page: PageRequest {
                            limit: 1,
                            after: None,
                            cancellation: Some(worker_token),
                        },
                    })
                    .map(|_| "unexpected-success".to_owned())
                    .unwrap_or_else(|error| error.code().to_owned());
                cancel_done_tx.send(()).expect("phase=cancel done");
                let _ = cancel_result_tx.send(outcome);
            })
            .unwrap_or_else(|error| panic!("case={case} phase=cancel spawn: {error}"));

        let peer_handle = thread::Builder::new()
            .name(format!("gf-cancel-peer-{case}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("phase=peer build runtime");
                peer_ready_tx.send(()).expect("phase=peer ready");
                recv(&start_peer_rx, deadline, "peer-start");
                let mut stream = peer_stream;
                let outcome = runtime.block_on(async move {
                    let first = stream
                        .next()
                        .await
                        .ok_or_else(|| "peer stream ended before first batch".to_owned())?
                        .map_err(|error| format!("peer first batch: {error}"))?;
                    peer_first_tx.send(()).map_err(|error| error.to_string())?;
                    recv(&cancel_done_rx, deadline, "peer-await-cancel");
                    let mut batches = vec![first];
                    while let Some(batch) = stream.next().await {
                        batches.push(batch.map_err(|error| format!("peer batch: {error}"))?);
                    }
                    logical_fingerprint(&batches).map_err(|error| error.to_string())
                });
                let _ = peer_result_tx.send(outcome);
            })
            .unwrap_or_else(|error| panic!("case={case} phase=peer spawn: {error}"));

        recv(&cancel_ready_rx, deadline, "main-cancel-ready");
        recv(&peer_ready_rx, deadline, "main-peer-ready");
        start_cancel_tx.send(()).expect("phase=main start cancel");
        start_peer_tx.send(()).expect("phase=main start peer");
        assert_eq!(
            recv(&cancel_result_rx, deadline, "main-cancel-result"),
            "GF_CANCELLED",
            "case={case} cancellation outcome changed"
        );
        assert!(token.is_cancelled(), "case={case} token was not cancelled");
        let actual = recv(&peer_result_rx, deadline, "main-peer-result")
            .unwrap_or_else(|error| panic!("case={case} peer failed: {error}"));
        assert_eq!(actual, expected, "case={case} peer canonical Arrow changed");
        cancel_handle
            .join()
            .unwrap_or_else(|_| panic!("case={case} cancel worker panicked"));
        peer_handle
            .join()
            .unwrap_or_else(|_| panic!("case={case} peer worker panicked"));
    }
}
