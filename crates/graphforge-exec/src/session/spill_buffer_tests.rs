//! A channel slot must precede producing the IPC batch that occupies it.
use super::*;
use arrow::array::Int64Array;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{
    RecordBatchStream, common::spawn_buffered, stream::RecordBatchStreamAdapter,
};
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

struct Probe {
    inner: SendableRecordBatchStream,
    starts: Arc<AtomicUsize>,
    active: bool,
    completed: usize,
    after_two: Option<oneshot::Sender<usize>>,
    after_four: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
    drop_release: Option<std::sync::mpsc::Receiver<()>>,
}
impl Stream for Probe {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.active {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.active = true;
        }
        let result = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(_)) = &result {
            self.active = false;
            self.completed += 1;
            if self.completed == 4 {
                if let Some(tx) = self.after_four.take() {
                    // The sole worker runs this only after the producer has
                    // completed its original send and awaits the next permit.
                    tokio::spawn(async move {
                        tx.send(()).ok();
                    });
                }
            }
            if self.completed == 2 {
                let tx = self.after_two.take().unwrap();
                let starts = Arc::clone(&self.starts);
                // This task runs on the same sole worker. The immediately-ready
                // producer cannot be preempted before its next Pending await.
                // Old code starts item3 then blocks send; corrected code blocks
                // reserve before polling item3. Capacity2 is below Tokio's
                // cooperative budget, so no sleep/timing oracle is involved.
                tokio::spawn(async move {
                    tx.send(starts.load(Ordering::SeqCst)).ok();
                });
            }
        }
        result
    }
}
impl RecordBatchStream for Probe {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(tx) = self.dropped.take() {
            tx.send(()).ok();
        }
        if let Some(release) = self.drop_release.take() {
            release.recv().unwrap();
        }
    }
}
fn probe(
    inner: SendableRecordBatchStream,
) -> (
    SendableRecordBatchStream,
    oneshot::Receiver<usize>,
    oneshot::Receiver<()>,
) {
    let (tx, ready) = oneshot::channel();
    let (dropped, done) = oneshot::channel();
    (
        Box::pin(Probe {
            inner,
            starts: Arc::new(AtomicUsize::new(0)),
            active: false,
            completed: 0,
            after_two: Some(tx),
            after_four: None,
            dropped: Some(dropped),
            drop_release: None,
        }),
        ready,
        done,
    )
}
fn synthetic() -> SendableRecordBatchStream {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = |value| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    };
    let items = vec![
        Ok(batch(0)),
        Ok(batch(1)),
        Err(datafusion::common::DataFusionError::Execution(
            "buffered error item".into(),
        )),
        Ok(batch(2)),
    ];
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(items),
    ))
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn spill_admission_buffer_reserves_before_poll_and_preserves_fifo_error_and_eof() -> Result<()> {
    runtime().block_on(async {
        let (source, ready, dropped) = probe(synthetic());
        let mut output = spawn_buffered(source, 2);
        assert_eq!(
            ready.await.unwrap(),
            2,
            "capacity2 must not produce a third pending-send item"
        );
        for expected in [0, 1] {
            let batch = output.next().await.unwrap()?;
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        let error = output.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("buffered error item"));
        let batch = output.next().await.unwrap()?;
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert!(output.next().await.is_none());
        dropped.await.unwrap();
        Ok(())
    })
}

#[test]
fn spill_admission_buffer_drop_cancels_full_queue_without_another_source_poll() -> Result<()> {
    runtime().block_on(async {
        let (source, ready, dropped) = probe(synthetic());
        let output = spawn_buffered(source, 2);
        assert_eq!(ready.await.unwrap(), 2);
        drop(output);
        dropped.await.unwrap();
        Ok(())
    })
}

#[test]
fn spill_admission_buffer_bounds_real_ipc_prefetch_and_cleans_up_after_escaped_leaf() -> Result<()>
{
    runtime().block_on(async {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..512)),
                Arc::new(StringArray::from_iter_values(
                    (0..512).map(|i| format!("{i:0128}")),
                )),
            ],
        )?;
        let writer_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let writer = manager(writer_pool, Arc::clone(&schema));
        let file = writer
            .spill_record_batch_and_finish(&vec![batch.clone(); 24], "bounded IPC prefetch")?
            .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(384 * 1024));
        let reader = manager(Arc::clone(&pool), schema);
        let raw = reader.read_spill_as_stream_unbuffered(file, None)?;
        let (source, ready, dropped) = probe(raw);
        let mut output = spawn_buffered(source, 2);
        assert_eq!(
            ready.await.unwrap(),
            2,
            "no third decode may begin outside the configured channel slots"
        );
        let first = output.next().await.unwrap()?;
        assert_eq!(first, batch);
        let alias = first.column(1).slice(17, 1);
        let body = first
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values()
            .capacity();
        drop(first);
        let mut rows = 512;
        while let Some(next) = output.next().await {
            let next = next?;
            assert_eq!(next, batch);
            rows += next.num_rows();
        }
        assert_eq!(rows, 24 * 512);
        dropped.await.unwrap();
        assert!(pool.reserved() >= body);
        drop(output);
        assert!(pool.reserved() >= body);
        drop(alias);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    })
}

#[test]
fn terminal_ownership_buffer_cancellation_two_calls_distinguishes_requested_and_retired()
-> Result<()> {
    use datafusion_physical_plan::terminal_ownership::{Envelope, Kind, Session};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cancellation-two-call.jsonl");
    let capture = Session::create(&path, Envelope::default())?;
    runtime().block_on(async {
        for _ in 0..2 {
            capture.begin_call()?;
            let (tx, ready) = oneshot::channel();
            let (dropped, done) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let source = Box::pin(Probe {
                inner: synthetic(),
                starts: Arc::new(AtomicUsize::new(0)),
                active: false,
                completed: 0,
                after_two: Some(tx),
                after_four: None,
                dropped: Some(dropped),
                drop_release: Some(wait),
            });
            let output = spawn_buffered(source, 2);
            assert_eq!(ready.await.unwrap(), 2);
            drop(output);
            // This signal is the original input's Drop entry. Its real fields
            // and the producer sentinel are deliberately still live here.
            done.await.unwrap();
            capture.snapshot(0);
            release.send(()).unwrap();
            capture.wait_for_retirement(Kind::Producer)?;
            capture.snapshot(0);
            capture.end_call(0);
        }
        Ok::<_, datafusion::common::DataFusionError>(())
    })?;
    capture.finish()?;
    let text = std::fs::read_to_string(&path)?;
    let cuts: Vec<_> = text
        .lines()
        .filter(|line| line.contains("\"kind\":\"snapshot_begin\""))
        .collect();
    assert_eq!(cuts.len(), 4);
    for pair in cuts.chunks_exact(2) {
        assert!(!pair[0].contains("\"in_flight\":0,"));
        assert!(pair[1].contains("\"in_flight\":0,"));
    }
    let producers: Vec<_> = text
        .lines()
        .filter(|line| line.contains("\"kind\":\"owner\"") && line.contains("\"owner_kind\":13,"))
        .collect();
    assert_eq!(producers.len(), 2);
    assert!(producers.iter().all(|owner| owner.contains("\"c\":1,")));
    if let Some(out) = std::env::var_os("GRAPHFORGE_OWNERSHIP_EVIDENCE_DIR") {
        std::fs::create_dir_all(&out)?;
        std::fs::copy(
            path,
            std::path::Path::new(&out).join("cancellation-two-call.jsonl"),
        )?;
    }
    Ok(())
}

#[test]
fn terminal_ownership_old_producer_cannot_publish_into_a_later_session() -> Result<()> {
    use datafusion_physical_plan::terminal_ownership::{Envelope, Session};
    let directory = tempfile::tempdir()?;
    let first_path = directory.path().join("first.jsonl");
    let second_path = directory.path().join("second.jsonl");
    runtime().block_on(async {
        let first = Session::create(
            &first_path,
            Envelope {
                calls: 1,
                ..Envelope::default()
            },
        )?;
        first.begin_call()?;
        let (tx, ready) = oneshot::channel();
        let (after_four, advanced) = oneshot::channel();
        let (dropped, done) = oneshot::channel();
        let source = Box::pin(Probe {
            inner: synthetic(),
            starts: Arc::new(AtomicUsize::new(0)),
            active: false,
            completed: 0,
            after_two: Some(tx),
            after_four: Some(after_four),
            dropped: Some(dropped),
            drop_release: None,
        });
        let mut output = spawn_buffered(source, 2);
        assert_eq!(ready.await.unwrap(), 2);
        first.snapshot(0);
        first.snapshot(0);
        first.end_call(0);
        first.finish()?;
        let second = Session::create(
            &second_path,
            Envelope {
                calls: 1,
                ..Envelope::default()
            },
        )?;
        second.begin_call()?;
        // Both reads free an existing old-session slot. The same old producer
        // then resumes polling and sends a fresh fourth batch after rotation.
        output.next().await.unwrap()?;
        output.next().await.unwrap()?;
        advanced.await.unwrap();
        second.snapshot(0);
        drop(output);
        done.await.unwrap();
        second.snapshot(0);
        second.end_call(0);
        second.finish()?;
        Ok::<_, datafusion::common::DataFusionError>(())
    })?;
    let text = std::fs::read_to_string(second_path)?;
    assert!(!text.lines().any(|line| line.contains("\"kind\":\"owner\"")));
    let cuts: Vec<_> = text
        .lines()
        .filter(|line| line.contains("\"kind\":\"snapshot_begin\""))
        .collect();
    assert_eq!(cuts.len(), 2);
    assert!(
        cuts.iter()
            .all(|line| line.contains("\"constructed\":0,") && line.contains("\"in_flight\":0,"))
    );
    Ok(())
}
