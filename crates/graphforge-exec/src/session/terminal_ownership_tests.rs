//! Real Field/Row merges qualify transport and observer non-interference.
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::terminal_ownership::{
    Envelope, GAP_ROWS_STORAGE, Session, verify_recorder_controls,
};
use futures::StreamExt;
use std::sync::{Arc, Mutex};

#[test]
fn terminal_ownership_transport_and_borrowed_layout_controls() {
    verify_recorder_controls();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolOperation {
    kind: &'static str,
    consumer: String,
    held: usize,
    amount: usize,
    before: usize,
    after: usize,
    accepted: bool,
}

#[derive(Debug)]
struct ObservedPool {
    inner: GreedyMemoryPool,
    operations: Mutex<Vec<PoolOperation>>,
}

impl ObservedPool {
    fn new() -> Self {
        Self {
            inner: GreedyMemoryPool::new(16 * 1024 * 1024),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, operation: PoolOperation) {
        self.operations.lock().unwrap().push(operation);
    }

    fn change(
        &self,
        kind: &'static str,
        reservation: &MemoryReservation,
        amount: usize,
        action: impl FnOnce() -> bool,
    ) -> bool {
        let before = self.inner.reserved();
        let held = reservation.size();
        let accepted = action();
        self.record(PoolOperation {
            kind,
            consumer: reservation.consumer().name().to_owned(),
            held,
            amount,
            before,
            after: self.inner.reserved(),
            accepted,
        });
        accepted
    }
}

impl std::fmt::Display for ObservedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("terminal ownership parity pool")
    }
}

impl MemoryPool for ObservedPool {
    fn name(&self) -> &str {
        "terminal ownership parity pool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
        self.record(PoolOperation {
            kind: "register",
            consumer: consumer.name().to_owned(),
            held: 0,
            amount: 0,
            before: self.inner.reserved(),
            after: self.inner.reserved(),
            accepted: true,
        });
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.record(PoolOperation {
            kind: "unregister",
            consumer: consumer.name().to_owned(),
            held: 0,
            amount: 0,
            before: self.inner.reserved(),
            after: self.inner.reserved(),
            accepted: true,
        });
    }

    fn grow(&self, reservation: &MemoryReservation, amount: usize) {
        self.change("grow", reservation, amount, || {
            self.inner.grow(reservation, amount);
            true
        });
    }

    fn try_grow(&self, reservation: &MemoryReservation, amount: usize) -> Result<()> {
        let mut result = Ok(());
        self.change("try_grow", reservation, amount, || {
            result = self.inner.try_grow(reservation, amount);
            result.is_ok()
        });
        result
    }

    fn shrink(&self, reservation: &MemoryReservation, amount: usize) {
        self.change("shrink", reservation, amount, || {
            self.inner.shrink(reservation, amount);
            true
        });
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("primary", DataType::Int64, false),
        Field::new("secondary", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

const PARTITIONS: [[[i64; 3]; 6]; 2] = [
    [
        [0, 0, 0],
        [1, 0, 10],
        [1, 2, 12],
        [2, 0, 20],
        [3, 0, 30],
        [4, 0, 40],
    ],
    [
        [0, 0, 1],
        [1, 1, 11],
        [1, 2, 13],
        [2, 0, 21],
        [3, 1, 31],
        [4, 0, 41],
    ],
];

fn inputs(schema: &SchemaRef) -> Vec<SendableRecordBatchStream> {
    PARTITIONS
        .iter()
        .map(|partition| {
            let batches = partition
                .chunks(2)
                .map(|rows| {
                    let columns = (0..3)
                        .map(|column| {
                            Arc::new(Int64Array::from_iter_values(
                                rows.iter().map(|row| row[column]),
                            )) as ArrayRef
                        })
                        .collect();
                    RecordBatch::try_new(Arc::clone(schema), columns).map_err(Into::into)
                })
                .collect::<Vec<Result<RecordBatch>>>();
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(schema),
                futures::stream::iter(batches),
            )) as SendableRecordBatchStream
        })
        .collect()
}

fn merge(pool: &Arc<dyn MemoryPool>, key_count: usize) -> Result<SendableRecordBatchStream> {
    let schema = schema();
    let ordering = LexOrdering::new(
        (0..key_count)
            .map(|column| {
                PhysicalSortExpr::new_default(Arc::new(Column::new(
                    schema.field(column).name(),
                    column,
                )))
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    StreamingMergeBuilder::new()
        .with_streams(inputs(&schema))
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_batch_size(2)
        .with_round_robin_tie_breaker(false)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_reservation(MemoryConsumer::new("terminal ownership merge").register(pool))
        .build()
}

fn rows(batches: &[RecordBatch]) -> Vec<[i64; 3]> {
    batches
        .iter()
        .flat_map(|batch| {
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect();
            (0..batch.num_rows())
                .map(move |row| std::array::from_fn(|column| columns[column].value(row)))
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    rows: Vec<Vec<[i64; 3]>>,
    operations: Vec<PoolOperation>,
    freed: usize,
}

async fn run(session: Option<&Session>) -> Result<Observation> {
    let observed = Arc::new(ObservedPool::new());
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let mut held_outputs = Vec::new();
    for key_count in 1..=2 {
        if let Some(session) = session {
            assert_eq!(session.begin_call().unwrap(), key_count as u64);
        }
        let mut stream = merge(&pool, key_count)?;
        let first = stream.next().await.unwrap()?;
        assert_eq!(first.num_rows(), 2);
        let mut outputs = vec![first];
        // The first cut has live current/previous keys and builder inputs.
        if let Some(session) = session {
            session.snapshot(0);
        }
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            assert!(batch.num_rows() <= 2);
            outputs.push(batch);
        }
        assert!(stream.next().await.is_none());
        drop(stream);
        // This legacy in-memory path does not claim escaped-output funding.
        // Holding the actual outputs here and through the next call must not
        // turn observed addresses into a claim of funding or complete lineage.
        assert_eq!(pool.reserved(), 0);
        let actual = rows(&outputs);
        assert_eq!(actual.len(), 12);
        assert!(actual.windows(2).all(|pair| {
            if key_count == 1 {
                pair[0][0] <= pair[1][0]
            } else {
                pair[0][..2] <= pair[1][..2]
            }
        }));
        let mut expected: Vec<_> = PARTITIONS.into_iter().flatten().collect();
        let mut unordered = actual.clone();
        expected.sort_unstable();
        unordered.sort_unstable();
        assert_eq!(unordered, expected);
        held_outputs.push(outputs);
        if let Some(session) = session {
            session.snapshot(0);
            session.end_call(0);
        }
    }
    let result_rows = held_outputs.iter().map(|batches| rows(batches)).collect();
    drop(held_outputs);
    assert_eq!(pool.reserved(), 0);
    let operations = observed.operations.lock().unwrap().clone();
    assert!(operations.iter().all(|op| op.accepted));
    let grown = operations
        .iter()
        .filter(|op| matches!(op.kind, "grow" | "try_grow"))
        .map(|op| op.amount)
        .sum::<usize>();
    let freed = operations
        .iter()
        .filter(|op| op.kind == "shrink")
        .map(|op| op.amount)
        .sum::<usize>();
    assert!(grown > 0);
    assert_eq!(grown, freed);
    Ok(Observation {
        rows: result_rows,
        operations,
        freed,
    })
}

fn number(line: &str, field: &str) -> u64 {
    let prefix = format!("\"{field}\":");
    let value = line.split_once(&prefix).unwrap().1;
    let length = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    value[..length].parse().unwrap()
}

#[tokio::test]
async fn terminal_ownership_real_field_and_row_merge_preserve_results_and_pool_operations()
-> Result<()> {
    let baseline = run(None).await?;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ownership.jsonl");
    let limits = Envelope::default();
    let bounds = limits.bounds().unwrap();
    let session = Session::create(&path, limits).unwrap();
    let captured = run(Some(&session)).await?;
    assert_eq!(captured, baseline);
    // Footer commits the bounded transport. Snapshot gaps remain explicit and
    // independently prevent treating this legacy-path control as funded proof.
    session.finish().unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    if let Some(directory) = std::env::var_os("GRAPHFORGE_OWNERSHIP_EVIDENCE_DIR") {
        std::fs::create_dir_all(&directory)?;
        std::fs::copy(
            &path,
            std::path::PathBuf::from(directory).join("field-row-two-call.jsonl"),
        )?;
    }
    let lines: Vec<_> = content.lines().collect();
    assert!(content.len() <= bounds.bytes);
    assert!(lines.len() <= bounds.records);
    for expected in [3, 4, 6, 7, 8] {
        assert!(
            lines.iter().any(|line| line.contains("\"kind\":\"owner\"")
                && number(line, "owner_kind") == expected),
            "missing owner kind {expected}"
        );
    }
    for expected in [36, 37, 38] {
        assert!(
            lines
                .iter()
                .any(|line| line.contains("\"kind\":\"backing\"")
                    && number(line, "type") == expected),
            "missing Row storage type {expected}"
        );
    }
    let begins: Vec<_> = lines
        .iter()
        .filter(|line| line.contains("\"kind\":\"snapshot_begin\""))
        .collect();
    assert_eq!(begins.len(), 4);
    assert_eq!(
        begins
            .iter()
            .map(|line| number(line, "call"))
            .collect::<Vec<_>>(),
        [1, 1, 2, 2]
    );
    assert_eq!(
        begins
            .iter()
            .map(|line| number(line, "snapshot"))
            .collect::<Vec<_>>(),
        [1, 2, 1, 2]
    );
    assert!(begins.iter().all(|line| number(line, "poison") == 0));
    assert!(
        begins
            .iter()
            .all(|line| number(line, "gaps") & GAP_ROWS_STORAGE == 0),
        "supported primitive Row layouts must expose their borrowed storage"
    );
    assert!(
        begins.iter().any(|line| number(line, "gaps") != 0),
        "legacy escaped output requires an explicit semantic gap"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("\"kind\":\"snapshot_end\"")
                && line.contains("\"complete\":false"))
    );
    assert!(lines.last().unwrap().contains("\"kind\":\"session_end\""));
    assert!(lines.last().unwrap().contains("\"complete\":true"));
    Ok(())
}
