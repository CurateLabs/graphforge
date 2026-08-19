//! Bounded, atomic GraphForge result sinks.
#![forbid(unsafe_code)]

use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

#[cfg(test)]
static FAIL_WRITE_AFTER_BATCHES: AtomicU64 = AtomicU64::new(u64::MAX);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// On-disk representation for a streamed query result.
pub enum ResultSinkFormat {
    /// Apache Parquet.
    Parquet,
    /// Arrow IPC streaming format.
    ArrowIpc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Resource controls for incremental writers.
pub struct ResultSinkOptions {
    /// Maximum rows buffered in one Parquet row group.
    pub max_row_group_rows: usize,
    /// Maximum rows accepted in one execution batch.
    pub max_batch_rows: usize,
}

impl Default for ResultSinkOptions {
    fn default() -> Self {
        Self {
            max_row_group_rows: 65_536,
            max_batch_rows: 65_536,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Work counters and terminal state for a sink.
pub struct ResultSinkProgress {
    /// Current or terminal phase.
    pub phase: &'static str,
    /// Rows accepted by the writer.
    pub rows: u64,
    /// Batches accepted by the writer.
    pub batches: u64,
    /// Bytes written to the output.
    pub bytes: u64,
    /// Wall-clock duration.
    pub elapsed: Duration,
    /// True only after atomic publication.
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Successful atomic publication receipt.
pub struct ResultSinkReceipt {
    /// Final destination.
    pub destination: PathBuf,
    /// Published representation.
    pub format: ResultSinkFormat,
    /// Terminal progress.
    pub progress: ResultSinkProgress,
}

#[derive(Debug, Error)]
/// Failed sink state with bounded progress and no completion claim.
#[error(
    "result sink failed during {phase}: {message} (rows={rows}, batches={batches}, bytes={bytes}, elapsed_ms={elapsed_ms})"
)]
pub struct ResultSinkError {
    /// Failure phase.
    pub phase: &'static str,
    /// Rows accepted before failure.
    pub rows: u64,
    /// Batches accepted before failure.
    pub batches: u64,
    /// Temporary bytes observed before failure.
    pub bytes: u64,
    /// Milliseconds elapsed before failure.
    pub elapsed_ms: u128,
    /// Sanitized underlying failure.
    pub message: String,
}

fn failure(
    started: Instant,
    phase: &'static str,
    rows: u64,
    batches: u64,
    bytes: u64,
    message: impl Into<String>,
) -> ResultSinkError {
    ResultSinkError {
        phase,
        rows,
        batches,
        bytes,
        elapsed_ms: started.elapsed().as_millis(),
        message: message.into(),
    }
}

enum IncrementalWriter {
    Parquet(Box<ArrowWriter<File>>),
    ArrowIpc(StreamWriter<File>),
}

fn create_writer(
    file: File,
    schema: &SchemaRef,
    format: ResultSinkFormat,
    options: &ResultSinkOptions,
) -> Result<IncrementalWriter, String> {
    match format {
        ResultSinkFormat::Parquet => {
            let mut metadata = schema
                .metadata()
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), Some(value.clone())))
                .collect::<Vec<_>>();
            metadata.sort_unstable_by(|left, right| left.key.cmp(&right.key));
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(options.max_row_group_rows))
                .set_key_value_metadata(Some(metadata))
                .build();
            ArrowWriter::try_new(file, Arc::clone(schema), Some(properties))
                .map(Box::new)
                .map(IncrementalWriter::Parquet)
                .map_err(|error| error.to_string())
        }
        ResultSinkFormat::ArrowIpc => StreamWriter::try_new(file, schema.as_ref())
            .map(IncrementalWriter::ArrowIpc)
            .map_err(|error| error.to_string()),
    }
}

fn temp_bytes(temporary: &tempfile::NamedTempFile) -> u64 {
    temporary
        .as_file()
        .metadata()
        .map_or(0, |metadata| metadata.len())
}

async fn drain_stream<E, F>(
    stream: &mut Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
    writer: &mut IncrementalWriter,
    schema: &SchemaRef,
    options: &ResultSinkOptions,
    temporary: &tempfile::NamedTempFile,
    started: Instant,
    cancelled: &mut F,
) -> Result<(u64, u64), ResultSinkError>
where
    E: Display,
    F: FnMut() -> bool,
{
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    loop {
        if cancelled() {
            return Err(failure(
                started,
                "cancelled",
                rows,
                batches,
                temp_bytes(temporary),
                "operation was cancelled",
            ));
        }
        let Some(item) = stream.next().await else {
            break;
        };
        let batch = item.map_err(|error| {
            failure(
                started,
                "execute",
                rows,
                batches,
                temp_bytes(temporary),
                error.to_string(),
            )
        })?;
        if batch.schema() != *schema {
            return Err(failure(
                started,
                "schema",
                rows,
                batches,
                temp_bytes(temporary),
                "execution batch schema changed during export",
            ));
        }
        if batch.num_rows() > options.max_batch_rows {
            return Err(failure(
                started,
                "limit",
                rows,
                batches,
                temp_bytes(temporary),
                format!(
                    "execution batch has {} rows, exceeding max_batch_rows {}",
                    batch.num_rows(),
                    options.max_batch_rows
                ),
            ));
        }
        if cancelled() {
            return Err(failure(
                started,
                "cancelled",
                rows,
                batches,
                temp_bytes(temporary),
                "operation was cancelled",
            ));
        }
        #[cfg(test)]
        if batches >= FAIL_WRITE_AFTER_BATCHES.load(Ordering::Relaxed) {
            return Err(failure(
                started,
                "write",
                rows,
                batches,
                temp_bytes(temporary),
                "simulated disk exhaustion",
            ));
        }
        writer.write(&batch).map_err(|error| {
            failure(
                started,
                "write",
                rows,
                batches,
                temp_bytes(temporary),
                error,
            )
        })?;
        rows = rows.saturating_add(batch.num_rows() as u64);
        batches = batches.saturating_add(1);
    }
    Ok((rows, batches))
}

impl IncrementalWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match self {
            Self::Parquet(writer) => writer.write(batch).map_err(|error| error.to_string()),
            Self::ArrowIpc(writer) => writer.write(batch).map_err(|error| error.to_string()),
        }
    }
    fn finish(self) -> Result<(), String> {
        match self {
            Self::Parquet(writer) => (*writer)
                .close()
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Self::ArrowIpc(mut writer) => writer.finish().map_err(|error| error.to_string()),
        }
    }
}

/// Drain an execution stream one batch at a time and publish only after a
/// successful writer close and file sync. Pulling only after each write gives
/// the writer natural backpressure over query execution.
pub async fn sink_record_batch_stream<E, F>(
    mut stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
    schema: SchemaRef,
    destination: &Path,
    format: ResultSinkFormat,
    options: &ResultSinkOptions,
    mut cancelled: F,
) -> Result<ResultSinkReceipt, ResultSinkError>
where
    E: Display,
    F: FnMut() -> bool,
{
    let started = Instant::now();
    if options.max_row_group_rows == 0 || options.max_batch_rows == 0 {
        return Err(failure(
            started,
            "validate",
            0,
            0,
            0,
            "sink limits must be non-zero",
        ));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = tempfile::Builder::new()
        .prefix(".graphforge-result-")
        .tempfile_in(parent)
        .map_err(|error| failure(started, "create", 0, 0, 0, error.to_string()))?;
    let writer_file = temporary
        .reopen()
        .map_err(|error| failure(started, "create", 0, 0, 0, error.to_string()))?;
    let mut writer = create_writer(writer_file, &schema, format, options)
        .map_err(|error| failure(started, "create", 0, 0, 0, error))?;
    let (rows, batches) = drain_stream(
        &mut stream,
        &mut writer,
        &schema,
        options,
        &temporary,
        started,
        &mut cancelled,
    )
    .await?;
    writer.finish().map_err(|error| {
        failure(
            started,
            "finish",
            rows,
            batches,
            temp_bytes(&temporary),
            error,
        )
    })?;
    temporary.as_file().sync_all().map_err(|error| {
        failure(
            started,
            "sync",
            rows,
            batches,
            temp_bytes(&temporary),
            error.to_string(),
        )
    })?;
    let final_bytes = temp_bytes(&temporary);
    temporary.persist(destination).map_err(|error| {
        failure(
            started,
            "publish",
            rows,
            batches,
            final_bytes,
            error.error.to_string(),
        )
    })?;
    Ok(ResultSinkReceipt {
        destination: destination.to_path_buf(),
        format,
        progress: ResultSinkProgress {
            phase: "complete",
            rows,
            batches,
            bytes: final_bytes,
            elapsed: started.elapsed(),
            complete: true,
        },
    })
}

#[must_use]
/// Return the crate name.
pub const fn name() -> &'static str {
    "graphforge-io"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use futures::stream;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn fixture() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ],
            [("graphforge.ordering".to_owned(), "explicit".to_owned())].into(),
        ));
        let batches = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["a", "b"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![3])),
                    Arc::new(StringArray::from(vec!["c"])),
                ],
            )
            .unwrap(),
        ];
        (schema, batches)
    }

    fn boxed(
        batches: Vec<RecordBatch>,
    ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>> {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }

    #[test]
    fn parquet_and_ipc_round_trip_schema_rows_and_batches() {
        let root = tempfile::tempdir().unwrap();
        for format in [ResultSinkFormat::Parquet, ResultSinkFormat::ArrowIpc] {
            let (schema, batches) = fixture();
            let path = root.path().join(match format {
                ResultSinkFormat::Parquet => "result.parquet",
                ResultSinkFormat::ArrowIpc => "result.arrow",
            });
            let receipt = futures::executor::block_on(sink_record_batch_stream(
                boxed(batches),
                Arc::clone(&schema),
                &path,
                format,
                &ResultSinkOptions {
                    max_row_group_rows: 2,
                    max_batch_rows: 2,
                },
                || false,
            ))
            .unwrap();
            assert_eq!(receipt.progress.rows, 3);
            assert_eq!(receipt.progress.batches, 2);
            assert!(receipt.progress.complete && receipt.progress.bytes > 0);
            let (read_schema, read) = match format {
                ResultSinkFormat::Parquet => {
                    let builder =
                        ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                            .unwrap();
                    let read_schema = Arc::clone(builder.schema());
                    let batches = builder
                        .build()
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    (read_schema, batches)
                }
                ResultSinkFormat::ArrowIpc => {
                    let reader = StreamReader::try_new(File::open(path).unwrap(), None).unwrap();
                    let read_schema = reader.schema();
                    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
                    (read_schema, batches)
                }
            };
            assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            assert_eq!(read_schema, schema);
        }
    }

    #[test]
    fn cancellation_limit_schema_and_destination_fail_without_final_output() {
        let root = tempfile::tempdir().unwrap();
        let (schema, batches) = fixture();
        let cancelled = root.path().join("cancelled.parquet");
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(batches.clone()),
            Arc::clone(&schema),
            &cancelled,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            || true,
        ))
        .unwrap_err();
        assert_eq!(error.phase, "cancelled");
        assert!(!cancelled.exists());

        let limited = root.path().join("limited.arrow");
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(batches.clone()),
            Arc::clone(&schema),
            &limited,
            ResultSinkFormat::ArrowIpc,
            &ResultSinkOptions {
                max_row_group_rows: 1,
                max_batch_rows: 1,
            },
            || false,
        ))
        .unwrap_err();
        assert_eq!(error.phase, "limit");
        assert!(!limited.exists());

        let changed_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let changed =
            RecordBatch::try_new(changed_schema, vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let mismatch = root.path().join("mismatch.arrow");
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(vec![changed]),
            schema,
            &mismatch,
            ResultSinkFormat::ArrowIpc,
            &ResultSinkOptions::default(),
            || false,
        ))
        .unwrap_err();
        assert_eq!(error.phase, "schema");
        assert!(!mismatch.exists());

        let missing = root.path().join("missing").join("result.parquet");
        let (schema, batches) = fixture();
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(batches),
            schema,
            &missing,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            || false,
        ))
        .unwrap_err();
        assert_eq!(error.phase, "create");
        assert!(!missing.exists());

        let disk_full = root.path().join("disk-full.parquet");
        let (schema, batches) = fixture();
        FAIL_WRITE_AFTER_BATCHES.store(1, Ordering::Relaxed);
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(batches),
            schema,
            &disk_full,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            || false,
        ))
        .unwrap_err();
        FAIL_WRITE_AFTER_BATCHES.store(u64::MAX, Ordering::Relaxed);
        assert_eq!(error.phase, "write");
        assert_eq!(error.rows, 2);
        assert!(!disk_full.exists());
    }
}
