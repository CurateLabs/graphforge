//! GraphForge bounded, atomic Parquet and Arrow IPC result sinks.
//!
//! Streams Arrow record batches into the formats enumerated by [`ResultSinkFormat`],
//! with bounded buffering, cancellation, progress, and atomic destination publication.
//! Sink failures use [`ResultSinkError`].
#![forbid(unsafe_code)]

use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
use std::cell::Cell;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

#[cfg(test)]
thread_local! {
    static FAIL_WRITE_AFTER_BATCHES: Cell<u64> = const { Cell::new(u64::MAX) };
}

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

fn check_sink_cancellation(
    cancelled: bool,
    started: Instant,
    rows: u64,
    batches: u64,
    temporary: &tempfile::NamedTempFile,
) -> Result<(), ResultSinkError> {
    if cancelled {
        Err(failure(
            started,
            "cancelled",
            rows,
            batches,
            temp_bytes(temporary),
            "operation was cancelled",
        ))
    } else {
        Ok(())
    }
}

async fn drain_stream<E, F, O>(
    stream: &mut Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
    writer: &mut IncrementalWriter,
    schema: &SchemaRef,
    options: &ResultSinkOptions,
    ownership: &mut ObservedSinkGuard<O>,
    started: Instant,
    cancelled: &mut F,
) -> Result<(u64, u64), ResultSinkError>
where
    E: Display,
    F: FnMut() -> bool,
    O: FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>,
{
    let temporary = ownership
        .temporary
        .as_ref()
        .expect("retained sink temporary");
    let observed = &mut ownership.observed;
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    loop {
        check_sink_cancellation(cancelled(), started, rows, batches, temporary)?;
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
        check_sink_cancellation(cancelled(), started, rows, batches, temporary)?;
        #[cfg(test)]
        if FAIL_WRITE_AFTER_BATCHES.with(|limit| batches >= limit.get()) {
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
        observed(temporary.path(), Some(temporary.as_file())).map_err(|error| {
            failure(
                started,
                "observe",
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
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
    schema: SchemaRef,
    destination: &Path,
    format: ResultSinkFormat,
    options: &ResultSinkOptions,
    cancelled: F,
) -> Result<ResultSinkReceipt, ResultSinkError>
where
    E: Display,
    F: FnMut() -> bool,
{
    sink_record_batch_stream_observed(
        stream,
        schema,
        destination,
        format,
        options,
        cancelled,
        |_, _| Ok(()),
    )
    .await
}

/// First-party file ownership observations at the existing result writer boundaries.
#[doc(hidden)]
pub async fn sink_record_batch_stream_observed<E, F, O>(
    mut stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
    schema: SchemaRef,
    destination: &Path,
    format: ResultSinkFormat,
    options: &ResultSinkOptions,
    mut cancelled: F,
    observed: O,
) -> Result<ResultSinkReceipt, ResultSinkError>
where
    E: Display,
    F: FnMut() -> bool,
    O: FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>,
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
    let mut ownership = ObservedSinkGuard {
        temporary: Some(temporary),
        observed,
    };
    let (rows, batches) = ownership
        .write_stream(
            &mut stream,
            &schema,
            format,
            options,
            &mut cancelled,
            started,
        )
        .await?;
    let temporary = ownership
        .temporary
        .as_ref()
        .expect("retained sink temporary");
    let mut observed = &mut ownership.observed;
    let temporary_path = temporary.path().to_path_buf();
    let final_bytes = temp_bytes(temporary);
    let temporary = ownership.temporary.take().expect("retained sink temporary");
    let published = match temporary.persist(destination) {
        Ok(published) => published,
        Err(error) => {
            let failure = failure(
                started,
                "publish",
                rows,
                batches,
                final_bytes,
                error.error.to_string(),
            );
            cleanup_observed_sink(error.file, &mut observed);
            return Err(failure);
        }
    };
    observed(&temporary_path, None).map_err(|error| {
        failure(
            started,
            "published-observe",
            rows,
            batches,
            final_bytes,
            error,
        )
    })?;
    observed(destination, Some(&published)).map_err(|error| {
        failure(
            started,
            "published-observe",
            rows,
            batches,
            final_bytes,
            error,
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

struct ObservedSinkGuard<O: FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>> {
    temporary: Option<tempfile::NamedTempFile>,
    observed: O,
}
impl<O: FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>> ObservedSinkGuard<O> {
    async fn write_stream<E: Display, F: FnMut() -> bool>(
        &mut self,
        stream: &mut Pin<Box<dyn Stream<Item = Result<RecordBatch, E>> + Send>>,
        schema: &SchemaRef,
        format: ResultSinkFormat,
        options: &ResultSinkOptions,
        cancelled: &mut F,
        started: Instant,
    ) -> Result<(u64, u64), ResultSinkError> {
        let writer_file = self
            .temporary
            .as_ref()
            .expect("retained sink temporary")
            .reopen()
            .map_err(|error| failure(started, "create", 0, 0, 0, error.to_string()))?;
        let mut writer = create_writer(writer_file, schema, format, options)
            .map_err(|error| failure(started, "create", 0, 0, 0, error))?;
        let (rows, batches) = drain_stream(
            stream,
            &mut writer,
            schema,
            options,
            self,
            started,
            cancelled,
        )
        .await?;
        let temporary = self.temporary.as_ref().expect("retained sink temporary");
        writer.finish().map_err(|error| {
            failure(
                started,
                "finish",
                rows,
                batches,
                temp_bytes(temporary),
                error,
            )
        })?;
        temporary.as_file().sync_all().map_err(|error| {
            failure(
                started,
                "sync",
                rows,
                batches,
                temp_bytes(temporary),
                error.to_string(),
            )
        })?;
        (self.observed)(temporary.path(), Some(temporary.as_file())).map_err(|error| {
            failure(
                started,
                "observe",
                rows,
                batches,
                temp_bytes(temporary),
                error,
            )
        })?;
        Ok((rows, batches))
    }
}

impl<O: FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>> Drop for ObservedSinkGuard<O> {
    fn drop(&mut self) {
        if let Some(temporary) = self.temporary.take() {
            cleanup_observed_sink(temporary, &mut self.observed);
        }
    }
}

fn cleanup_observed_sink(
    temporary: tempfile::NamedTempFile,
    observed: &mut impl FnMut(&Path, Option<&std::fs::File>) -> Result<(), String>,
) {
    let path = temporary.path().to_path_buf();
    let _ = observed(&path, Some(temporary.as_file()));
    // Never remove evidence for a retained file after a failed unlink.
    if temporary.close().is_ok() {
        let _ = observed(&path, None);
    }
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
    fn dropping_pending_observed_sink_releases_temporary_ownership() {
        use std::future::Future;
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("result.arrow");
        let (schema, batches) = fixture();
        let input =
            stream::iter(vec![Ok::<_, String>(batches[0].clone())]).chain(stream::pending());
        let live = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
        let reported = Arc::clone(&live);
        let options = ResultSinkOptions::default();
        let mut future = Box::pin(sink_record_batch_stream_observed(
            Box::pin(input),
            schema,
            &destination,
            ResultSinkFormat::ArrowIpc,
            &options,
            || false,
            move |path, file| {
                let mut live = reported.lock().unwrap();
                if let Some(file) = file {
                    live.insert(path.to_path_buf(), file.metadata().unwrap().len());
                } else {
                    assert!(!path.exists());
                    live.remove(path);
                }
                Ok(())
            },
        ));
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert!(live.lock().unwrap().values().any(|bytes| *bytes > 0));
        drop(future);
        assert!(live.lock().unwrap().is_empty());
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn observed_partial_failure_and_cancellation_release_only_removed_temporary() {
        for cancel in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let destination = root.path().join("result.arrow");
            std::fs::write(&destination, b"original destination").unwrap();
            let (schema, batches) = fixture();
            let stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>> =
                Box::pin(stream::iter(vec![
                    Ok(batches[0].clone()),
                    Err("intentional execution failure".to_owned()),
                ]));
            let seen_write = std::cell::Cell::new(false);
            let mut live = std::collections::BTreeMap::new();
            let mut observed_nonempty = false;
            let error = futures::executor::block_on(sink_record_batch_stream_observed(
                stream,
                schema,
                &destination,
                ResultSinkFormat::ArrowIpc,
                &ResultSinkOptions::default(),
                || cancel && seen_write.get(),
                |path, file| {
                    if let Some(file) = file {
                        let bytes = file.metadata().unwrap().len();
                        observed_nonempty |= bytes > 0;
                        live.insert(path.to_path_buf(), bytes);
                        seen_write.set(true);
                    } else {
                        assert!(!path.exists());
                        assert!(live.remove(path).is_some());
                    }
                    Ok(())
                },
            ))
            .unwrap_err();
            assert_eq!(error.phase, if cancel { "cancelled" } else { "execute" });
            if !cancel {
                assert!(error.to_string().contains("intentional execution failure"));
            }
            assert!(observed_nonempty);
            assert!(live.is_empty());
            assert_eq!(
                std::fs::read(&destination).unwrap(),
                b"original destination"
            );
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
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
        FAIL_WRITE_AFTER_BATCHES.set(1);
        let error = futures::executor::block_on(sink_record_batch_stream(
            boxed(batches),
            schema,
            &disk_full,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            || false,
        ))
        .unwrap_err();
        FAIL_WRITE_AFTER_BATCHES.set(u64::MAX);
        assert_eq!(error.phase, "write");
        assert_eq!(error.rows, 2);
        assert!(!disk_full.exists());
    }
}
