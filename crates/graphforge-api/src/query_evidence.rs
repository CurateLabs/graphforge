//! Sanitized ordinary-query work evidence.

use arrow::array::{Array, Int64Array, UInt64Array};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, Write};

use crate::{
    CancellationToken, GfError, GraphForge, IrLiteral, ResultSinkFormat, ResultSinkOptions,
    ResultSinkReceipt,
};

/// Versioned aggregate-only query evidence emitted by ordinary execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueryExecutionEvidence {
    /// Evidence schema identifier.
    pub contract: &'static str,
    /// Per-hop physical work in stable plan order.
    pub hops: Vec<QueryHopEvidence>,
    /// Fetch-aware physical sort work.
    pub sorts: Vec<QuerySortEvidence>,
    /// Sanitized operator RSS samples.
    pub operator_rss: Vec<QueryOperatorRssEvidence>,
    /// Maximum concurrent filtered reads.
    pub max_in_flight_reads: u64,
    /// Query memory reservation before execution.
    pub memory_reserved_before: u64,
    /// Query memory reservation after every stream was released.
    pub memory_reserved_after: u64,
    /// Arrow bytes retained by returned batches while the sink consumed them.
    pub returned_batch_bytes: u64,
    /// Configured physical execution batch-row bound.
    pub execution_batch_rows: u64,
    /// Largest observed operator RSS sample.
    pub peak_rss_bytes: u64,
    /// Largest operator RSS sample after stream release.
    pub rss_after_release_bytes: u64,
}

/// Aggregate deterministic counters for one fixed hop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueryHopEvidence {
    /// Stable hop ordinal, independent of internal variable identifiers.
    pub ordinal: usize,
    /// Input batches pulled by the hop.
    pub input_batches: u64,
    /// Input rows pulled by the hop.
    pub input_rows: u64,
    /// Adjacency candidates examined before projection.
    pub candidates_generated: u64,
    /// Rows emitted by the hop.
    pub rows_emitted: u64,
    /// Chunks served by destination-only projection.
    pub projected_chunks: u64,
    /// Rows served by destination-only projection.
    pub projected_rows: u64,
    /// Required output columns at this hop.
    pub projected_columns: u64,
    /// Required edge columns.
    pub edge_projected_columns: u64,
    /// Required destination-node columns.
    pub node_projected_columns: u64,
    /// Edge reader calls opened.
    pub edge_reader_calls: u64,
    /// Edge rows returned by readers.
    pub edge_rows_returned: u64,
    /// Edge rows evaluated by physical readers.
    pub edge_logical_rows_scanned: u64,
    /// Edge reads that fell back to full materialization.
    pub edge_full_reads: u64,
    /// Node reader calls opened.
    pub node_reader_calls: u64,
    /// Node rows returned by readers.
    pub node_rows_returned: u64,
    /// Node rows evaluated by physical readers.
    pub node_logical_rows_scanned: u64,
    /// Node reads that fell back to full materialization.
    pub node_full_reads: u64,
    /// Bounded identity reader calls.
    pub identity_reader_calls: u64,
    /// Logical identity bytes read.
    pub identity_logical_bytes: u64,
    /// Coalesced ordinal ranges selected.
    pub identity_ranges_selected: u64,
    /// Largest identity request or transient buffer.
    pub identity_peak_buffer_bytes: u64,
    /// Forbidden per-record identity seeks.
    pub identity_per_record_seeks: u64,
    /// Generation-authority validation calls.
    pub identity_revalidation_calls: u64,
    /// Bytes read while validating identity authority.
    pub identity_revalidation_bytes: u64,
}

/// Aggregate deterministic counters for one fetch-aware sort.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QuerySortEvidence {
    /// Stable sort ordinal.
    pub ordinal: usize,
    /// Physical TopK bound, or `None` for an unbounded sort.
    pub fetch_rows: Option<usize>,
    /// Rows observed by the sort.
    pub output_rows: u64,
    /// Spill operations performed.
    pub spill_count: u64,
    /// Rows written to spill files.
    pub spilled_rows: u64,
    /// Bytes written to spill files.
    pub spilled_bytes: u64,
    /// Sort memory retained after stream release.
    pub retained_bytes: u64,
}

/// Content-free RSS lifetime for one physical operator class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueryOperatorRssEvidence {
    /// Stable operator ordinal.
    pub ordinal: usize,
    /// Sanitized physical operator class.
    pub operator: &'static str,
    /// RSS before the operator stream was created.
    pub before_bytes: u64,
    /// Largest RSS sample while the stream lived.
    pub peak_bytes: u64,
    /// RSS after the operator stream was released.
    pub after_bytes: u64,
}

/// Atomic result-sink publication plus ordinary-query evidence.
#[derive(Debug)]
pub struct QuerySinkEvidenceReceipt {
    /// Atomic result-sink publication receipt.
    pub sink: ResultSinkReceipt,
    /// SHA-256 of the atomically published result artifact.
    pub result_sha256: String,
    /// Exact unsigned scalar for a one-row integer result representable as `u64`.
    pub scalar_u64: Option<u64>,
    /// Sanitized ordinary physical-query evidence.
    pub evidence: QueryExecutionEvidence,
}

struct DigestWriter<'a>(&'a mut Sha256);

impl Write for DigestWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn published_result_metadata(
    path: &std::path::Path,
    format: ResultSinkFormat,
) -> Result<(Option<u64>, String), GfError> {
    let mut rows = 0usize;
    let mut scalar = None;
    let mut scalar_shape = true;
    let mut digest = Sha256::new();
    let mut logical_schema = None;
    let mut observe = |batch: arrow::record_batch::RecordBatch| -> Result<(), GfError> {
        let schema = logical_schema.get_or_insert_with(|| {
            std::sync::Arc::new(arrow::datatypes::Schema::new(
                batch.schema().fields().clone(),
            ))
        });
        let logical_batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(schema),
            batch.columns().to_vec(),
        )
        .map_err(|error| GfError::Storage(error.to_string()))?;
        let mut writer = StreamWriter::try_new(DigestWriter(&mut digest), schema)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        writer
            .write(&logical_batch)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if batch.num_columns() != 1 {
            scalar_shape = false;
        }
        let before = rows;
        rows = rows.saturating_add(batch.num_rows());
        if rows > 1 {
            scalar_shape = false;
            scalar = None;
            return Ok(());
        }
        if before == 0 && batch.num_rows() == 1 && scalar_shape {
            let column = batch.column(0);
            scalar = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .filter(|array| !array.is_null(0))
                .map(|array| array.value(0))
                .or_else(|| {
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .filter(|array| !array.is_null(0))
                        .and_then(|array| u64::try_from(array.value(0)).ok())
                });
        }
        Ok(())
    };
    match format {
        ResultSinkFormat::Parquet => {
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).map_err(|error| GfError::Storage(error.to_string()))?,
            )
            .map_err(|error| GfError::Storage(error.to_string()))?
            .build()
            .map_err(|error| GfError::Storage(error.to_string()))?;
            for batch in reader {
                observe(batch.map_err(|error| GfError::Storage(error.to_string()))?)?;
            }
        }
        ResultSinkFormat::ArrowIpc => {
            let reader = StreamReader::try_new(
                BufReader::new(
                    File::open(path).map_err(|error| GfError::Storage(error.to_string()))?,
                ),
                None,
            )
            .map_err(|error| GfError::Storage(error.to_string()))?;
            for batch in reader {
                observe(batch.map_err(|error| GfError::Storage(error.to_string()))?)?;
            }
        }
    }
    Ok((
        (rows == 1 && scalar_shape).then_some(scalar).flatten(),
        hex_digest(&digest.finalize()),
    ))
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

impl From<graphforge_exec::demand::DemandSnapshot> for QueryExecutionEvidence {
    fn from(snapshot: graphforge_exec::demand::DemandSnapshot) -> Self {
        let hops = snapshot
            .hops
            .into_values()
            .enumerate()
            .map(|(ordinal, hop)| QueryHopEvidence {
                ordinal,
                input_batches: hop.input_batches,
                input_rows: hop.input_rows,
                candidates_generated: hop.candidates_generated,
                rows_emitted: hop.rows_emitted,
                projected_chunks: hop.projected_chunks,
                projected_rows: hop.projected_rows,
                projected_columns: hop.projected_columns,
                edge_projected_columns: hop.edge_projected_columns,
                node_projected_columns: hop.node_projected_columns,
                edge_reader_calls: hop.edge_reads_started,
                edge_rows_returned: hop.edge_rows_returned,
                edge_logical_rows_scanned: hop.edge_rows_scanned,
                edge_full_reads: hop.edge_full_reads,
                node_reader_calls: hop.node_reads_started,
                node_rows_returned: hop.node_rows_returned,
                node_logical_rows_scanned: hop.node_rows_scanned,
                node_full_reads: hop.node_full_reads,
                identity_reader_calls: hop.identity_read_calls,
                identity_logical_bytes: hop.identity_bytes_read,
                identity_ranges_selected: hop.identity_ranges_selected,
                identity_peak_buffer_bytes: hop.identity_peak_buffer_bytes,
                identity_per_record_seeks: hop.identity_per_record_seeks,
                identity_revalidation_calls: hop.identity_revalidation_calls,
                identity_revalidation_bytes: hop.identity_revalidation_bytes,
            })
            .collect();
        let sorts = snapshot
            .sorts
            .into_iter()
            .map(|sort| QuerySortEvidence {
                ordinal: sort.ordinal,
                fetch_rows: sort.fetch,
                output_rows: sort.output_rows,
                spill_count: sort.spill_count,
                spilled_rows: sort.spilled_rows,
                spilled_bytes: sort.spilled_bytes,
                retained_bytes: sort.retained_bytes,
            })
            .collect();
        let operator_rss = snapshot
            .operator_rss
            .into_iter()
            .map(|operator| QueryOperatorRssEvidence {
                ordinal: operator.ordinal,
                operator: operator.operator,
                before_bytes: operator.before_bytes,
                peak_bytes: operator.peak_bytes,
                after_bytes: operator.after_bytes,
            })
            .collect::<Vec<_>>();
        let peak_rss_bytes = operator_rss
            .iter()
            .map(|operator| operator.peak_bytes)
            .max()
            .unwrap_or(0);
        let rss_after_release_bytes = operator_rss
            .iter()
            .map(|operator| operator.after_bytes)
            .max()
            .unwrap_or(0);
        Self {
            contract: "graphforge-query-evidence/1",
            hops,
            sorts,
            operator_rss,
            max_in_flight_reads: snapshot.max_in_flight_reads,
            memory_reserved_before: snapshot.memory_reserved_before,
            memory_reserved_after: snapshot.memory_reserved_after,
            returned_batch_bytes: snapshot.returned_batch_bytes,
            execution_batch_rows: snapshot.execution_batch_rows,
            peak_rss_bytes,
            rss_after_release_bytes,
        }
    }
}

impl GraphForge {
    /// Execute an ordinary streaming query sink and retain sanitized physical evidence.
    pub fn execute_to_result_sink_with_evidence(
        &self,
        cypher: &str,
        params: &std::collections::HashMap<String, IrLiteral>,
        path: &str,
        format: ResultSinkFormat,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<QuerySinkEvidenceReceipt, GfError> {
        let (sink, evidence) = graphforge_exec::demand::capture(|| {
            self.execute_to_result_sink_with_params(
                cypher,
                params,
                path,
                format,
                options,
                cancellation,
            )
        });
        let sink = sink?;
        let (scalar_u64, result_sha256) =
            published_result_metadata(&sink.destination, sink.format)?;
        Ok(QuerySinkEvidenceReceipt {
            sink,
            result_sha256,
            scalar_u64,
            evidence: evidence.into(),
        })
    }
}
