//! The per-operation latency clock, the measured pass, and the result digest.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use graphforge_api::{
    ClusterAlgorithm, ClusterOptions, GfError, GraphForge, IrLiteral, NodeSelector, PathAlgorithm,
    PathsOptions, PropValue, RankAlgorithm, RankOptions,
};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::workload::{Binding, Operation, SourceSelector, Variant};
use super::{QueryCause, QueryError};
use crate::identity::hex;

/// The one declared per-operation latency clock. `docs/development/benchmarking.md`
/// names it, and `gdc_measurement_policy.py` rejects latency from anything else.
pub const LATENCY_CLOCK: &str = "graphforge-gdc-query-clock/1";
/// SHA-256 over a canonical rendering of the result; see [`result_digest`].
pub const RESULT_DIGEST: &str = "graphforge-gdc-result-digest/1";
/// One measured result written for a reference check; see [`ResultsDir`].
pub const RESULT_SCHEMA: &str = "graphforge-gdc-query-result/1";
/// Error text kept per failed sample; longer text is cut at a character boundary.
pub const MAX_ERROR_BYTES: usize = 1024;

/// One binding's execution in the measured pass.
#[derive(Debug, Serialize)]
pub struct Sample {
    pub binding_id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

/// A measured sample carries latency; a failed one never does.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Measured(Measured),
    Failed(Failure),
}

#[derive(Debug, Serialize)]
pub struct Measured {
    pub latency_ns: u64,
    pub rows: u64,
    pub result_sha256: String,
}

/// Why one binding produced no measurement.
#[derive(Debug, Serialize)]
pub struct Failure {
    /// `query_failed` when the product call returned an error, or
    /// `result_unrenderable` when its result could not be digested.
    pub cause: &'static str,
    /// The product's stable `GfError::code()`, when the product reported the error.
    pub error_code: Option<&'static str>,
    /// At most [`MAX_ERROR_BYTES`] of the error text.
    pub error: String,
}

impl Sample {
    #[must_use]
    pub fn measured(&self) -> Option<&Measured> {
        match &self.outcome {
            Outcome::Measured(measured) => Some(measured),
            Outcome::Failed(_) => None,
        }
    }

    #[must_use]
    pub fn failure(&self) -> Option<&Failure> {
        match &self.outcome {
            Outcome::Measured(_) => None,
            Outcome::Failed(failure) => Some(failure),
        }
    }
}

/// Nearest-rank percentiles over a variant's measured samples only.
#[derive(Debug, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
}

/// The excluded warm-up pass. It carries no latency, by construction. A failed
/// warm-up is not retried: its binding runs again in the measured pass, where
/// a failure is recorded.
#[derive(Debug, Serialize)]
pub struct Warmup {
    pub binding_id: String,
    pub excluded: bool,
    pub completed: bool,
}

#[derive(Debug, Serialize)]
pub struct VariantMeasurement {
    pub query_id: String,
    pub interface: &'static str,
    pub ordered: bool,
    /// The kept result columns when the variant names them; see `Variant::columns`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// `measured` when every binding produced a sample, otherwise `failed`.
    pub status: &'static str,
    pub warmup: Warmup,
    pub samples: Vec<Sample>,
    /// `None` when no binding was measured.
    pub summary: Option<Summary>,
}

/// A call with every argument decoded, so decoding stays outside the clock.
enum Prepared {
    Cypher(String, HashMap<String, IrLiteral>),
    Rank(String, RankOptions),
    Cluster(String, ClusterOptions),
    /// Source, optional target and options, boxed to keep the variants small.
    Paths(Box<(NodeSelector, Option<NodeSelector>, PathsOptions)>),
}

/// The clock. Its interval is exactly one public API call that returns a fully
/// materialized Arrow result: `execute_with_params` returns collected record
/// batches and the analyst verbs return one record batch. Project open,
/// argument decoding, the warm-up pass and result digesting are outside it.
fn timed<T>(call: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let value = call();
    (value, start.elapsed())
}

impl Prepared {
    fn call(self, forge: &GraphForge) -> Result<(SchemaRef, Vec<RecordBatch>), GfError> {
        match self {
            Self::Cypher(text, params) => forge
                .execute_with_params(&text, &params)
                .map(|result| (result.schema, result.batches)),
            Self::Rank(label, options) => forge
                .rank(&label, options)
                .map(|batch| (batch.schema(), vec![batch])),
            Self::Cluster(label, options) => forge
                .cluster(&label, options)
                .map(|batch| (batch.schema(), vec![batch])),
            Self::Paths(call) => {
                let (source, target, options) = *call;
                forge
                    .paths(&source, target.as_ref(), options)
                    .map(|batch| (batch.schema(), vec![batch]))
            }
        }
    }
}

fn algorithm<T: std::str::FromStr<Err = GfError>>(
    variant: &str,
    by: &str,
) -> Result<T, QueryError> {
    by.parse().map_err(|error: GfError| {
        QueryError::new(
            QueryCause::InvalidWorkload,
            format!("variant {variant}: {error}"),
        )
    })
}

/// The `paths` source a binding selects; `parse_workload` checks every binding with it.
pub(super) fn source_selector(
    variant: &str,
    source: &SourceSelector,
    params: &BTreeMap<String, IrLiteral>,
) -> Result<NodeSelector, QueryError> {
    let literal = &params[source.param()];
    match source {
        SourceSelector::Uuid(_) => match literal {
            IrLiteral::Str(value) => NodeSelector::uuid(value).map_err(|error| {
                QueryError::new(
                    QueryCause::InvalidWorkload,
                    format!("variant {variant}: {error}"),
                )
            }),
            other => Err(QueryError::new(
                QueryCause::InvalidWorkload,
                format!("variant {variant}: a source UUID must be a string, not {other:?}"),
            )),
        },
        SourceSelector::Match(source) => Ok(NodeSelector::Match {
            label: source.label.clone(),
            property: source.property.clone(),
            value: prop_value(variant, literal)?,
        }),
    }
}

fn prop_value(variant: &str, literal: &IrLiteral) -> Result<PropValue, QueryError> {
    match literal {
        IrLiteral::Int(value) => Ok(PropValue::Int(*value)),
        IrLiteral::Str(value) => Ok(PropValue::Str(value.clone())),
        IrLiteral::Float(value) => Ok(PropValue::Float(*value)),
        IrLiteral::Bool(value) => Ok(PropValue::Bool(*value)),
        other => Err(QueryError::new(
            QueryCause::InvalidWorkload,
            format!("variant {variant}: a source selector value cannot be {other:?}"),
        )),
    }
}

fn prepare(variant: &Variant, binding: &Binding) -> Result<Prepared, QueryError> {
    let id = variant.id.as_str();
    Ok(match &variant.operation {
        Operation::Cypher { text } => Prepared::Cypher(
            text.clone(),
            binding
                .params
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        ),
        Operation::Rank {
            label,
            by,
            directed,
            via,
            pagerank,
            clustering_normalization,
        } => Prepared::Rank(
            label.clone(),
            RankOptions {
                by: algorithm::<RankAlgorithm>(id, by)?,
                via: via.clone(),
                directed: *directed,
                write_property: None,
                pagerank: pagerank.as_ref().map(|options| options.options()),
                clustering_normalization: clustering_normalization.map(|mode| mode.normalization()),
            },
        ),
        Operation::Cluster {
            label,
            by,
            directed,
            via,
            synchronous_label_propagation,
        } => Prepared::Cluster(
            label.clone(),
            ClusterOptions {
                by: algorithm::<ClusterAlgorithm>(id, by)?,
                vector_property: None,
                via: via.clone(),
                directed: *directed,
                write_property: None,
                synchronous_label_propagation: synchronous_label_propagation
                    .as_ref()
                    .map(|options| options.options()),
            },
        ),
        Operation::Paths {
            by,
            directed,
            source,
            target,
            via,
            weight,
        } => Prepared::Paths(Box::new((
            source_selector(id, source, &binding.params)?,
            target
                .as_ref()
                .map(|target| source_selector(id, target, &binding.params))
                .transpose()?,
            PathsOptions {
                by: algorithm::<PathAlgorithm>(id, by)?,
                directed: *directed,
                via: via.clone(),
                weight: weight.clone(),
                ..Default::default()
            },
        ))),
    })
}

type CallResult = Result<(SchemaRef, Vec<RecordBatch>), GfError>;

fn execute(
    forge: &GraphForge,
    variant: &Variant,
    binding: &Binding,
) -> Result<(CallResult, Duration), QueryError> {
    let prepared = prepare(variant, binding)?;
    Ok(timed(|| prepared.call(forge)))
}

fn bounded(text: &str) -> String {
    if text.len() <= MAX_ERROR_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_ERROR_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

fn sample(
    forge: &GraphForge,
    variant: &Variant,
    binding: &Binding,
    results: Option<&ResultsDir>,
) -> Result<Outcome, QueryError> {
    let (result, elapsed) = execute(forge, variant, binding)?;
    let (schema, batches) = match result {
        Ok(result) => result,
        Err(error) => {
            return Ok(Outcome::Failed(Failure {
                cause: "query_failed",
                error_code: Some(error.code()),
                error: bounded(&error.to_string()),
            }));
        }
    };
    let projected = match &variant.columns {
        None => Ok((schema, batches)),
        Some(columns) => project(&schema, &batches, columns),
    };
    // The result is rendered a row at a time (#1914): once to digest it and, when the
    // reference check wants the cells, once more to write them. A Graphalytics result
    // has a row per vertex, and holding every rendered row (plus the copy the JSON
    // writer used to make) cost over 300 bytes a row.
    let digested = projected.and_then(|(schema, batches)| {
        let result_sha256 = streamed_digest(&schema, &batches, variant.ordered)?;
        Ok((schema, batches, result_sha256))
    });
    let (schema, batches, result_sha256) = match digested {
        Ok(digested) => digested,
        Err(error) => {
            return Ok(Outcome::Failed(Failure {
                cause: "result_unrenderable",
                error_code: None,
                error: bounded(error.message()),
            }));
        }
    };
    if let Some(results) = results {
        results.write(variant, binding, &schema, &batches, &result_sha256)?;
    }
    Ok(Outcome::Measured(Measured {
        latency_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
        result_sha256,
    }))
}

/// Keep only the named columns, in the named order, after the clock stops.
fn project(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    columns: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
    let indices = columns
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                QueryError::new(
                    QueryCause::QueryFailed,
                    format!("the result has no column {name:?} to keep"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let failed = |error: arrow::error::ArrowError| {
        QueryError::new(QueryCause::QueryFailed, error.to_string())
    };
    let projected = std::sync::Arc::new(schema.project(&indices).map_err(failed)?);
    let batches = batches
        .iter()
        .map(|batch| batch.project(&indices).map_err(failed))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((projected, batches))
}

/// Nearest-rank percentile: the smallest sample with at least `percent`% of
/// samples at or below it, i.e. `sorted[ceil(percent * n / 100) - 1]`.
///
/// # Panics
/// If `sorted` is empty or `percent` is not in `1..=100`.
#[must_use]
pub fn nearest_rank(sorted: &[u64], percent: u64) -> u64 {
    assert!(!sorted.is_empty() && (1..=100).contains(&percent));
    let n = sorted.len() as u64;
    let rank = (percent * n).div_ceil(100);
    sorted[usize::try_from(rank - 1).expect("rank fits usize")]
}

/// Run one excluded warm-up with the first binding, then one measured pass
/// over every binding in declared order. A binding whose call fails becomes a
/// failed sample, without latency, and the pass continues.
///
/// # Errors
/// `invalid_workload` only if an operation's arguments cannot be decoded,
/// which `parse_workload` already rules out.
pub fn measure_variant(
    forge: &GraphForge,
    variant: &Variant,
    results: Option<&ResultsDir>,
) -> Result<VariantMeasurement, QueryError> {
    let first = &variant.bindings[0];
    // Only whether the warm-up completed is kept: its result is dropped here, not held
    // through the measured pass beside each sample's own.
    let warmup_completed = execute(forge, variant, first)?.0.is_ok();
    let mut samples = Vec::with_capacity(variant.bindings.len());
    for binding in &variant.bindings {
        samples.push(Sample {
            binding_id: binding.id.clone(),
            outcome: sample(forge, variant, binding, results)?,
        });
    }
    let mut sorted: Vec<u64> = samples
        .iter()
        .filter_map(Sample::measured)
        .map(|measured| measured.latency_ns)
        .collect();
    sorted.sort_unstable();
    let summary = (!sorted.is_empty()).then(|| Summary {
        count: sorted.len() as u64,
        p50_ns: nearest_rank(&sorted, 50),
        p95_ns: nearest_rank(&sorted, 95),
    });
    let failed = samples.iter().any(|sample| sample.failure().is_some());
    Ok(VariantMeasurement {
        query_id: variant.id.clone(),
        interface: variant.operation.interface(),
        ordered: variant.ordered,
        columns: variant.columns.clone(),
        status: if failed { "failed" } else { "measured" },
        warmup: Warmup {
            binding_id: first.id.clone(),
            excluded: true,
            completed: warmup_completed,
        },
        samples,
        summary,
    })
}

fn cell(row: &mut Vec<u8>, text: &str) {
    row.extend_from_slice(format!("V{}:", text.len()).as_bytes());
    row.extend_from_slice(text.as_bytes());
}

/// SHA-256 over the ordering mode, the column names and Arrow types, then
/// every row. Each cell is `N` for null or `V<byte length>:<Arrow display
/// text>`, so a null differs from an empty string and no cell boundary is
/// ambiguous. An `ordered` result is digested in result order. An unordered
/// one has its encoded rows sorted bytewise first, so row order cannot change
/// its digest. Schema metadata and batch boundaries are not part of the digest.
///
/// The digest is computed a row at a time: an ordered result holds one rendered
/// row, an unordered one also keeps its encoded rows (see [`streamed_digest`]).
/// [`Rendered::digest`] is the same definition over rows held in memory.
///
/// # Errors
/// `query_failed` if a column cannot be rendered.
pub fn result_digest(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    ordered: bool,
) -> Result<String, QueryError> {
    streamed_digest(schema, batches, ordered)
}

/// Visit every row of `batches` in result order with its cells rendered as Arrow
/// display text. `texts[column]` is a cell's text unless `nulls[column]`; both
/// slices are reused from row to row, so a visit sees one row's cells and no more.
fn visit_rows(
    batches: &[RecordBatch],
    mut visit: impl FnMut(&[String], &[bool]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let options = FormatOptions::default();
    let mut texts: Vec<String> = Vec::new();
    let mut nulls: Vec<bool> = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| QueryError::new(QueryCause::QueryFailed, error.to_string()))?;
        texts.resize_with(formatters.len(), String::new);
        nulls.resize(formatters.len(), false);
        for index in 0..batch.num_rows() {
            for (position, (column, formatter)) in
                batch.columns().iter().zip(&formatters).enumerate()
            {
                nulls[position] = column.is_null(index);
                if !nulls[position] {
                    texts[position].clear();
                    write!(texts[position], "{}", formatter.value(index)).map_err(|error| {
                        QueryError::new(QueryCause::QueryFailed, error.to_string())
                    })?;
                }
            }
            visit(&texts, &nulls)?;
        }
    }
    Ok(())
}

/// [`Rendered::digest`] without the rendered rows: each row is rendered, encoded
/// and dropped. An ordered result hashes as it streams. An unordered one keeps its
/// encoded rows in one buffer with an offset per row and sorts the offsets, which
/// is the digest's bytewise row sort at a fraction of the memory of a `Vec<Vec<u8>>`.
fn streamed_digest(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    ordered: bool,
) -> Result<String, QueryError> {
    let mut header = Vec::new();
    cell(&mut header, RESULT_DIGEST);
    cell(&mut header, if ordered { "ordered" } else { "unordered" });
    for field in schema.fields() {
        cell(&mut header, field.name());
        cell(&mut header, &field.data_type().to_string());
    }
    header.push(b'\n');
    let mut hasher = Sha256::new();
    hasher.update(&header);
    let mut encoded: Vec<u8> = Vec::new();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut row: Vec<u8> = Vec::new();
    visit_rows(batches, |texts, nulls| {
        row.clear();
        for (text, null) in texts.iter().zip(nulls) {
            if *null {
                row.push(b'N');
            } else {
                cell(&mut row, text);
            }
        }
        row.push(b'\n');
        if ordered {
            hasher.update(&row);
        } else {
            spans.push((encoded.len(), row.len()));
            encoded.extend_from_slice(&row);
        }
        Ok(())
    })?;
    if !ordered {
        spans.sort_unstable_by(|left, right| {
            encoded[left.0..left.0 + left.1].cmp(&encoded[right.0..right.0 + right.1])
        });
        for (start, length) in spans {
            hasher.update(&encoded[start..start + length]);
        }
    }
    Ok(hex(&hasher.finalize()))
}

/// One result as the digest sees it: each column's name and Arrow type, and
/// every cell's Arrow display text, `None` for null. Batch boundaries and
/// schema metadata are not kept.
pub struct Rendered {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl Rendered {
    /// Render every cell with Arrow's default display options.
    ///
    /// # Errors
    /// `query_failed` if a column cannot be rendered.
    pub fn new(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Self, QueryError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().to_string()))
            .collect();
        let options = FormatOptions::default();
        let mut rows = Vec::new();
        for batch in batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| QueryError::new(QueryCause::QueryFailed, error.to_string()))?;
            for index in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .zip(&formatters)
                        .map(|(column, formatter)| {
                            (!column.is_null(index)).then(|| formatter.value(index).to_string())
                        })
                        .collect(),
                );
            }
        }
        Ok(Self { columns, rows })
    }

    /// The `graphforge-gdc-result-digest/1` SHA-256; see [`result_digest`].
    #[must_use]
    pub fn digest(&self, ordered: bool) -> String {
        let mut header = Vec::new();
        cell(&mut header, RESULT_DIGEST);
        cell(&mut header, if ordered { "ordered" } else { "unordered" });
        for (name, data_type) in &self.columns {
            cell(&mut header, name);
            cell(&mut header, data_type);
        }
        header.push(b'\n');
        let mut rows: Vec<Vec<u8>> = self
            .rows
            .iter()
            .map(|cells| {
                let mut row = Vec::new();
                for value in cells {
                    match value {
                        None => row.push(b'N'),
                        Some(text) => cell(&mut row, text),
                    }
                }
                row.push(b'\n');
                row
            })
            .collect();
        if !ordered {
            rows.sort_unstable();
        }
        let mut hasher = Sha256::new();
        hasher.update(&header);
        for row in &rows {
            hasher.update(row);
        }
        hex(&hasher.finalize())
    }
}

/// Where the driver writes each measured result for a later reference check,
/// as one `graphforge-gdc-query-result/1` JSON file per sample: the query and
/// binding ids, the ordering flag, the digest, the columns and every rendered
/// cell. Writing happens after the clock stops, so it never enters a latency.
#[derive(Debug)]
pub struct ResultsDir {
    path: PathBuf,
    next: Cell<u64>,
}

impl ResultsDir {
    /// Use an existing empty directory, so the results of two runs never mix.
    ///
    /// # Errors
    /// `io_error` when the directory is missing, unreadable or not empty.
    pub fn new(path: &Path) -> Result<Self, QueryError> {
        let io = |error: std::io::Error| {
            QueryError::new(QueryCause::Io, format!("{}: {error}", path.display()))
        };
        if std::fs::read_dir(path).map_err(io)?.next().is_some() {
            return Err(QueryError::new(
                QueryCause::Io,
                format!("results directory {} is not empty", path.display()),
            ));
        }
        Ok(Self {
            path: path.to_owned(),
            next: Cell::new(0),
        })
    }

    /// Write one result as compact JSON with its keys in sorted order, the layout
    /// `serde_json` gives a `json!` object, and the rows after every other member
    /// the check needs to start (`schema` alone follows them). The rows are
    /// rendered and written one at a time; no document is built.
    fn write(
        &self,
        variant: &Variant,
        binding: &Binding,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        result_sha256: &str,
    ) -> Result<(), QueryError> {
        let ordinal = self.next.get();
        self.next.set(ordinal + 1);
        let path = self.path.join(format!("{ordinal:08}.json"));
        let io = |error: std::io::Error| {
            QueryError::new(QueryCause::Io, format!("{}: {error}", path.display()))
        };
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io)?;
        let mut writer = BufWriter::new(file);
        let columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| json!({"name": field.name(), "type": field.data_type().to_string()}))
            .collect();
        (|| -> std::io::Result<()> {
            writer.write_all(b"{\"binding_id\":")?;
            put_json(&mut writer, &binding.id)?;
            writer.write_all(b",\"columns\":")?;
            put_json(&mut writer, &columns)?;
            writer.write_all(b",\"ordered\":")?;
            put_json(&mut writer, &variant.ordered)?;
            writer.write_all(b",\"query_id\":")?;
            put_json(&mut writer, &variant.id)?;
            writer.write_all(b",\"result_sha256\":")?;
            put_json(&mut writer, result_sha256)?;
            writer.write_all(b",\"rows\":[")
        })()
        .map_err(io)?;
        let mut first = true;
        visit_rows(batches, |texts, nulls| {
            (|| -> std::io::Result<()> {
                if !first {
                    writer.write_all(b",")?;
                }
                first = false;
                writer.write_all(b"[")?;
                for (position, (text, null)) in texts.iter().zip(nulls).enumerate() {
                    if position > 0 {
                        writer.write_all(b",")?;
                    }
                    if *null {
                        writer.write_all(b"null")?;
                    } else {
                        put_json(&mut writer, text.as_str())?;
                    }
                }
                writer.write_all(b"]")
            })()
            .map_err(io)
        })?;
        (|| -> std::io::Result<()> {
            writer.write_all(b"],\"schema\":")?;
            put_json(&mut writer, RESULT_SCHEMA)?;
            writer.write_all(b"}\n")?;
            writer.flush()
        })()
        .map_err(io)
    }
}

fn put_json<T: Serialize + ?Sized>(writer: &mut impl Write, value: &T) -> std::io::Result<()> {
    serde_json::to_writer(writer, value).map_err(std::io::Error::from)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::{Value, json};

    use super::*;

    fn variant(ordered: bool) -> Variant {
        serde_json::from_value(json!({
            "id": "q",
            "ordered": ordered,
            "operation": {"kind": "cypher", "text": "RETURN 1"},
            "bindings": [{"id": "b"}],
        }))
        .unwrap()
    }

    fn batches() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = |ids: Vec<i64>, names: Vec<Option<&str>>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(names)) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let batches = vec![
            batch(vec![1, 2], vec![Some("a\"b"), None]),
            batch(vec![], vec![]),
            batch(vec![3], vec![Some("")]),
        ];
        (schema, batches)
    }

    /// The document the writer built before it streamed: a `json!` object, rows rendered whole.
    fn built_document(
        variant: &Variant,
        binding: &Binding,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        result_sha256: &str,
    ) -> Vec<u8> {
        let rendered = Rendered::new(schema, batches).unwrap();
        let document = json!({
            "schema": RESULT_SCHEMA,
            "query_id": variant.id,
            "binding_id": binding.id,
            "ordered": variant.ordered,
            "result_sha256": result_sha256,
            "columns": rendered
                .columns
                .iter()
                .map(|(name, data_type)| json!({"name": name, "type": data_type}))
                .collect::<Vec<_>>(),
            "rows": rendered.rows,
        });
        let mut bytes = serde_json::to_vec(&document).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn a_streamed_result_file_is_byte_for_byte_the_document_built_whole() {
        let (schema, batches) = batches();
        for ordered in [true, false] {
            let variant = variant(ordered);
            let binding = &variant.bindings[0];
            let sha = streamed_digest(&schema, &batches, ordered).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let results = ResultsDir::new(directory.path()).unwrap();
            results
                .write(&variant, binding, &schema, &batches, &sha)
                .unwrap();
            let written = std::fs::read(directory.path().join("00000000.json")).unwrap();
            assert_eq!(
                String::from_utf8(written.clone()).unwrap(),
                String::from_utf8(built_document(&variant, binding, &schema, &batches, &sha))
                    .unwrap()
            );
            let parsed: Value = serde_json::from_slice(&written).unwrap();
            assert_eq!(
                parsed["rows"],
                json!([["1", "a\"b"], ["2", null], ["3", ""]]),
                "a null cell is JSON null, an empty string stays empty"
            );
        }
    }

    #[test]
    fn a_result_with_no_rows_is_written_with_an_empty_array() {
        let (schema, _) = batches();
        let variant = variant(false);
        let directory = tempfile::tempdir().unwrap();
        let results = ResultsDir::new(directory.path()).unwrap();
        results
            .write(&variant, &variant.bindings[0], &schema, &[], "d")
            .unwrap();
        let written: Value =
            serde_json::from_slice(&std::fs::read(directory.path().join("00000000.json")).unwrap())
                .unwrap();
        assert_eq!(written["rows"], json!([]));
        assert_eq!(written["schema"], RESULT_SCHEMA);
    }

    #[test]
    fn a_null_and_an_empty_string_digest_differently_when_streamed() {
        let (schema, batches) = batches();
        let with_null = streamed_digest(&schema, &batches, true).unwrap();
        let rendered = Rendered::new(&schema, &batches).unwrap();
        assert_eq!(with_null, rendered.digest(true));
        let mut emptied = Rendered::new(&schema, &batches).unwrap();
        emptied.rows[1][1] = Some(String::new());
        assert_ne!(with_null, emptied.digest(true));
    }
}
