//! The per-operation latency clock, the measured pass, and the result digest.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
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
    let rendered = match projected.and_then(|(schema, batches)| Rendered::new(&schema, &batches)) {
        Ok(rendered) => rendered,
        Err(error) => {
            return Ok(Outcome::Failed(Failure {
                cause: "result_unrenderable",
                error_code: None,
                error: bounded(error.message()),
            }));
        }
    };
    let result_sha256 = rendered.digest(variant.ordered);
    if let Some(results) = results {
        results.write(variant, binding, &rendered, &result_sha256)?;
    }
    Ok(Outcome::Measured(Measured {
        latency_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        rows: rendered.rows.len() as u64,
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
    let (warmup, _) = execute(forge, variant, first)?;
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
            completed: warmup.is_ok(),
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
/// # Errors
/// `query_failed` if a column cannot be rendered.
pub fn result_digest(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    ordered: bool,
) -> Result<String, QueryError> {
    Ok(Rendered::new(schema, batches)?.digest(ordered))
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

    fn write(
        &self,
        variant: &Variant,
        binding: &Binding,
        rendered: &Rendered,
        result_sha256: &str,
    ) -> Result<(), QueryError> {
        let ordinal = self.next.get();
        self.next.set(ordinal + 1);
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
        serde_json::to_writer(&mut writer, &document)
            .map_err(|error| QueryError::new(QueryCause::Io, error.to_string()))?;
        writer.write_all(b"\n").map_err(io)?;
        writer.flush().map_err(io)
    }
}
