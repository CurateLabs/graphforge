//! The per-operation latency clock, the measured pass, and the result digest.

use std::collections::HashMap;
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
use sha2::{Digest, Sha256};

use super::workload::{Binding, Operation, Variant};
use super::{QueryCause, QueryError};
use crate::identity::hex;

/// The one declared per-operation latency clock. `docs/development/benchmarking.md`
/// names it, and `gdc_measurement_policy.py` rejects latency from anything else.
pub const LATENCY_CLOCK: &str = "graphforge-gdc-query-clock/1";
/// SHA-256 over a canonical rendering of the result; see [`result_digest`].
pub const RESULT_DIGEST: &str = "graphforge-gdc-result-digest/1";
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
    Paths(NodeSelector, Box<PathsOptions>),
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
            Self::Paths(source, options) => forge
                .paths(&source, None, *options)
                .map(|batch| (batch.schema(), vec![batch])),
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

/// The selector value for a `paths` source; `parse_workload` checks every binding with it.
pub(super) fn prop_value(variant: &str, literal: &IrLiteral) -> Result<PropValue, QueryError> {
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
        } => Prepared::Rank(
            label.clone(),
            RankOptions {
                by: algorithm::<RankAlgorithm>(id, by)?,
                via: via.clone(),
                directed: *directed,
                write_property: None,
            },
        ),
        Operation::Cluster {
            label,
            by,
            directed,
            via,
        } => Prepared::Cluster(
            label.clone(),
            ClusterOptions {
                by: algorithm::<ClusterAlgorithm>(id, by)?,
                vector_property: None,
                via: via.clone(),
                directed: *directed,
                write_property: None,
            },
        ),
        Operation::Paths {
            by,
            directed,
            source,
            via,
            weight,
        } => Prepared::Paths(
            NodeSelector::Match {
                label: source.label.clone(),
                property: source.property.clone(),
                value: prop_value(id, &binding.params[&source.param])?,
            },
            Box::new(PathsOptions {
                by: algorithm::<PathAlgorithm>(id, by)?,
                directed: *directed,
                via: via.clone(),
                weight: weight.clone(),
                ..Default::default()
            }),
        ),
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

fn sample(forge: &GraphForge, variant: &Variant, binding: &Binding) -> Result<Outcome, QueryError> {
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
    Ok(match result_digest(&schema, &batches, variant.ordered) {
        Ok(result_sha256) => Outcome::Measured(Measured {
            latency_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            result_sha256,
        }),
        Err(error) => Outcome::Failed(Failure {
            cause: "result_unrenderable",
            error_code: None,
            error: bounded(error.message()),
        }),
    })
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
) -> Result<VariantMeasurement, QueryError> {
    let first = &variant.bindings[0];
    let (warmup, _) = execute(forge, variant, first)?;
    let mut samples = Vec::with_capacity(variant.bindings.len());
    for binding in &variant.bindings {
        samples.push(Sample {
            binding_id: binding.id.clone(),
            outcome: sample(forge, variant, binding)?,
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
    let mut header = Vec::new();
    cell(&mut header, RESULT_DIGEST);
    cell(&mut header, if ordered { "ordered" } else { "unordered" });
    for field in schema.fields() {
        cell(&mut header, field.name());
        cell(&mut header, &field.data_type().to_string());
    }
    header.push(b'\n');
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
            let mut row = Vec::new();
            for (column, formatter) in batch.columns().iter().zip(&formatters) {
                if column.is_null(index) {
                    row.push(b'N');
                } else {
                    cell(&mut row, &formatter.value(index).to_string());
                }
            }
            row.push(b'\n');
            rows.push(row);
        }
    }
    if !ordered {
        rows.sort_unstable();
    }
    let mut hasher = Sha256::new();
    hasher.update(&header);
    for row in &rows {
        hasher.update(row);
    }
    Ok(hex(&hasher.finalize()))
}
