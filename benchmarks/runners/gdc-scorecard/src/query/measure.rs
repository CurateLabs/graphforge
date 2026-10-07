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

/// One measured execution of one binding.
#[derive(Debug, Serialize)]
pub struct Sample {
    pub binding_id: String,
    pub latency_ns: u64,
    pub rows: u64,
    pub result_sha256: String,
}

/// Nearest-rank percentiles over a variant's measured samples.
#[derive(Debug, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
}

/// The excluded warm-up pass. It carries no latency, by construction.
#[derive(Debug, Serialize)]
pub struct Warmup {
    pub binding_id: String,
    pub excluded: bool,
}

#[derive(Debug, Serialize)]
pub struct VariantMeasurement {
    pub query_id: String,
    pub interface: &'static str,
    pub warmup: Warmup,
    pub samples: Vec<Sample>,
    pub summary: Summary,
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

fn execute(
    forge: &GraphForge,
    variant: &Variant,
    binding: &Binding,
) -> Result<((SchemaRef, Vec<RecordBatch>), Duration), QueryError> {
    let prepared = prepare(variant, binding)?;
    let (result, elapsed) = timed(|| prepared.call(forge));
    let result = result.map_err(|error| {
        QueryError::new(
            QueryCause::QueryFailed,
            format!("variant {} binding {}: {error}", variant.id, binding.id),
        )
    })?;
    Ok((result, elapsed))
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
/// over every binding in declared order.
///
/// # Errors
/// `query_failed` when a call returns an error, and `invalid_workload` when an
/// analyst operation names an unknown algorithm or an unusable source value.
pub fn measure_variant(
    forge: &GraphForge,
    variant: &Variant,
) -> Result<VariantMeasurement, QueryError> {
    let first = &variant.bindings[0];
    execute(forge, variant, first)?;
    let mut samples = Vec::with_capacity(variant.bindings.len());
    for binding in &variant.bindings {
        let ((schema, batches), elapsed) = execute(forge, variant, binding)?;
        let rows = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        samples.push(Sample {
            binding_id: binding.id.clone(),
            latency_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            rows,
            result_sha256: result_digest(&schema, &batches)?,
        });
    }
    let mut sorted: Vec<u64> = samples.iter().map(|sample| sample.latency_ns).collect();
    sorted.sort_unstable();
    Ok(VariantMeasurement {
        query_id: variant.id.clone(),
        interface: variant.operation.interface(),
        warmup: Warmup {
            binding_id: first.id.clone(),
            excluded: true,
        },
        summary: Summary {
            count: sorted.len() as u64,
            p50_ns: nearest_rank(&sorted, 50),
            p95_ns: nearest_rank(&sorted, 95),
        },
        samples,
    })
}

fn cell(hasher: &mut Sha256, text: &str) {
    hasher.update(format!("V{}:", text.len()));
    hasher.update(text);
}

/// SHA-256 over the column names and Arrow types, then every row in result
/// order. Each cell is `N` for null or `V<byte length>:<Arrow display text>`,
/// so a null differs from an empty string and no cell boundary is ambiguous.
/// Schema metadata and batch boundaries are not part of the digest.
///
/// # Errors
/// `query_failed` if a column cannot be rendered.
pub fn result_digest(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String, QueryError> {
    let mut hasher = Sha256::new();
    hasher.update(RESULT_DIGEST);
    hasher.update("\n");
    for field in schema.fields() {
        cell(&mut hasher, field.name());
        cell(&mut hasher, &field.data_type().to_string());
    }
    hasher.update("\n");
    let options = FormatOptions::default();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| QueryError::new(QueryCause::QueryFailed, error.to_string()))?;
        for row in 0..batch.num_rows() {
            for (column, formatter) in batch.columns().iter().zip(&formatters) {
                if column.is_null(row) {
                    hasher.update("N");
                } else {
                    cell(&mut hasher, &formatter.value(row).to_string());
                }
            }
            hasher.update("\n");
        }
    }
    Ok(hex(&hasher.finalize()))
}
