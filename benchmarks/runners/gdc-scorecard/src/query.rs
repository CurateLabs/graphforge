//! `query`: the per-operation driver every GDC suite plugs into.
//!
//! It opens an existing durable project, reconciles its node and edge counts
//! with the rung's expected counts, then measures each registered query
//! variant: one excluded warm-up, then one pass over its parameter bindings,
//! timed by the single declared clock in [`measure`]. Suites supply only
//! documents (see [`workload`]); the driver has no suite-specific code.

mod measure;
mod reconcile;
mod workload;

use std::fmt;
use std::path::Path;

use graphforge_api::GraphForge;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::identity::hex;

pub use measure::{
    Failure, LATENCY_CLOCK, MAX_ERROR_BYTES, Measured, Outcome, RESULT_DIGEST, Sample, Summary,
    VariantMeasurement, Warmup, measure_variant, nearest_rank, result_digest,
};
pub use reconcile::{CountPair, Reconciliation, reconcile};
pub use workload::{
    Binding, EXPECTED_COUNTS_SCHEMA, ExpectedCounts, Operation, SourceSelector, Variant,
    WORKLOAD_SCHEMA, Workload, parse_expected_counts, parse_workload,
};

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-query-evidence/1";
/// The producer every latency in the evidence comes from.
pub const DRIVER: &str = "graphforge-benchmark-gdc-scorecard query";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryCause {
    InvalidWorkload,
    InvalidExpectedCounts,
    ProjectMissing,
    ProjectOpenFailed,
    CountProbeFailed,
    CountMismatch,
    QueryFailed,
    OutputExists,
    Io,
}

impl QueryCause {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidWorkload => "invalid_workload",
            Self::InvalidExpectedCounts => "invalid_expected_counts",
            Self::ProjectMissing => "project_missing",
            Self::ProjectOpenFailed => "project_open_failed",
            Self::CountProbeFailed => "count_probe_failed",
            Self::CountMismatch => "count_mismatch",
            Self::QueryFailed => "query_failed",
            Self::OutputExists => "output_exists",
            Self::Io => "io_error",
        }
    }
}

#[derive(Debug)]
pub struct QueryError {
    cause: QueryCause,
    message: String,
}

impl QueryError {
    #[must_use]
    pub fn new(cause: QueryCause, message: impl Into<String>) -> Self {
        Self {
            cause,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn cause(&self) -> QueryCause {
        self.cause
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.cause.as_str(), self.message)
    }
}

impl std::error::Error for QueryError {}

#[derive(Debug, Serialize)]
pub struct Driver {
    pub name: &'static str,
    pub version: &'static str,
    pub executable_sha256: String,
}

#[derive(Debug, Serialize)]
pub struct Inputs {
    pub workload_sha256: String,
    pub expected_counts_sha256: String,
}

#[derive(Debug, Serialize)]
pub struct Project {
    pub path: String,
    pub opened_with: &'static str,
}

/// Declares the clock so a consumer can refuse latency from any other source.
#[derive(Debug, Serialize)]
pub struct LatencyClock {
    pub id: &'static str,
    pub producer: &'static str,
    pub source: &'static str,
    pub unit: &'static str,
    pub interval: &'static str,
    pub warmup_passes_per_variant: u64,
    pub percentile_method: &'static str,
}

const CLOCK: LatencyClock = LatencyClock {
    id: LATENCY_CLOCK,
    producer: DRIVER,
    source: "std::time::Instant",
    unit: "ns",
    interval: "one public GraphForge API call returning a fully materialized Arrow result",
    warmup_passes_per_variant: 1,
    percentile_method: "nearest_rank",
};

/// `graphforge-gdc-query-evidence/1`; see `benchmarks/schemas/gdc-query-evidence.json`.
#[derive(Debug, Serialize)]
pub struct Evidence {
    pub schema: &'static str,
    pub certification: bool,
    pub suite: String,
    /// `passed` when every binding of every variant was measured, else `failed`.
    pub status: &'static str,
    /// Every failed sample, in run order; empty when the run passed.
    pub failures: Vec<FailedSample>,
    pub driver: Driver,
    pub inputs: Inputs,
    pub project: Project,
    pub reconciliation: Reconciliation,
    pub latency_clock: LatencyClock,
    pub result_digest: &'static str,
    pub variants: Vec<VariantMeasurement>,
}

/// One failed sample, listed at the top of the evidence.
#[derive(Debug, Serialize)]
pub struct FailedSample {
    pub query_id: String,
    pub binding_id: String,
    pub cause: &'static str,
    pub error_code: Option<&'static str>,
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Open an existing durable project. The product initializes a missing or
/// empty directory as a new project, so the driver refuses both rather than
/// measure an empty graph; any other non-project directory is refused by the
/// product itself.
///
/// # Errors
/// `project_missing` when `path` is missing or an empty directory, and
/// `project_open_failed` when the product refuses to open it.
pub fn open_project(path: &Path) -> Result<GraphForge, QueryError> {
    let populated = std::fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some());
    if !populated {
        return Err(QueryError::new(
            QueryCause::ProjectMissing,
            format!("{} is missing or empty, not a project", path.display()),
        ));
    }
    let text = path
        .to_str()
        .ok_or_else(|| QueryError::new(QueryCause::ProjectMissing, "project path is not UTF-8"))?;
    GraphForge::new(Some(text)).map_err(|error| {
        QueryError::new(
            QueryCause::ProjectOpenFailed,
            format!("{}: {error}", path.display()),
        )
    })
}

/// Reconcile, then measure every variant in declared order. A failing call
/// does not stop the run: it becomes a failed sample and the run's `status`
/// becomes `failed`, so one pass surfaces every failure.
///
/// # Errors
/// A document, project-open or reconciliation [`QueryCause`]. `count_mismatch`
/// stops the run before any query is timed, since every later number would
/// describe the wrong graph.
pub fn run(
    project: &Path,
    workload: &[u8],
    expected_counts: &[u8],
    executable_sha256: String,
) -> Result<Evidence, QueryError> {
    let parsed = parse_workload(workload)?;
    let expected = parse_expected_counts(expected_counts)?;
    let forge = open_project(project)?;
    let reconciliation = reconcile(&forge, &expected)?;
    let variants = parsed
        .variants
        .iter()
        .map(|variant| measure_variant(&forge, variant))
        .collect::<Result<Vec<_>, _>>()?;
    let failures: Vec<FailedSample> = variants
        .iter()
        .flat_map(|variant| {
            variant.samples.iter().filter_map(|sample| {
                sample.failure().map(|failure| FailedSample {
                    query_id: variant.query_id.clone(),
                    binding_id: sample.binding_id.clone(),
                    cause: failure.cause,
                    error_code: failure.error_code,
                })
            })
        })
        .collect();
    Ok(Evidence {
        schema: EVIDENCE_SCHEMA,
        certification: false,
        suite: parsed.suite,
        status: if failures.is_empty() {
            "passed"
        } else {
            "failed"
        },
        failures,
        driver: Driver {
            name: DRIVER,
            version: env!("CARGO_PKG_VERSION"),
            executable_sha256,
        },
        inputs: Inputs {
            workload_sha256: sha256_hex(workload),
            expected_counts_sha256: sha256_hex(expected_counts),
        },
        project: Project {
            path: project.display().to_string(),
            opened_with: "graphforge_api::GraphForge::new(Some(path))",
        },
        reconciliation,
        latency_clock: CLOCK,
        result_digest: RESULT_DIGEST,
        variants,
    })
}
