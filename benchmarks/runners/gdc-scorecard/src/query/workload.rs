//! The two documents a suite hands the driver: its query variants with their
//! parameter bindings, and the per-type counts a loaded rung must reconcile to.
//!
//! A suite registers work by writing these documents; the driver never names
//! a suite, a query or a parameter.

use std::collections::{BTreeMap, BTreeSet};

use graphforge_api::{ClusterAlgorithm, GfError, IrLiteral, PathAlgorithm, RankAlgorithm};
use serde::Deserialize;

use super::{QueryCause, QueryError};

pub const WORKLOAD_SCHEMA: &str = "graphforge-gdc-query-workload/1";
pub const EXPECTED_COUNTS_SCHEMA: &str = "graphforge-gdc-expected-counts/1";

/// A suite's mapped query variants for one rung.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    pub schema: String,
    pub suite: String,
    pub variants: Vec<Variant>,
}

/// One query variant: one operation measured over its parameter bindings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Variant {
    pub id: String,
    pub operation: Operation,
    /// Whether row order is part of the answer (the query sorts its result).
    /// Unordered results are digested order-independently; see `result_digest`.
    pub ordered: bool,
    /// The result columns that form the answer, in order. The call is timed
    /// whole; only these columns are digested and written, after the clock
    /// stops. `None` keeps every column. A Graphalytics BFS answer is each
    /// target's depth, so the `path` column `paths` also returns is left out.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    pub bindings: Vec<Binding>,
}

/// The public product call a variant makes.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// `GraphForge::execute_with_params(text, params)`.
    Cypher { text: String },
    /// `GraphForge::rank(label, RankOptions { by, directed, via })`.
    Rank {
        label: String,
        by: String,
        directed: bool,
        #[serde(default)]
        via: Option<String>,
    },
    /// `GraphForge::cluster(label, ClusterOptions { by, directed, via })`.
    Cluster {
        label: String,
        by: String,
        directed: bool,
        #[serde(default)]
        via: Option<String>,
    },
    /// `GraphForge::paths(source, None, PathsOptions { by, directed, via, weight })`,
    /// where the source node is selected by UUID or by `(:label {property: $param})`.
    Paths {
        by: String,
        directed: bool,
        source: SourceSelector,
        #[serde(default)]
        via: Option<String>,
        #[serde(default)]
        weight: Option<String>,
    },
}

/// Selects a `paths` source node from one binding parameter.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SourceSelector {
    /// `NodeSelector::Uuid`: the parameter is the node's canonical UUID string.
    Uuid(UuidSource),
    /// `NodeSelector::Match`: the unique node `(:label {property: $param})`.
    Match(MatchSource),
}

/// The source node's UUID is the string parameter `uuid_param`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UuidSource {
    pub uuid_param: String,
}

/// The unique node of `label` whose `property` equals the parameter `param`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchSource {
    pub label: String,
    pub property: String,
    pub param: String,
}

impl SourceSelector {
    /// The binding parameter that selects the source.
    #[must_use]
    pub fn param(&self) -> &str {
        match self {
            Self::Uuid(source) => &source.uuid_param,
            Self::Match(source) => &source.param,
        }
    }
}

/// One parameter binding. Values use `IrLiteral`'s tagged JSON encoding,
/// for example `{"type": "Int", "value": 3}`, so a binding is never typed by guesswork.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub id: String,
    #[serde(default)]
    pub params: BTreeMap<String, IrLiteral>,
}

impl Operation {
    /// The public interface the operation calls, recorded in the evidence.
    #[must_use]
    pub fn interface(&self) -> &'static str {
        match self {
            Self::Cypher { .. } => "graphforge_api::GraphForge::execute_with_params",
            Self::Rank { .. } => "graphforge_api::GraphForge::rank",
            Self::Cluster { .. } => "graphforge_api::GraphForge::cluster",
            Self::Paths { .. } => "graphforge_api::GraphForge::paths",
        }
    }

    /// The analyst algorithm name must be one the product accepts.
    fn check_algorithm(&self) -> Result<(), GfError> {
        match self {
            Self::Cypher { .. } => Ok(()),
            Self::Rank { by, .. } => by.parse::<RankAlgorithm>().map(drop),
            Self::Cluster { by, .. } => by.parse::<ClusterAlgorithm>().map(drop),
            Self::Paths { by, .. } => by.parse::<PathAlgorithm>().map(drop),
        }
    }

    /// Parameter names a binding must supply exactly. Cypher declares none here:
    /// the query binder rejects a missing or unused parameter itself.
    fn required_params(&self) -> Option<BTreeSet<&str>> {
        match self {
            Self::Cypher { .. } => None,
            Self::Rank { .. } | Self::Cluster { .. } => Some(BTreeSet::new()),
            Self::Paths { source, .. } => Some(BTreeSet::from([source.param()])),
        }
    }
}

/// Per-type counts a reopened project must reproduce exactly.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedCounts {
    pub schema: String,
    /// Where the counts come from, for example the LDBC page that publishes them.
    pub source: String,
    pub nodes: u64,
    pub edges: u64,
    /// Every node label in the project, with its node count.
    pub labels: BTreeMap<String, u64>,
    /// Every relationship type in the project, with its edge count.
    pub types: BTreeMap<String, u64>,
}

fn invalid(cause: QueryCause, message: String) -> QueryError {
    QueryError::new(cause, message)
}

/// Parse and check a workload document.
///
/// # Errors
/// `invalid_workload` for a malformed document, a duplicate variant or binding
/// id, a variant without bindings, or a binding whose parameters do not match
/// an analyst operation's declared parameters.
pub fn parse_workload(bytes: &[u8]) -> Result<Workload, QueryError> {
    let workload: Workload = serde_json::from_slice(bytes)
        .map_err(|error| invalid(QueryCause::InvalidWorkload, error.to_string()))?;
    let fail = |message: String| Err(invalid(QueryCause::InvalidWorkload, message));
    if workload.schema != WORKLOAD_SCHEMA {
        return fail(format!("schema must be {WORKLOAD_SCHEMA}"));
    }
    if workload.suite.is_empty() || workload.variants.is_empty() {
        return fail("a workload names its suite and at least one variant".into());
    }
    let mut variant_ids = BTreeSet::new();
    for variant in &workload.variants {
        if variant.id.is_empty() || !variant_ids.insert(variant.id.as_str()) {
            return fail(format!("variant id {:?} is empty or repeated", variant.id));
        }
        if variant.bindings.is_empty() {
            return fail(format!("variant {} has no bindings", variant.id));
        }
        if let Err(error) = variant.operation.check_algorithm() {
            return fail(format!("variant {}: {error}", variant.id));
        }
        if let Some(columns) = &variant.columns {
            let unique: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
            if columns.is_empty() || unique.len() != columns.len() || unique.contains("") {
                return fail(format!(
                    "variant {} columns must be non-empty, unique names",
                    variant.id
                ));
            }
        }
        let mut binding_ids = BTreeSet::new();
        for binding in &variant.bindings {
            if binding.id.is_empty() || !binding_ids.insert(binding.id.as_str()) {
                return fail(format!(
                    "variant {} binding id {:?} is empty or repeated",
                    variant.id, binding.id
                ));
            }
            if let Some(required) = variant.operation.required_params() {
                let supplied: BTreeSet<&str> = binding.params.keys().map(String::as_str).collect();
                if supplied != required {
                    return fail(format!(
                        "variant {} binding {} supplies {supplied:?}; the operation takes {required:?}",
                        variant.id, binding.id
                    ));
                }
            }
            if let Operation::Paths { source, .. } = &variant.operation {
                super::measure::source_selector(&variant.id, source, &binding.params)?;
            }
        }
    }
    Ok(workload)
}

/// Parse and check an expected-counts document.
///
/// # Errors
/// `invalid_expected_counts` for a malformed document, or relationship type
/// counts that do not sum to the edge total. Every edge has exactly one type,
/// so the sum identity plus the total probe also rules out an undeclared type.
pub fn parse_expected_counts(bytes: &[u8]) -> Result<ExpectedCounts, QueryError> {
    let expected: ExpectedCounts = serde_json::from_slice(bytes)
        .map_err(|error| invalid(QueryCause::InvalidExpectedCounts, error.to_string()))?;
    let fail = |message: String| Err(invalid(QueryCause::InvalidExpectedCounts, message));
    if expected.schema != EXPECTED_COUNTS_SCHEMA {
        return fail(format!("schema must be {EXPECTED_COUNTS_SCHEMA}"));
    }
    if expected.source.is_empty() {
        return fail("expected counts must cite their source".into());
    }
    let typed = expected
        .types
        .values()
        .try_fold(0_u64, |sum, count| sum.checked_add(*count));
    if typed != Some(expected.edges) {
        return fail(format!(
            "relationship type counts sum to {typed:?}, not the edge total {}",
            expected.edges
        ));
    }
    if expected
        .labels
        .keys()
        .chain(expected.types.keys())
        .any(String::is_empty)
    {
        return fail("a label or relationship type is empty".into());
    }
    Ok(expected)
}
