//! Reads node and edge counts back from a reopened project and compares them
//! with the counts the rung must reproduce.

use std::collections::BTreeMap;

use arrow::array::{Array, Int64Array, UInt64Array};
use graphforge_api::GraphForge;
use serde::Serialize;

use super::workload::ExpectedCounts;
use super::{QueryCause, QueryError};

/// One expected count and the count read back from the project.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CountPair {
    pub expected: u64,
    pub observed: u64,
}

impl CountPair {
    fn matches(self) -> bool {
        self.expected == self.observed
    }
}

/// Every count the driver read back, all of which matched.
#[derive(Debug, Serialize)]
pub struct Reconciliation {
    pub status: &'static str,
    pub source: String,
    /// The public calls the counts were read through.
    pub method: &'static str,
    pub nodes: CountPair,
    pub edges: CountPair,
    pub labels: BTreeMap<String, CountPair>,
    pub types: BTreeMap<String, CountPair>,
}

const METHOD: &str = "Cypher count probes through GraphForge::execute, plus GraphForge::labels";

fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn probe(forge: &GraphForge, cypher: &str) -> Result<u64, QueryError> {
    let failed = |message: String| QueryError::new(QueryCause::CountProbeFailed, message);
    let result = forge
        .execute(cypher)
        .map_err(|error| failed(format!("{cypher}: {error}")))?;
    let mut values = Vec::new();
    for batch in &result.batches {
        if batch.num_columns() != 1 {
            return Err(failed(format!("{cypher}: expected one column")));
        }
        let column = batch.column(0);
        for row in 0..column.len() {
            if column.is_null(row) {
                return Err(failed(format!("{cypher}: null count")));
            }
            if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                values.push(u64::try_from(array.value(row)).map_err(|_| {
                    failed(format!("{cypher}: negative count {}", array.value(row)))
                })?);
            } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                values.push(array.value(row));
            } else {
                return Err(failed(format!(
                    "{cypher}: count column has type {}",
                    column.data_type()
                )));
            }
        }
    }
    match values.as_slice() {
        [count] => Ok(*count),
        _ => Err(failed(format!(
            "{cypher}: expected one count row, read {}",
            values.len()
        ))),
    }
}

/// Read every declared count back and fail on any difference.
///
/// # Errors
/// `count_probe_failed` when a count cannot be read, and `count_mismatch`
/// naming every differing count when one does not match, including a label
/// present in the project that the expected counts do not declare.
pub fn reconcile(
    forge: &GraphForge,
    expected: &ExpectedCounts,
) -> Result<Reconciliation, QueryError> {
    let pair = |expected: u64, cypher: &str| {
        probe(forge, cypher).map(|observed| CountPair { expected, observed })
    };
    let nodes = pair(expected.nodes, "MATCH (n) RETURN count(n) AS count")?;
    let edges = pair(expected.edges, "MATCH ()-[r]->() RETURN count(r) AS count")?;
    let mut labels = BTreeMap::new();
    for (label, count) in &expected.labels {
        let cypher = format!("MATCH (n:{}) RETURN count(n) AS count", quoted(label));
        labels.insert(label.clone(), pair(*count, &cypher)?);
    }
    let mut types = BTreeMap::new();
    for (rel_type, count) in &expected.types {
        let cypher = format!(
            "MATCH ()-[r:{}]->() RETURN count(r) AS count",
            quoted(rel_type)
        );
        types.insert(rel_type.clone(), pair(*count, &cypher)?);
    }
    let present = forge.labels().map_err(|error| {
        QueryError::new(QueryCause::CountProbeFailed, format!("labels(): {error}"))
    })?;

    let mut differences = Vec::new();
    let mut note = |name: String, pair: CountPair| {
        if !pair.matches() {
            differences.push(format!(
                "{name}: expected {}, read {}",
                pair.expected, pair.observed
            ));
        }
    };
    note("nodes".into(), nodes);
    note("edges".into(), edges);
    for (label, pair) in &labels {
        note(format!("label {label}"), *pair);
    }
    for (rel_type, pair) in &types {
        note(format!("type {rel_type}"), *pair);
    }
    for label in present {
        if !expected.labels.contains_key(&label) {
            differences.push(format!("label {label}: present but not declared"));
        }
    }
    if !differences.is_empty() {
        return Err(QueryError::new(
            QueryCause::CountMismatch,
            differences.join("; "),
        ));
    }
    Ok(Reconciliation {
        status: "reconciled",
        source: expected.source.clone(),
        method: METHOD,
        nodes,
        edges,
        labels,
        types,
    })
}
