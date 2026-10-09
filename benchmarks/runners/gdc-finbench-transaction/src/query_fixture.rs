//! Live execution of every runnable read over the committed query fixture.
//!
//! The fixture (`fixtures/gdc/finbench-transaction-queries`) holds a
//! FinBench-shaped graph, parameter bindings per read, and `expected.json`,
//! which `graphforge_bench.gdc_finbench_transaction_reference` derives from the
//! graph without GraphForge. This module loads the graph through the public
//! API, runs each read's Cypher with each binding, and compares the rows.

use crate::queries::{ColumnKind, QueryDefinition, query_definition};
use crate::{
    Operation, OperationStatus, ResultRows, SuiteError, ValidationMode, api_error, validate_result,
};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array};
use arrow::datatypes::{DataType, Int64Type};
use graphforge_api::{GraphForge, IrLiteral};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

pub const QUERY_FIXTURE_SCHEMA: &str = "graphforge-gdc-finbench-query-fixture/1";
pub const QUERY_PARAMETERS_SCHEMA: &str = "graphforge-gdc-finbench-query-parameters/1";
pub const QUERY_EXPECTED_SCHEMA: &str = "graphforge-gdc-finbench-query-expected/1";
pub const QUERY_EVIDENCE_SCHEMA: &str = "graphforge-gdc-finbench-query-evidence/1";
pub const QUERY_DATASET_ID: &str = "finbench-engineering-queries-v1";
pub const QUERY_REFERENCE_DERIVATION: &str = "graphforge_bench.gdc_finbench_transaction_reference";

pub type Binding = serde_json::Map<String, serde_json::Value>;
pub type Rows = Vec<Vec<String>>;
/// Loaded entity counts by node label or edge type.
pub type Counts = BTreeMap<String, u64>;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureGraph {
    pub schema: String,
    pub dataset_id: String,
    pub note: String,
    pub nodes: BTreeMap<String, Table>,
    pub edges: BTreeMap<String, Table>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureParameters {
    pub schema: String,
    pub dataset_id: String,
    pub bindings: BTreeMap<Operation, Vec<Binding>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedResult {
    pub binding: Binding,
    pub rows: Rows,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpected {
    pub schema: String,
    pub dataset_id: String,
    pub derivation: String,
    pub results: BTreeMap<Operation, Vec<ExpectedResult>>,
}

/// The validated fixture: graph, bindings and independently derived rows.
#[derive(Clone, Debug)]
pub struct QueryFixture {
    pub graph: FixtureGraph,
    pub parameters: FixtureParameters,
    pub expected: FixtureExpected,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BindingOutcome {
    pub operation: Operation,
    pub binding_index: usize,
    pub binding: Binding,
    pub status: OperationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    pub rows: Option<Rows>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct QueryEvidence {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub status: OperationStatus,
    pub certification: bool,
    pub execution_mode: String,
    pub interface: String,
    pub reference_derivation: String,
    pub loaded_nodes: Counts,
    pub loaded_edges: Counts,
    pub outcomes: Vec<BindingOutcome>,
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, SuiteError> {
    let text = fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })?;
    serde_json::from_str(&text).map_err(|error| {
        SuiteError::InvalidDocument(format!("invalid {}: {error}", path.display()))
    })
}

impl QueryFixture {
    /// Read and cross-check the three fixture documents.
    pub fn load(dir: &Path) -> Result<Self, SuiteError> {
        let graph: FixtureGraph = read_json(&dir.join("graph.json"))?;
        let parameters: FixtureParameters = read_json(&dir.join("parameters.json"))?;
        let expected: FixtureExpected = read_json(&dir.join("expected.json"))?;
        let schemas = [
            (graph.schema.as_str(), QUERY_FIXTURE_SCHEMA),
            (parameters.schema.as_str(), QUERY_PARAMETERS_SCHEMA),
            (expected.schema.as_str(), QUERY_EXPECTED_SCHEMA),
        ];
        for (found, wanted) in schemas {
            if found != wanted {
                return Err(SuiteError::InvalidDocument(format!(
                    "query fixture schema {found}, expected {wanted}"
                )));
            }
        }
        for dataset in [
            &graph.dataset_id,
            &parameters.dataset_id,
            &expected.dataset_id,
        ] {
            if dataset != QUERY_DATASET_ID {
                return Err(SuiteError::InvalidDocument(format!(
                    "query fixture dataset {dataset}, expected {QUERY_DATASET_ID}"
                )));
            }
        }
        if expected.derivation != QUERY_REFERENCE_DERIVATION {
            return Err(SuiteError::InvalidDocument(format!(
                "expected rows must come from {QUERY_REFERENCE_DERIVATION}, not {}",
                expected.derivation
            )));
        }
        let expected_bindings: BTreeMap<Operation, Vec<&Binding>> = expected
            .results
            .iter()
            .map(|(operation, results)| {
                (
                    *operation,
                    results.iter().map(|result| &result.binding).collect(),
                )
            })
            .collect();
        let parameter_bindings: BTreeMap<Operation, Vec<&Binding>> = parameters
            .bindings
            .iter()
            .map(|(operation, bindings)| (*operation, bindings.iter().collect()))
            .collect();
        if expected_bindings != parameter_bindings {
            return Err(SuiteError::InvalidDocument(
                "expected.json is stale: its bindings differ from parameters.json".into(),
            ));
        }
        Ok(Self {
            graph,
            parameters,
            expected,
        })
    }

    /// Expected rows for one binding.
    pub fn expected_rows(&self, operation: Operation, index: usize) -> Option<&Rows> {
        self.expected
            .results
            .get(&operation)
            .and_then(|results| results.get(index))
            .map(|result| &result.rows)
    }
}

fn identifier(name: &str) -> Result<&str, SuiteError> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && chars.all(|rest| rest.is_ascii_alphanumeric());
    if valid {
        Ok(name)
    } else {
        Err(SuiteError::InvalidDocument(format!(
            "fixture name {name:?} is not a plain identifier"
        )))
    }
}

fn literal(value: &serde_json::Value) -> Result<IrLiteral, SuiteError> {
    match value {
        serde_json::Value::Bool(flag) => Ok(IrLiteral::Bool(*flag)),
        serde_json::Value::String(text) => Ok(IrLiteral::Str(text.clone())),
        serde_json::Value::Number(number) => number
            .as_i64()
            .map(IrLiteral::Int)
            .or_else(|| number.as_f64().map(IrLiteral::Float))
            .ok_or_else(|| SuiteError::InvalidDocument(format!("unsupported number {number}"))),
        other => Err(SuiteError::InvalidDocument(format!(
            "unsupported fixture value {other}"
        ))),
    }
}

fn row_params(
    columns: &[String],
    row: &[serde_json::Value],
) -> Result<HashMap<String, IrLiteral>, SuiteError> {
    if columns.len() != row.len() {
        return Err(SuiteError::InvalidDocument(format!(
            "fixture row {row:?} does not match columns {columns:?}"
        )));
    }
    columns
        .iter()
        .zip(row)
        .map(|(column, value)| Ok((column.clone(), literal(value)?)))
        .collect()
}

fn load_error(error: graphforge_api::GfError) -> SuiteError {
    api_error("load", error)
}

fn node_variable(id: i64) -> String {
    if id < 0 {
        format!("m{}", id.unsigned_abs())
    } else {
        format!("n{id}")
    }
}

/// Bind one row's properties as uniquely named parameters; returns the
/// ` {name: $param, ...}` map text (empty when there are no properties).
fn bind_properties(
    prefix: &str,
    columns: &[String],
    row: &[serde_json::Value],
    skip: &[&str],
    params: &mut HashMap<String, IrLiteral>,
) -> Result<String, SuiteError> {
    let values = row_params(columns, row)?;
    let mut entries = Vec::new();
    for column in columns {
        if skip.contains(&column.as_str()) {
            continue;
        }
        let name = identifier(column)?;
        let parameter = format!("{prefix}_{name}");
        params.insert(parameter.clone(), values[column].clone());
        entries.push(format!("{name}: ${parameter}"));
    }
    Ok(if entries.is_empty() {
        String::new()
    } else {
        format!(" {{{}}}", entries.join(", "))
    })
}

fn integer_column(
    columns: &[String],
    row: &[serde_json::Value],
    name: &str,
) -> Result<i64, SuiteError> {
    match row_params(columns, row)?.get(name) {
        Some(IrLiteral::Int(value)) => Ok(*value),
        _ => Err(SuiteError::InvalidDocument(format!(
            "fixture row {row:?} has no integer {name}"
        ))),
    }
}

/// Load the fixture graph through one public Cypher `CREATE` statement.
///
/// One statement keeps the load a single write: each statement publishes a
/// project generation, and per-row statements made loading quadratic.
pub fn load_graph(forge: &GraphForge, graph: &FixtureGraph) -> Result<(), SuiteError> {
    let mut labels: HashMap<i64, String> = HashMap::new();
    let mut patterns = Vec::new();
    let mut params = HashMap::new();
    for (label, table) in &graph.nodes {
        let label = identifier(label)?;
        for row in &table.rows {
            let id = integer_column(&table.columns, row, "id")?;
            if labels.insert(id, label.to_string()).is_some() {
                return Err(SuiteError::InvalidDocument(format!(
                    "node id {id} is not unique across labels"
                )));
            }
            let variable = node_variable(id);
            let properties = bind_properties(&variable, &table.columns, row, &[], &mut params)?;
            patterns.push(format!("({variable}:{label}{properties})"));
        }
    }
    for (edge_type, table) in &graph.edges {
        let edge_type = identifier(edge_type)?;
        for (index, row) in table.rows.iter().enumerate() {
            let endpoint = |name: &str| -> Result<String, SuiteError> {
                let id = integer_column(&table.columns, row, name)?;
                if labels.contains_key(&id) {
                    Ok(node_variable(id))
                } else {
                    Err(SuiteError::InvalidDocument(format!(
                        "{edge_type} {name} {id} is not a fixture node"
                    )))
                }
            };
            let (from, to) = (endpoint("from")?, endpoint("to")?);
            let properties = bind_properties(
                &format!("e_{edge_type}_{index}"),
                &table.columns,
                row,
                &["from", "to"],
                &mut params,
            )?;
            patterns.push(format!("({from})-[:{edge_type}{properties}]->({to})"));
        }
    }
    if patterns.is_empty() {
        return Ok(());
    }
    forge
        .execute_with_params(&format!("CREATE {}", patterns.join(", ")), &params)
        .map_err(load_error)?;
    Ok(())
}

fn single_count(forge: &GraphForge, statement: &str) -> Result<u64, SuiteError> {
    let result = forge.execute(statement).map_err(load_error)?;
    let mut counts = Vec::new();
    for batch in &result.batches {
        let column = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| {
                SuiteError::InvalidDocument(format!("count is not Int64: {statement}"))
            })?;
        counts.extend(column.iter().flatten());
    }
    match counts.as_slice() {
        [count] => {
            u64::try_from(*count).map_err(|_| SuiteError::InvalidDocument("negative count".into()))
        }
        _ => Err(SuiteError::InvalidDocument(format!(
            "count returned {counts:?}: {statement}"
        ))),
    }
}

/// Read per-label and per-type counts back and require they equal the fixture.
pub fn reconcile_counts(
    forge: &GraphForge,
    graph: &FixtureGraph,
) -> Result<(Counts, Counts), SuiteError> {
    let mut nodes = BTreeMap::new();
    for (label, table) in &graph.nodes {
        let loaded = single_count(
            forge,
            &format!("MATCH (n:{}) RETURN count(n) AS c", identifier(label)?),
        )?;
        if loaded != table.rows.len() as u64 {
            return Err(SuiteError::InvalidDocument(format!(
                "count_mismatch: {label} loaded {loaded}, fixture has {}",
                table.rows.len()
            )));
        }
        nodes.insert(label.clone(), loaded);
    }
    let mut edges = BTreeMap::new();
    for (edge_type, table) in &graph.edges {
        let loaded = single_count(
            forge,
            &format!(
                "MATCH ()-[e:{}]->() RETURN count(e) AS c",
                identifier(edge_type)?
            ),
        )?;
        if loaded != table.rows.len() as u64 {
            return Err(SuiteError::InvalidDocument(format!(
                "count_mismatch: {edge_type} loaded {loaded}, fixture has {}",
                table.rows.len()
            )));
        }
        edges.insert(edge_type.clone(), loaded);
    }
    Ok((nodes, edges))
}

fn non_null(column: &ArrayRef, row: usize, name: &str) -> Result<(), SuiteError> {
    if column.is_null(row) {
        Err(SuiteError::InvalidDocument(format!(
            "column {name} is null at row {row}"
        )))
    } else {
        Ok(())
    }
}

fn format_cell(
    column: &ArrayRef,
    row: usize,
    name: &str,
    kind: ColumnKind,
) -> Result<String, SuiteError> {
    non_null(column, row, name)?;
    let wrong_type = || {
        SuiteError::InvalidDocument(format!(
            "column {name} is {}, declared {kind:?}",
            column.data_type()
        ))
    };
    match kind {
        ColumnKind::Int => match column.data_type() {
            DataType::Int64 => Ok(column.as_primitive::<Int64Type>().value(row).to_string()),
            DataType::UInt64 => Ok(column
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row)
                .to_string()),
            _ => Err(wrong_type()),
        },
        ColumnKind::Float3 => {
            let value = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(wrong_type)?
                .value(row);
            let text = format!("{value:.3}");
            let printed: f64 = text.parse().map_err(|_| wrong_type())?;
            if (printed - value).abs() > 1e-9 {
                return Err(SuiteError::InvalidDocument(format!(
                    "column {name} value {value} is not rounded to 3 decimals"
                )));
            }
            Ok(text)
        }
        ColumnKind::Bool => Ok(column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(wrong_type)?
            .value(row)
            .to_string()),
        ColumnKind::Text => match column.data_type() {
            DataType::Utf8 => Ok(column.as_string::<i32>().value(row).to_string()),
            DataType::LargeUtf8 => Ok(column.as_string::<i64>().value(row).to_string()),
            DataType::Utf8View => Ok(column.as_string_view().value(row).to_string()),
            _ => Err(wrong_type()),
        },
        ColumnKind::IntList => {
            let list = column
                .as_list_opt::<i32>()
                .ok_or_else(wrong_type)?
                .value(row);
            let values = list
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(wrong_type)?;
            if values.null_count() > 0 {
                return Err(SuiteError::InvalidDocument(format!(
                    "column {name} list has a null at row {row}"
                )));
            }
            let items: Vec<String> = values.values().iter().map(i64::to_string).collect();
            Ok(format!("[{}]", items.join(", ")))
        }
    }
}

/// Run one definition's Cypher and print its rows in the declared text form.
pub fn execute_definition(
    forge: &GraphForge,
    definition: &QueryDefinition,
    cypher: &str,
    params: &HashMap<String, IrLiteral>,
) -> Result<Rows, SuiteError> {
    let result = forge
        .execute_with_params(cypher, params)
        .map_err(|error| api_error("execution", error))?;
    let names: Vec<&str> = result
        .schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let declared: Vec<&str> = definition
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    if names != declared {
        return Err(SuiteError::InvalidDocument(format!(
            "{} returned columns {names:?}, declared {declared:?}",
            definition.operation
        )));
    }
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            let cells = definition
                .columns
                .iter()
                .zip(batch.columns())
                .map(|(column, array)| format_cell(array, row, column.name, column.kind))
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(cells);
        }
    }
    Ok(rows)
}

fn as_result_rows(rows: &Rows) -> ResultRows {
    rows.iter()
        .map(|row| serde_json::to_string(row).expect("string rows serialize"))
        .collect()
}

/// Compare produced rows with the expected rows under the read's ordering.
pub fn compare_rows(
    mode: ValidationMode,
    expected: &Rows,
    produced: &Rows,
) -> Result<(), SuiteError> {
    validate_result(mode, &as_result_rows(expected), &as_result_rows(produced))
}

fn outcome_for(
    forge: &GraphForge,
    definition: &QueryDefinition,
    cypher: &str,
    binding: &Binding,
    expected: &Rows,
) -> (OperationStatus, Option<String>, Option<Rows>) {
    let params = match definition.bind(binding) {
        Ok(params) => params,
        Err(error @ SuiteError::SemanticIncompatibility { .. }) => {
            return (
                OperationStatus::SemanticIncompatibility,
                Some(error.to_string()),
                None,
            );
        }
        Err(error) => return (OperationStatus::HarnessError, Some(error.to_string()), None),
    };
    match execute_definition(forge, definition, cypher, &params) {
        Ok(rows) => match compare_rows(definition.validation_mode(), expected, &rows) {
            Ok(()) => (OperationStatus::Passed, None, Some(rows)),
            Err(error) => (
                OperationStatus::CorrectnessFailed,
                Some(error.to_string()),
                Some(rows),
            ),
        },
        Err(SuiteError::LiveExecution {
            resource: true,
            detail,
            ..
        }) => (OperationStatus::ResourceExceeded, Some(detail), None),
        Err(error) => (OperationStatus::HarnessError, Some(error.to_string()), None),
    }
}

/// Load the fixture into a fresh in-memory project, ready for queries.
pub fn loaded_forge(fixture: &QueryFixture) -> Result<(GraphForge, Counts, Counts), SuiteError> {
    let forge = GraphForge::new(None).map_err(load_error)?;
    load_graph(&forge, &fixture.graph)?;
    let (nodes, edges) = reconcile_counts(&forge, &fixture.graph)?;
    Ok((forge, nodes, edges))
}

/// Run the bindings of every read for which `cypher_for` returns query text.
pub fn run_fixture_with(
    fixture: &QueryFixture,
    cypher_for: impl Fn(&QueryDefinition) -> Option<String>,
) -> Result<QueryEvidence, SuiteError> {
    let (forge, loaded_nodes, loaded_edges) = loaded_forge(fixture)?;
    let mut outcomes = Vec::new();
    for (operation, bindings) in &fixture.parameters.bindings {
        let definition = query_definition(*operation).ok_or_else(|| {
            SuiteError::InvalidDocument(format!("{operation} has bindings but is not runnable"))
        })?;
        let Some(cypher) = cypher_for(definition) else {
            continue;
        };
        for (index, binding) in bindings.iter().enumerate() {
            let expected = fixture.expected_rows(*operation, index).ok_or_else(|| {
                SuiteError::InvalidDocument(format!(
                    "{operation} binding {index} has no expected rows"
                ))
            })?;
            let (status, cause, rows) = outcome_for(&forge, definition, &cypher, binding, expected);
            outcomes.push(BindingOutcome {
                operation: *operation,
                binding_index: index,
                binding: binding.clone(),
                status,
                cause,
                rows,
            });
        }
    }
    let has = |status: OperationStatus| outcomes.iter().any(|outcome| outcome.status == status);
    let status = if has(OperationStatus::HarnessError) {
        OperationStatus::HarnessError
    } else if has(OperationStatus::CorrectnessFailed) {
        OperationStatus::CorrectnessFailed
    } else if has(OperationStatus::ResourceExceeded) {
        OperationStatus::ResourceExceeded
    } else if has(OperationStatus::SemanticIncompatibility) {
        OperationStatus::SemanticIncompatibility
    } else {
        OperationStatus::Passed
    };
    Ok(QueryEvidence {
        schema: QUERY_EVIDENCE_SCHEMA.into(),
        suite_id: crate::SUITE_ID.into(),
        dataset_id: fixture.graph.dataset_id.clone(),
        status,
        certification: false,
        execution_mode: "live_graphforge".into(),
        interface: "graphforge_api::GraphForge::execute_with_params".into(),
        reference_derivation: fixture.expected.derivation.clone(),
        loaded_nodes,
        loaded_edges,
        outcomes,
    })
}

/// Run every read's pinned Cypher over the fixture in `dir`.
pub fn run_query_fixture(dir: &Path) -> Result<QueryEvidence, SuiteError> {
    let fixture = QueryFixture::load(dir)?;
    run_fixture_with(&fixture, |definition| Some(definition.cypher.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::QUERIES;

    fn fixture_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/gdc/finbench-transaction-queries")
    }

    fn committed() -> QueryFixture {
        QueryFixture::load(&fixture_dir()).unwrap()
    }

    /// Run one read with `from` replaced by `to` in its Cypher; the statuses of
    /// its bindings, in order.
    fn statuses_after_mutation(operation: Operation, from: &str, to: &str) -> Vec<OperationStatus> {
        let definition = query_definition(operation).unwrap();
        assert!(
            definition.cypher.contains(from),
            "{operation} mutation target {from:?} is not in its Cypher"
        );
        let evidence = run_fixture_with(&committed(), |query| {
            (query.operation == operation).then(|| query.cypher.replacen(from, to, 1))
        })
        .unwrap();
        evidence
            .outcomes
            .into_iter()
            .map(|outcome| outcome.status)
            .collect()
    }

    #[test]
    fn every_read_runs_live_and_matches_the_independent_rows() {
        let evidence = run_query_fixture(&fixture_dir()).unwrap();
        let failures: Vec<_> = evidence
            .outcomes
            .iter()
            .filter(|outcome| outcome.status != OperationStatus::Passed)
            .map(|outcome| {
                (
                    outcome.operation,
                    outcome.binding_index,
                    outcome.cause.clone(),
                )
            })
            .collect();
        assert!(failures.is_empty(), "{failures:#?}");
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(!evidence.certification);
        assert_eq!(evidence.reference_derivation, QUERY_REFERENCE_DERIVATION);
        for query in QUERIES {
            assert!(
                evidence
                    .outcomes
                    .iter()
                    .any(|outcome| outcome.operation == query.operation),
                "{} has no binding in the fixture",
                query.operation
            );
        }
        let graph = committed().graph;
        assert_eq!(
            evidence.loaded_edges["transfer"],
            graph.edges["transfer"].rows.len() as u64
        );
        assert_eq!(
            evidence.loaded_nodes["Account"],
            graph.nodes["Account"].rows.len() as u64
        );
        assert_eq!(evidence.loaded_edges.len(), 9);
        assert_eq!(evidence.loaded_nodes.len(), 5);
    }

    #[test]
    fn every_truncated_read_has_a_binding_where_truncation_changes_the_rows() {
        let fixture = committed();
        for query in QUERIES.iter().filter(|query| query.truncation.is_some()) {
            let results = &fixture.expected.results[&query.operation];
            let without_limit = |result: &ExpectedResult| {
                let mut binding = result.binding.clone();
                let limit = binding.remove("truncationLimit").and_then(|l| l.as_i64());
                (binding, limit)
            };
            let changed = results.iter().any(|truncated| {
                let (rest, limit) = without_limit(truncated);
                limit.is_some_and(|limit| limit < crate::queries::DEFAULT_TRUNCATION_LIMIT)
                    && results.iter().any(|full| {
                        without_limit(full)
                            == (rest.clone(), Some(crate::queries::DEFAULT_TRUNCATION_LIMIT))
                            && full.rows != truncated.rows
                    })
            });
            assert!(changed, "{} never exercises truncation", query.operation);
        }
    }

    #[test]
    fn dropping_tcr6_mid_truncation_is_detected() {
        let statuses = statuses_after_mutation(
            Operation::Tcr6,
            "collect([s.id, t.timestamp])[0..$truncationLimit]",
            "collect([s.id, t.timestamp])",
        );
        assert_eq!(statuses[0], OperationStatus::Passed);
        assert_eq!(statuses[1], OperationStatus::CorrectnessFailed);
    }

    #[test]
    fn reversing_the_tcr1_truncation_tie_break_is_detected() {
        let statuses = statuses_after_mutation(
            Operation::Tcr1,
            "ORDER BY e.timestamp DESC, w.id ASC",
            "ORDER BY e.timestamp DESC, w.id DESC",
        );
        assert_eq!(
            statuses,
            [OperationStatus::Passed, OperationStatus::CorrectnessFailed]
        );
    }

    #[test]
    fn dropping_the_tcr1_ascending_timestamp_rule_is_detected() {
        let statuses =
            statuses_after_mutation(Operation::Tcr1, " OR (i > 0 AND ts[i - 1] >= ts[i])", "");
        assert!(statuses.contains(&OperationStatus::CorrectnessFailed));
    }

    #[test]
    fn windowing_the_tsr3_denominator_is_detected() {
        let statuses = statuses_after_mutation(
            Operation::Tsr3,
            "OPTIONAL MATCH (dst)<-[edge2:transfer]-(:Account) ",
            "OPTIONAL MATCH (dst)<-[edge2:transfer]-(:Account) \
             WHERE $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
        );
        assert_eq!(statuses[0], OperationStatus::CorrectnessFailed);
    }

    #[test]
    fn stale_or_foreign_expected_rows_are_rejected_before_execution() {
        let source = fixture_dir();
        let work =
            std::env::temp_dir().join(format!("finbench-query-fixture-{}", std::process::id()));
        fs::create_dir_all(&work).unwrap();
        for name in ["graph.json", "parameters.json"] {
            fs::copy(source.join(name), work.join(name)).unwrap();
        }
        let expected: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(source.join("expected.json")).unwrap())
                .unwrap();

        let mut stale = expected.clone();
        stale["results"]["TCR1"][1]["binding"]["truncationLimit"] = serde_json::json!(4);
        fs::write(work.join("expected.json"), stale.to_string()).unwrap();
        let error = QueryFixture::load(&work).unwrap_err();
        assert!(error.to_string().contains("stale"), "{error}");

        let mut foreign = expected;
        foreign["derivation"] = serde_json::json!("graphforge_output");
        fs::write(work.join("expected.json"), foreign.to_string()).unwrap();
        let error = QueryFixture::load(&work).unwrap_err();
        assert!(
            error.to_string().contains(QUERY_REFERENCE_DERIVATION),
            "{error}"
        );
        fs::remove_dir_all(&work).unwrap();
    }
}
