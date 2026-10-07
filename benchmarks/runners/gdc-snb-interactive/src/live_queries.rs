//! Execute the runnable SNB Interactive reads through the public GraphForge API.
//!
//! [`execute_query`] runs one [`QueryDefinition`] with typed parameters against
//! any open `GraphForge` and returns its rows as JSON values. The trusted query
//! lane ([`run_live_queries`]) loads the committed synthetic fixture into an
//! in-memory project, runs every definition for every committed parameter
//! binding, and compares the rows with expected results that
//! `graphforge_bench.gdc_snb_interactive_reference` derived from the fixture
//! data without GraphForge.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, IrLiteral, NodeSelector, PathAlgorithm, PathsOptions, PropValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::queries::{ParameterType, QueryDefinition, QueryInterface, query_definitions};
use crate::{
    EVIDENCE_SCHEMA, EvidenceLane, JOB_SCHEMA, Operation, OperationJob, OperationOutcome,
    OperationStatus, PhaseEvidence, PhaseStatus, SUITE_ID, SuiteError, SuiteEvidence,
    ValidationMode, phase_names, run_job, sha256,
};

pub const QUERY_DATASET_ID: &str = "snb-interactive-query-synthetic-v1";
pub const QUERY_FIXTURE_SCHEMA: &str = "graphforge-gdc-snb-interactive-query-fixture/1";
pub const QUERY_EXPECTED_SCHEMA: &str = "graphforge-gdc-snb-interactive-query-expected/1";

const QUERY_GRAPH: &str = include_str!("../../../fixtures/gdc/snb-interactive-queries/graph.json");
const QUERY_EXPECTED: &str =
    include_str!("../../../fixtures/gdc/snb-interactive-queries/expected.json");

/// Parameter bindings for one execution, keyed by parameter name.
pub type QueryParameters = BTreeMap<String, Value>;

/// Rows returned by one query execution.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// Check `parameters` against the definition and convert them to typed literals.
///
/// # Errors
/// [`SuiteError::InvalidDocument`] for a missing, extra or mistyped parameter.
pub fn bind_parameters(
    definition: &QueryDefinition,
    parameters: &QueryParameters,
) -> Result<HashMap<String, IrLiteral>, SuiteError> {
    let declared: BTreeSet<&str> = definition.parameter_names().into_iter().collect();
    let supplied: BTreeSet<&str> = parameters.keys().map(String::as_str).collect();
    if declared != supplied {
        return Err(SuiteError::InvalidDocument(format!(
            "{} parameters must be exactly {declared:?}, got {supplied:?}",
            definition.operation
        )));
    }
    let mut bound = HashMap::new();
    for parameter in definition.parameters {
        let value = &parameters[parameter.name];
        let literal = match parameter.data_type {
            ParameterType::Int64 => value.as_i64().map(IrLiteral::Int),
            ParameterType::Utf8 => value.as_str().map(|text| IrLiteral::Str(text.into())),
        }
        .ok_or_else(|| {
            SuiteError::InvalidDocument(format!(
                "{} parameter {} must be {}, got {value}",
                definition.operation,
                parameter.name,
                parameter.data_type.name()
            ))
        })?;
        bound.insert(parameter.name.to_string(), literal);
    }
    Ok(bound)
}

/// Execute one runnable read through the public GraphForge API.
///
/// # Errors
/// Parameter errors, GraphForge errors, a result schema that differs from the
/// definition's columns, or an Arrow value without a JSON mapping.
pub fn execute_query(
    forge: &GraphForge,
    definition: &QueryDefinition,
    parameters: &QueryParameters,
) -> Result<QueryResult, SuiteError> {
    let bound = bind_parameters(definition, parameters)?;
    let result = match definition.interface {
        QueryInterface::Cypher(text) => {
            let execution = forge.execute_with_params(text, &bound).map_err(|error| {
                SuiteError::LiveExecution(format!(
                    "{} execution failed: {error}",
                    definition.operation
                ))
            })?;
            arrow_rows(&execution.batches, &execution.schema)?
        }
        QueryInterface::BfsPathLength {
            label,
            id_property,
            relationship_type,
            source_parameter,
            target_parameter,
        } => {
            let selector = |name: &str| NodeSelector::Match {
                label: label.into(),
                property: id_property.into(),
                value: PropValue::Int(parameters[name].as_i64().expect("bound as int64")),
            };
            let batch = forge
                .paths(
                    Some(&selector(source_parameter)),
                    Some(&selector(target_parameter)),
                    PathsOptions {
                        by: PathAlgorithm::Bfs,
                        via: Some(relationship_type.into()),
                        directed: false,
                        ..Default::default()
                    },
                )
                .map_err(|error| {
                    SuiteError::LiveExecution(format!(
                        "{} paths failed: {error}",
                        definition.operation
                    ))
                })?;
            bfs_path_length(&batch)?
        }
    };
    if result.columns != definition.columns {
        return Err(SuiteError::LiveExecution(format!(
            "{} returned columns {:?}, expected {:?}",
            definition.operation, result.columns, definition.columns
        )));
    }
    Ok(result)
}

fn bfs_path_length(batch: &RecordBatch) -> Result<QueryResult, SuiteError> {
    let length = match batch.num_rows() {
        0 => -1,
        1 => {
            let cost = batch
                .column_by_name("cost")
                .and_then(|column| column.as_primitive_opt::<Float64Type>())
                .ok_or_else(|| SuiteError::LiveExecution("bfs output lacks float64 cost".into()))?;
            let value = cost.value(0);
            if cost.is_null(0)
                || value.fract() != 0.0
                || !(0.0..=f64::from(i32::MAX)).contains(&value)
            {
                return Err(SuiteError::LiveExecution(format!(
                    "bfs returned a non-integral hop count {value}"
                )));
            }
            // Checked above: an integral value within 0..=i32::MAX.
            #[allow(clippy::cast_possible_truncation)]
            let hops = value as i64;
            hops
        }
        rows => {
            return Err(SuiteError::LiveExecution(format!(
                "bfs between two selected nodes returned {rows} rows"
            )));
        }
    };
    Ok(QueryResult {
        columns: vec!["shortestPathLength".into()],
        rows: vec![vec![Value::from(length)]],
    })
}

fn arrow_rows(
    batches: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<QueryResult, SuiteError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| arrow_value(column.as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(QueryResult { columns, rows })
}

/// Convert one Arrow cell to JSON, preserving integer, float, string, boolean,
/// list and struct (map) distinctions. Unknown types fail closed.
fn arrow_value(array: &dyn Array, row: usize) -> Result<Value, SuiteError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    let value = match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::Int8 => Value::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => Value::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => Value::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Value::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => Value::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => Value::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => float(f64::from(array.as_primitive::<Float32Type>().value(row)))?,
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row))?,
        DataType::Utf8 => Value::from(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Value::from(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Value::from(array.as_string_view().value(row)),
        DataType::List(_) => list(array.as_list::<i32>().value(row).as_ref())?,
        DataType::LargeList(_) => list(array.as_list::<i64>().value(row).as_ref())?,
        DataType::Struct(fields) => {
            let structure = array.as_struct();
            let mut object = serde_json::Map::new();
            for (index, field) in fields.iter().enumerate() {
                if field.name().starts_with("__het_") {
                    return Err(SuiteError::LiveExecution(format!(
                        "heterogeneous-list encoding {} has no exact JSON mapping",
                        field.name()
                    )));
                }
                object.insert(
                    field.name().clone(),
                    arrow_value(structure.column(index).as_ref(), row)?,
                );
            }
            Value::Object(object)
        }
        other => {
            return Err(SuiteError::LiveExecution(format!(
                "Arrow type {other} has no JSON mapping in the SNB Interactive runner"
            )));
        }
    };
    Ok(value)
}

fn float(value: f64) -> Result<Value, SuiteError> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| SuiteError::LiveExecution(format!("non-finite float {value}")))
}

fn list(values: &dyn Array) -> Result<Value, SuiteError> {
    (0..values.len())
        .map(|index| arrow_value(values, index))
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

/// Sort the definition's set-valued list columns so both sides compare as sets
/// of elements while row order stays exact.
pub fn normalize_rows(definition: &QueryDefinition, rows: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let unordered: Vec<usize> = definition
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| definition.unordered_list_columns.contains(column))
        .map(|(index, _)| index)
        .collect();
    rows.iter()
        .map(|row| {
            let mut row = row.clone();
            for &index in &unordered {
                if let Some(Value::Array(items)) = row.get_mut(index) {
                    items.sort_by_cached_key(Value::to_string);
                }
            }
            row
        })
        .collect()
}

/// Compare GraphForge rows with expected rows: exact row order, exact values,
/// set semantics only inside the definition's unordered list columns.
///
/// # Errors
/// [`SuiteError::ReferenceMismatch`] naming the first differing row.
pub fn validate_rows(
    definition: &QueryDefinition,
    expected: &[Vec<Value>],
    actual: &[Vec<Value>],
) -> Result<(), SuiteError> {
    let expected = normalize_rows(definition, expected);
    let actual = normalize_rows(definition, actual);
    if expected.len() != actual.len() {
        return Err(SuiteError::ReferenceMismatch(format!(
            "{} returned {} rows, expected {}",
            definition.operation,
            actual.len(),
            expected.len()
        )));
    }
    for (index, (want, got)) in expected.iter().zip(&actual).enumerate() {
        if want != got {
            return Err(SuiteError::ReferenceMismatch(format!(
                "{} row {index}: expected {} got {}",
                definition.operation,
                Value::Array(want.clone()),
                Value::Array(got.clone())
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryFixture {
    schema: String,
    dataset_id: String,
    classification: String,
    nodes: Vec<QueryFixtureNode>,
    edges: Vec<QueryFixtureEdge>,
}

/// `[key, label, properties]`.
#[derive(Debug, Deserialize)]
struct QueryFixtureNode(String, String, BTreeMap<String, Value>);

/// `[source key, relationship type, destination key, properties]`.
#[derive(Debug, Deserialize)]
struct QueryFixtureEdge(String, String, String, BTreeMap<String, Value>);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDocument {
    schema: String,
    dataset_id: String,
    derivation: String,
    operations: BTreeMap<String, Vec<ExpectedCase>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExpectedCase {
    pub(crate) parameters: QueryParameters,
    pub(crate) rows: Vec<Vec<Value>>,
}

fn literal(value: &Value) -> Result<IrLiteral, SuiteError> {
    match value {
        Value::String(text) => Ok(IrLiteral::Str(text.clone())),
        Value::Bool(flag) => Ok(IrLiteral::Bool(*flag)),
        Value::Number(number) => number.as_i64().map(IrLiteral::Int).ok_or_else(|| {
            SuiteError::InvalidDocument("fixture numbers must be signed 64-bit integers".into())
        }),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::String(text) => Ok(IrLiteral::Str(text.clone())),
                _ => Err(SuiteError::InvalidDocument(
                    "fixture list properties must hold strings".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(IrLiteral::List),
        _ => Err(SuiteError::InvalidDocument(
            "fixture properties must be strings, integers, booleans or string lists".into(),
        )),
    }
}

fn is_identifier(text: &str) -> bool {
    text.chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && text
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn identifiers<'a>(names: impl IntoIterator<Item = &'a String>) -> Result<(), SuiteError> {
    for name in names {
        if !is_identifier(name) {
            return Err(SuiteError::InvalidDocument(format!(
                "fixture name {name:?} is not a plain identifier"
            )));
        }
    }
    Ok(())
}

fn live(error: impl std::fmt::Display) -> SuiteError {
    SuiteError::LiveExecution(error.to_string())
}

fn execute_rows(forge: &GraphForge, query: &str, rows: Vec<IrLiteral>) -> Result<(), SuiteError> {
    forge
        .execute_with_params(
            query,
            &HashMap::from([("rows".into(), IrLiteral::List(rows))]),
        )
        .map(|_| ())
        .map_err(|error| SuiteError::LiveExecution(format!("fixture load `{query}`: {error}")))
}

fn count_query(forge: &GraphForge, query: &str) -> Result<i64, SuiteError> {
    let result = forge.execute(query).map_err(live)?;
    let rows = arrow_rows(&result.batches, &result.schema)?.rows;
    match rows.as_slice() {
        [row] => row
            .first()
            .and_then(Value::as_i64)
            .ok_or_else(|| SuiteError::LiveExecution(format!("`{query}` returned no count"))),
        _ => Err(SuiteError::LiveExecution(format!(
            "`{query}` returned {} rows",
            rows.len()
        ))),
    }
}

/// Load the fixture through public Cypher.
///
/// Nodes are created with one `UNWIND $rows ... CREATE` per label and
/// property-key set. Every node has one label, as an import session stores,
/// and its key is `<label>:<id>`. Edges are created per (source label, type,
/// destination label, property-key set) by matching both endpoints on their
/// unique `id`. Per-label node counts and per-type edge counts are checked
/// afterwards.
fn load_query_fixture(forge: &GraphForge, fixture: QueryFixture) -> Result<(), SuiteError> {
    type NodeGroup = (String, Vec<String>);
    type EdgeGroup = (String, String, String, Vec<String>);
    let mut node_groups: BTreeMap<NodeGroup, Vec<IrLiteral>> = BTreeMap::new();
    let mut label_counts: BTreeMap<String, i64> = BTreeMap::new();
    let mut endpoints: HashMap<String, (String, i64)> = HashMap::new();
    for QueryFixtureNode(key, label, values) in fixture.nodes {
        identifiers(std::iter::once(&label).chain(values.keys()))?;
        let id = values.get("id").and_then(Value::as_i64).ok_or_else(|| {
            SuiteError::InvalidDocument(format!("node {key} lacks an integer id"))
        })?;
        if key != format!("{label}:{id}") {
            return Err(SuiteError::InvalidDocument(format!(
                "node key {key} is not {label}:{id}"
            )));
        }
        if endpoints.insert(key.clone(), (label.clone(), id)).is_some() {
            return Err(SuiteError::InvalidDocument(format!(
                "duplicate node key {key}"
            )));
        }
        *label_counts.entry(label.clone()).or_default() += 1;
        let row = values
            .iter()
            .map(|(name, value)| Ok((name.clone(), literal(value)?)))
            .collect::<Result<Vec<_>, SuiteError>>()?;
        node_groups
            .entry((label, values.keys().cloned().collect()))
            .or_default()
            .push(IrLiteral::Map(row));
    }
    for ((label, names), rows) in node_groups {
        let assignments: Vec<String> = names
            .iter()
            .map(|name| format!("{name}: row.{name}"))
            .collect();
        let query = format!(
            "UNWIND $rows AS row CREATE (n:{label} {{{}}})",
            assignments.join(", ")
        );
        execute_rows(forge, &query, rows)?;
    }
    let mut edge_groups: BTreeMap<EdgeGroup, Vec<IrLiteral>> = BTreeMap::new();
    let mut type_counts: BTreeMap<String, i64> = BTreeMap::new();
    for QueryFixtureEdge(source, rel_type, destination, values) in fixture.edges {
        identifiers(std::iter::once(&rel_type).chain(values.keys()))?;
        let endpoint = |key: &String| {
            endpoints.get(key).cloned().ok_or_else(|| {
                SuiteError::InvalidDocument(format!("edge endpoint {key} is missing"))
            })
        };
        let (source_label, source_id) = endpoint(&source)?;
        let (destination_label, destination_id) = endpoint(&destination)?;
        let mut row = vec![
            ("source".to_string(), IrLiteral::Int(source_id)),
            ("destination".to_string(), IrLiteral::Int(destination_id)),
        ];
        for (name, value) in &values {
            row.push((format!("property_{name}"), literal(value)?));
        }
        *type_counts.entry(rel_type.clone()).or_default() += 1;
        edge_groups
            .entry((
                source_label,
                rel_type,
                destination_label,
                values.keys().cloned().collect(),
            ))
            .or_default()
            .push(IrLiteral::Map(row));
    }
    for ((source_label, rel_type, destination_label, names), rows) in edge_groups {
        let properties = if names.is_empty() {
            String::new()
        } else {
            let assignments: Vec<String> = names
                .iter()
                .map(|name| format!("{name}: row.property_{name}"))
                .collect();
            format!(" {{{}}}", assignments.join(", "))
        };
        // Matching the endpoints in two clauses keeps the row bound to both.
        let query = format!(
            "UNWIND $rows AS row \
             MATCH (a:{source_label} {{id: row.source}}) WITH a, row \
             MATCH (b:{destination_label} {{id: row.destination}}) \
             CREATE (a)-[:{rel_type}{properties}]->(b)"
        );
        execute_rows(forge, &query, rows)?;
    }
    for (label, expected) in label_counts {
        let loaded = count_query(forge, &format!("MATCH (n:{label}) RETURN count(n) AS c"))?;
        if loaded != expected {
            return Err(SuiteError::LiveExecution(format!(
                "label {label}: loaded {loaded} nodes, fixture declares {expected}"
            )));
        }
    }
    for (rel_type, expected) in type_counts {
        let loaded = count_query(
            forge,
            &format!("MATCH ()-[r:{rel_type}]->() RETURN count(r) AS c"),
        )?;
        if loaded != expected {
            return Err(SuiteError::LiveExecution(format!(
                "type {rel_type}: loaded {loaded} edges, fixture declares {expected}"
            )));
        }
    }
    Ok(())
}

fn parse_fixture(graph: &str) -> Result<QueryFixture, SuiteError> {
    let fixture: QueryFixture = serde_json::from_str(graph)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid query fixture: {error}")))?;
    if fixture.schema != QUERY_FIXTURE_SCHEMA
        || fixture.dataset_id != QUERY_DATASET_ID
        || fixture.classification != "synthetic_engineering_fixture"
    {
        return Err(SuiteError::InvalidDocument(
            "query fixture identity is not the synthetic SNB Interactive query fixture".into(),
        ));
    }
    Ok(fixture)
}

pub(crate) fn parse_expected(
    expected: &str,
) -> Result<BTreeMap<Operation, Vec<ExpectedCase>>, SuiteError> {
    let document: ExpectedDocument = serde_json::from_str(expected).map_err(|error| {
        SuiteError::InvalidDocument(format!("invalid expected results: {error}"))
    })?;
    if document.schema != QUERY_EXPECTED_SCHEMA
        || document.dataset_id != QUERY_DATASET_ID
        || document.derivation.is_empty()
    {
        return Err(SuiteError::InvalidDocument(
            "expected results do not belong to the query fixture".into(),
        ));
    }
    let mut cases = BTreeMap::new();
    for (code, operation_cases) in document.operations {
        let operation: Operation = code.parse()?;
        if operation_cases.is_empty() {
            return Err(SuiteError::InvalidDocument(format!(
                "{code} has no expected case"
            )));
        }
        cases.insert(operation, operation_cases);
    }
    let runnable: BTreeSet<Operation> = query_definitions()
        .iter()
        .map(|definition| definition.operation)
        .collect();
    if cases.keys().copied().collect::<BTreeSet<_>>() != runnable {
        return Err(SuiteError::InvalidDocument(
            "expected results must cover exactly the runnable reads".into(),
        ));
    }
    Ok(cases)
}

/// Open an in-memory project holding the committed query fixture.
///
/// # Errors
/// An invalid fixture document or a construction failure.
pub fn load_committed_query_fixture() -> Result<GraphForge, SuiteError> {
    let forge = GraphForge::new(None).map_err(live)?;
    load_query_fixture(&forge, parse_fixture(QUERY_GRAPH)?)?;
    Ok(forge)
}

/// Execute every case of one definition and validate it against `cases`.
pub(crate) fn check_definition(
    forge: &GraphForge,
    definition: &QueryDefinition,
    cases: &[ExpectedCase],
) -> Result<(), SuiteError> {
    for (index, case) in cases.iter().enumerate() {
        let result = execute_query(forge, definition, &case.parameters)?;
        validate_rows(definition, &case.rows, &result.rows).map_err(|error| match error {
            SuiteError::ReferenceMismatch(message) => {
                SuiteError::ReferenceMismatch(format!("case {index}: {message}"))
            }
            other => other,
        })?;
    }
    Ok(())
}

/// Context recorded by the trusted query lane.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct QueryLaneContext {
    pub fixture_sha256: String,
    pub expected_sha256: String,
    pub public_api: String,
    pub mode: String,
    pub runnable_operations: usize,
    pub cases: usize,
}

/// Run every runnable read against the committed fixture and validate it.
///
/// Refused operations (IC14, IU1–IU8) are reported with their typed cause.
///
/// # Errors
/// Only for an invalid committed fixture or expected document, or a load
/// failure; per-operation failures are reported as failed outcomes.
pub fn run_live_queries() -> Result<SuiteEvidence, SuiteError> {
    let cases = parse_expected(QUERY_EXPECTED)?;
    let forge = load_committed_query_fixture()?;
    let mut warmup_failures = 0;
    for definition in query_definitions() {
        for case in &cases[&definition.operation] {
            if execute_query(&forge, definition, &case.parameters).is_err() {
                warmup_failures += 1;
            }
        }
    }
    let mut outcomes = Vec::new();
    for operation in Operation::ALL {
        let job = OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: QUERY_DATASET_ID.into(),
            operation,
            parameters: None,
        };
        let mut outcome = run_job(&job, None, None);
        if let Some(definition) = crate::queries::query_definition(operation) {
            match check_definition(&forge, definition, &cases[&operation]) {
                Ok(()) => {
                    outcome.status = OperationStatus::Passed;
                    outcome.cause = None;
                }
                Err(error) => {
                    outcome.status = OperationStatus::Failed;
                    outcome.cause = Some(error.to_string());
                }
            }
            outcome.validation_mode = ValidationMode::Exact.name().into();
        }
        outcomes.push(outcome);
    }
    let failed = outcomes
        .iter()
        .any(|outcome| outcome.status == OperationStatus::Failed);
    let phase = |phase: &str, status: PhaseStatus, detail: String| PhaseEvidence {
        phase: phase.into(),
        status,
        detail,
    };
    Ok(SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: QUERY_DATASET_ID.into(),
        lane: EvidenceLane::LiveQueryFixture,
        status: if failed {
            OperationStatus::Failed
        } else {
            OperationStatus::Passed
        },
        certification: false,
        phases: phase_names(),
        phase_evidence: vec![
            phase(
                "load",
                PhaseStatus::Passed,
                "synthetic SNB fixture loaded with public Cypher UNWIND/CREATE, one label per \
                 node; per-label node and per-type edge counts checked"
                    .into(),
            ),
            phase(
                "warmup",
                if warmup_failures == 0 {
                    PhaseStatus::Passed
                } else {
                    PhaseStatus::Failed
                },
                format!("every case executed once; {warmup_failures} failed"),
            ),
            phase(
                "execution",
                if failed {
                    PhaseStatus::Failed
                } else {
                    PhaseStatus::Passed
                },
                "every runnable read executed through GraphForge.execute_with_params or \
                 GraphForge.paths(by=bfs)"
                    .into(),
            ),
            phase(
                "validation",
                if failed {
                    PhaseStatus::Failed
                } else {
                    PhaseStatus::Passed
                },
                "Arrow rows converted to typed JSON and compared row-for-row with expected rows \
                 derived independently from the fixture"
                    .into(),
            ),
        ],
        live_context: None,
        query_context: Some(QueryLaneContext {
            fixture_sha256: sha256(QUERY_GRAPH),
            expected_sha256: sha256(QUERY_EXPECTED),
            public_api: "graphforge_api::GraphForge".into(),
            mode: "in_memory".into(),
            runnable_operations: query_definitions().len(),
            cases: cases.values().map(Vec::len).sum(),
        }),
        identities: serde_json::json!({
            "fixture": {"classification": "synthetic_engineering_fixture"},
            "reference": {
                "derivation": "graphforge_bench.gdc_snb_interactive_reference",
                "semantics": "ldbc_snb_interactive_v1_impls cypher/queries at f9c394a92cd55e535893f6c9907b141d6533c817"
            },
            "runner": {
                "name": "graphforge-benchmark-gdc-snb-interactive",
                "release": "workspace",
                "commit": null
            }
        }),
        operations: outcomes,
    })
}

/// Outcome lookup used by tests and the CLI.
pub fn outcome_for(evidence: &SuiteEvidence, operation: Operation) -> Option<&OperationOutcome> {
    evidence
        .operations
        .iter()
        .find(|outcome| outcome.operation == operation)
}

#[cfg(test)]
mod tests;
