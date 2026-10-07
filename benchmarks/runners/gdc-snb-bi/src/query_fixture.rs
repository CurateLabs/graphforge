//! Executes the runnable BI reads on the committed LDBC-shaped fixture (#1879).
//!
//! The fixture (`fixtures/gdc/snb-bi-queries`) holds pipe-separated CSV files
//! in LDBC's per-entity layout, typed parameters per query, and expected rows
//! derived independently from the CSV files by
//! `graphforge_bench.gdc_snb_bi_reference` (never captured from GraphForge).
//! This module loads the CSV files through public Cypher, runs every
//! [`BI_QUERIES`] entry through `GraphForge::execute_with_params`, and compares
//! the rows with the expected ones.

use crate::queries::{BI_QUERIES, BiQuery, ParameterKind, REFUSED_READS};
use crate::{Operation, SuiteError};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
    Time64NanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, IrLiteral};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

pub const QUERY_EVIDENCE_SCHEMA: &str = "graphforge-gdc-snb-bi-query-evidence/1";
pub const QUERY_PARAMETERS_SCHEMA: &str = "graphforge-gdc-snb-bi-query-parameters/1";
pub const QUERY_EXPECTED_SCHEMA: &str = "graphforge-gdc-snb-bi-expected/1";
/// Authority recorded in every expected-result document.
pub const EXPECTED_AUTHORITY: &str = "independent_python_derivation_from_fixture_csv";
/// Relative tolerance for floating-point result cells.
const FLOAT_TOLERANCE: f64 = 1e-9;

#[derive(Clone, Copy)]
enum Column {
    Int(&'static str),
    Str(&'static str),
    OptionalStr(&'static str),
    DateTime(&'static str),
}

impl Column {
    fn name(self) -> &'static str {
        match self {
            Self::Int(name) | Self::Str(name) | Self::OptionalStr(name) | Self::DateTime(name) => {
                name
            }
        }
    }
}

struct NodeFile {
    file: &'static str,
    labels: &'static str,
    columns: &'static [Column],
}

struct EdgeFile {
    file: &'static str,
    source: (&'static str, &'static str),
    relationship: &'static str,
    target: (&'static str, &'static str),
    properties: &'static [Column],
}

const NODE_FILES: &[NodeFile] = &[
    NodeFile {
        file: "TagClass",
        labels: "TagClass",
        columns: &[Column::Int("id"), Column::Str("name")],
    },
    NodeFile {
        file: "Tag",
        labels: "Tag",
        columns: &[Column::Int("id"), Column::Str("name")],
    },
    NodeFile {
        file: "Country",
        labels: "Country",
        columns: &[Column::Int("id"), Column::Str("name")],
    },
    NodeFile {
        file: "City",
        labels: "City",
        columns: &[Column::Int("id"), Column::Str("name")],
    },
    NodeFile {
        file: "Person",
        labels: "Person",
        columns: &[
            Column::Int("id"),
            Column::Str("firstName"),
            Column::Str("lastName"),
            Column::DateTime("creationDate"),
        ],
    },
    NodeFile {
        file: "Forum",
        labels: "Forum",
        columns: &[
            Column::Int("id"),
            Column::Str("title"),
            Column::DateTime("creationDate"),
        ],
    },
    NodeFile {
        file: "Post",
        labels: "Message:Post",
        columns: &[
            Column::Int("id"),
            Column::DateTime("creationDate"),
            Column::OptionalStr("content"),
            Column::Int("length"),
            Column::OptionalStr("language"),
        ],
    },
    NodeFile {
        file: "Comment",
        labels: "Message:Comment",
        columns: &[
            Column::Int("id"),
            Column::DateTime("creationDate"),
            Column::OptionalStr("content"),
            Column::Int("length"),
        ],
    },
];

const fn edge(
    file: &'static str,
    source: (&'static str, &'static str),
    relationship: &'static str,
    target: (&'static str, &'static str),
    properties: &'static [Column],
) -> EdgeFile {
    EdgeFile {
        file,
        source,
        relationship,
        target,
        properties,
    }
}

const CREATED: &[Column] = &[Column::DateTime("creationDate")];

const EDGE_FILES: &[EdgeFile] = &[
    edge(
        "Tag_hasType_TagClass",
        ("Tag", "TagId"),
        "HAS_TYPE",
        ("TagClass", "TagClassId"),
        &[],
    ),
    edge(
        "City_isPartOf_Country",
        ("City", "CityId"),
        "IS_PART_OF",
        ("Country", "CountryId"),
        &[],
    ),
    edge(
        "Person_isLocatedIn_City",
        ("Person", "PersonId"),
        "IS_LOCATED_IN",
        ("City", "CityId"),
        &[],
    ),
    edge(
        "Person_knows_Person",
        ("Person", "Person1Id"),
        "KNOWS",
        ("Person", "Person2Id"),
        CREATED,
    ),
    edge(
        "Person_hasInterest_Tag",
        ("Person", "PersonId"),
        "HAS_INTEREST",
        ("Tag", "TagId"),
        &[],
    ),
    edge(
        "Forum_hasModerator_Person",
        ("Forum", "ForumId"),
        "HAS_MODERATOR",
        ("Person", "PersonId"),
        &[],
    ),
    edge(
        "Forum_hasMember_Person",
        ("Forum", "ForumId"),
        "HAS_MEMBER",
        ("Person", "PersonId"),
        CREATED,
    ),
    edge(
        "Forum_containerOf_Post",
        ("Forum", "ForumId"),
        "CONTAINER_OF",
        ("Post", "PostId"),
        &[],
    ),
    edge(
        "Post_hasCreator_Person",
        ("Post", "PostId"),
        "HAS_CREATOR",
        ("Person", "PersonId"),
        &[],
    ),
    edge(
        "Comment_hasCreator_Person",
        ("Comment", "CommentId"),
        "HAS_CREATOR",
        ("Person", "PersonId"),
        &[],
    ),
    edge(
        "Post_hasTag_Tag",
        ("Post", "PostId"),
        "HAS_TAG",
        ("Tag", "TagId"),
        &[],
    ),
    edge(
        "Comment_hasTag_Tag",
        ("Comment", "CommentId"),
        "HAS_TAG",
        ("Tag", "TagId"),
        &[],
    ),
    edge(
        "Comment_replyOf_Post",
        ("Comment", "CommentId"),
        "REPLY_OF",
        ("Post", "PostId"),
        &[],
    ),
    edge(
        "Comment_replyOf_Comment",
        ("Comment", "CommentId"),
        "REPLY_OF",
        ("Comment", "ParentCommentId"),
        &[],
    ),
    edge(
        "Person_likes_Post",
        ("Person", "PersonId"),
        "LIKES",
        ("Post", "PostId"),
        CREATED,
    ),
    edge(
        "Person_likes_Comment",
        ("Person", "PersonId"),
        "LIKES",
        ("Comment", "CommentId"),
        CREATED,
    ),
];

fn invalid(message: impl Into<String>) -> SuiteError {
    SuiteError::InvalidDocument(message.into())
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Parse an LDBC UTC timestamp, `YYYY-MM-DDTHH:MM:SS.mmm+00:00`.
pub fn parse_utc_datetime(text: &str) -> Result<IrLiteral, SuiteError> {
    let bad = || invalid(format!("datetime must be YYYY-MM-DDTHH:MM:SS.mmm+00:00: {text}"));
    let bytes = text.as_bytes();
    if bytes.len() != 29 || !text.ends_with("+00:00") {
        return Err(bad());
    }
    for (index, separator) in [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'.'),
    ] {
        if bytes[index] != separator {
            return Err(bad());
        }
    }
    let number = |range: std::ops::Range<usize>| -> Result<i64, SuiteError> {
        let digits = &text[range];
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(bad());
        }
        digits.parse::<i64>().map_err(|_| bad())
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second, millis) = (
        number(11..13)?,
        number(14..16)?,
        number(17..19)?,
        number(20..23)?,
    );
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(bad());
    }
    let days = days_from_civil(year, month, day);
    if civil_from_days(days) != (year, month, day) {
        return Err(bad());
    }
    let nanos = ((hour * 60 + minute) * 60 + second) * 1_000_000_000 + millis * 1_000_000;
    Ok(IrLiteral::ZonedDateTime {
        days,
        nanos,
        offset: 0,
        zone: None,
    })
}

fn render_datetime(days: i64, nanos: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    let seconds = nanos / 1_000_000_000;
    let millis = (nanos % 1_000_000_000) / 1_000_000;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

struct CsvTable {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn read_csv(directory: &Path, name: &str) -> Result<CsvTable, SuiteError> {
    let path = directory.join(format!("{name}.csv"));
    let text = fs::read_to_string(&path)
        .map_err(|error| invalid(format!("failed to read {}: {error}", path.display())))?;
    let mut lines = text.lines();
    let header: Vec<String> = lines
        .next()
        .ok_or_else(|| invalid(format!("{name}.csv has no header")))?
        .split('|')
        .map(str::to_string)
        .collect();
    let mut rows = Vec::new();
    for line in lines {
        let row: Vec<String> = line.split('|').map(str::to_string).collect();
        if row.len() != header.len() {
            return Err(invalid(format!(
                "{name}.csv: expected {} fields in {line:?}",
                header.len()
            )));
        }
        rows.push(row);
    }
    Ok(CsvTable { header, rows })
}

fn cell(table: &CsvTable, row: &[String], name: &str, file: &str) -> Result<String, SuiteError> {
    let index = table
        .header
        .iter()
        .position(|column| column == name)
        .ok_or_else(|| invalid(format!("{file}.csv has no column {name}")))?;
    Ok(row[index].clone())
}

fn typed(column: Column, text: &str, file: &str) -> Result<IrLiteral, SuiteError> {
    match column {
        Column::Int(name) => text
            .parse::<i64>()
            .map(IrLiteral::Int)
            .map_err(|_| invalid(format!("{file}.csv {name}: not an integer: {text:?}"))),
        Column::Str(name) if text.is_empty() => {
            Err(invalid(format!("{file}.csv {name}: empty required string")))
        }
        Column::Str(_) => Ok(IrLiteral::Str(text.to_string())),
        Column::OptionalStr(_) if text.is_empty() => Ok(IrLiteral::Null),
        Column::OptionalStr(_) => Ok(IrLiteral::Str(text.to_string())),
        Column::DateTime(_) => parse_utc_datetime(text),
    }
}

fn created_count(result: &graphforge_api::ExecutionResult, field: &str) -> Result<u64, SuiteError> {
    let mut total = 0;
    for batch in &result.batches {
        let column = batch
            .column_by_name(field)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
            .ok_or_else(|| invalid(format!("write summary has no {field}")))?;
        total += column.iter().flatten().sum::<u64>();
    }
    Ok(total)
}

/// Rows created by [`load_query_graph`], checked against the CSV row counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadSummary {
    pub nodes: u64,
    pub edges: u64,
}

/// Load the fixture CSV files into `forge` through public Cypher.
///
/// Every node file becomes one `UNWIND $rows ... CREATE` statement and every
/// edge file one `UNWIND ... MATCH ... CREATE`; the created counts must equal
/// the CSV row counts, so a dangling edge endpoint fails the load.
pub fn load_query_graph(forge: &GraphForge, directory: &Path) -> Result<LoadSummary, SuiteError> {
    let mut summary = LoadSummary::default();
    for node_file in NODE_FILES {
        let table = read_csv(directory, node_file.file)?;
        // Rows are grouped by which properties they carry: an absent optional
        // value is left off the CREATE rather than bound as null, because
        // GraphForge panics on a list of maps whose values mix null and string.
        let mut groups: BTreeMap<Vec<&'static str>, Vec<IrLiteral>> = BTreeMap::new();
        for row in &table.rows {
            let mut entries = Vec::new();
            for column in node_file.columns {
                let text = cell(&table, row, column.name(), node_file.file)?;
                let value = typed(*column, &text, node_file.file)?;
                if value != IrLiteral::Null {
                    entries.push((column.name().to_string(), value));
                }
            }
            let present = node_file
                .columns
                .iter()
                .map(|column| column.name())
                .filter(|name| entries.iter().any(|(key, _)| key == name))
                .collect();
            groups.entry(present).or_default().push(IrLiteral::Map(entries));
        }
        for (present, rows) in groups {
            let properties = present
                .iter()
                .map(|name| format!("{name}: row.{name}"))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "UNWIND $rows AS row CREATE (:{} {{{properties}}})",
                node_file.labels
            );
            let expected = rows.len() as u64;
            let result = forge
                .execute_with_params(
                    &query,
                    &HashMap::from([("rows".into(), IrLiteral::List(rows))]),
                )
                .map_err(|error| invalid(format!("load {}: {error}", node_file.file)))?;
            let created = created_count(&result, "nodes_created")?;
            if created != expected {
                return Err(invalid(format!(
                    "load {}: created {created} nodes for {expected} rows",
                    node_file.file
                )));
            }
            summary.nodes += created;
        }
    }
    for edge_file in EDGE_FILES {
        let table = read_csv(directory, edge_file.file)?;
        let mut rows = Vec::with_capacity(table.rows.len());
        for row in &table.rows {
            let mut entries = Vec::new();
            for (key, column) in [("source", edge_file.source.1), ("target", edge_file.target.1)] {
                let text = cell(&table, row, column, edge_file.file)?;
                entries.push((key.to_string(), typed(Column::Int(column), &text, edge_file.file)?));
            }
            for column in edge_file.properties {
                let text = cell(&table, row, column.name(), edge_file.file)?;
                entries.push((column.name().to_string(), typed(*column, &text, edge_file.file)?));
            }
            rows.push(IrLiteral::Map(entries));
        }
        let properties = if edge_file.properties.is_empty() {
            String::new()
        } else {
            format!(
                " {{{}}}",
                edge_file
                    .properties
                    .iter()
                    .map(|column| format!("{0}: row.{0}", column.name()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        // The endpoints are matched in two clauses separated by WITH: matching
        // both in one clause after UNWIND fails to plan in GraphForge once node
        // labels carry different property sets.
        let query = format!(
            "UNWIND $rows AS row \
             MATCH (source:{} {{id: row.source}}) \
             WITH source, row \
             MATCH (target:{} {{id: row.target}}) \
             CREATE (source)-[:{}{properties}]->(target)",
            edge_file.source.0, edge_file.target.0, edge_file.relationship
        );
        let expected = rows.len() as u64;
        let result = forge
            .execute_with_params(&query, &HashMap::from([("rows".into(), IrLiteral::List(rows))]))
            .map_err(|error| invalid(format!("load {}: {error}", edge_file.file)))?;
        let created = created_count(&result, "edges_created")?;
        if created != expected {
            return Err(invalid(format!(
                "load {}: created {created} edges for {expected} rows",
                edge_file.file
            )));
        }
        summary.edges += created;
    }
    Ok(summary)
}

/// Typed parameter bindings for every runnable query, keyed by operation.
pub type QueryParameters = BTreeMap<Operation, HashMap<String, IrLiteral>>;

fn bind_parameter(
    query: &BiQuery,
    name: &str,
    binding: &Value,
) -> Result<IrLiteral, SuiteError> {
    let declared = query
        .parameters
        .iter()
        .find(|parameter| parameter.name == name)
        .ok_or_else(|| invalid(format!("{} has no parameter {name}", query.operation)))?;
    let object = binding
        .as_object()
        .filter(|object| object.len() == 2)
        .ok_or_else(|| invalid(format!("{} {name}: binding must be {{kind, value}}", query.operation)))?;
    let kind = object.get("kind").and_then(Value::as_str).unwrap_or_default();
    if kind != declared.kind.name() {
        return Err(invalid(format!(
            "{} {name}: kind {kind:?} does not match declared {}",
            query.operation,
            declared.kind.name()
        )));
    }
    let value = object
        .get("value")
        .ok_or_else(|| invalid(format!("{} {name}: missing value", query.operation)))?;
    let wrong = || invalid(format!("{} {name}: value does not match kind {kind}", query.operation));
    let literal = match declared.kind {
        ParameterKind::String => IrLiteral::Str(value.as_str().ok_or_else(wrong)?.to_string()),
        ParameterKind::Int64 => IrLiteral::Int(value.as_i64().ok_or_else(wrong)?),
        ParameterKind::DateTime => parse_utc_datetime(value.as_str().ok_or_else(wrong)?)?,
        ParameterKind::StringList => IrLiteral::List(
            value
                .as_array()
                .ok_or_else(wrong)?
                .iter()
                .map(|item| item.as_str().map(|text| IrLiteral::Str(text.to_string())))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(wrong)?,
        ),
    };
    match (declared.fixed, &literal) {
        (Some(fixed), IrLiteral::Int(actual)) if *actual != fixed => Err(invalid(format!(
            "parameter_not_fixed_specification_value: {} {name} must be {fixed}, got {actual}",
            query.operation
        ))),
        _ => Ok(literal),
    }
}

/// Read `parameters.json`, requiring exactly the declared names and kinds for
/// every runnable query.
pub fn load_query_parameters(fixture: &Path) -> Result<QueryParameters, SuiteError> {
    let path = fixture.join("parameters.json");
    let text = fs::read_to_string(&path)
        .map_err(|error| invalid(format!("failed to read {}: {error}", path.display())))?;
    let document: Value =
        serde_json::from_str(&text).map_err(|error| invalid(format!("parameters.json: {error}")))?;
    if document.get("schema").and_then(Value::as_str) != Some(QUERY_PARAMETERS_SCHEMA) {
        return Err(invalid("parameters.json: unexpected schema"));
    }
    let queries = document
        .get("queries")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("parameters.json: missing queries"))?;
    let mut parameters = QueryParameters::new();
    for (code, bindings) in queries {
        let operation: Operation = code.parse()?;
        let query = crate::queries::bi_query(operation)
            .ok_or_else(|| invalid(format!("parameters.json: {code} is not a runnable query")))?;
        let bindings = bindings
            .as_object()
            .ok_or_else(|| invalid(format!("parameters.json: {code} must be an object")))?;
        let mut bound = HashMap::new();
        for (name, binding) in bindings {
            bound.insert(name.clone(), bind_parameter(query, name, binding)?);
        }
        for parameter in query.parameters {
            if !bound.contains_key(parameter.name) {
                return Err(invalid(format!(
                    "parameters.json: {code} is missing {}",
                    parameter.name
                )));
            }
        }
        parameters.insert(operation, bound);
    }
    for query in &BI_QUERIES {
        if !parameters.contains_key(&query.operation) {
            return Err(invalid(format!(
                "parameters.json: missing {}",
                query.operation
            )));
        }
    }
    Ok(parameters)
}

fn cell_value(column: &dyn Array, row: usize) -> Result<Value, SuiteError> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    let any = column.as_any();
    match column.data_type() {
        DataType::Int64 => Ok(Value::from(
            any.downcast_ref::<Int64Array>().expect("Int64").value(row),
        )),
        DataType::Float64 => Ok(Value::from(
            any.downcast_ref::<Float64Array>().expect("Float64").value(row),
        )),
        DataType::Utf8 => Ok(Value::from(
            any.downcast_ref::<StringArray>().expect("Utf8").value(row),
        )),
        DataType::Boolean => Ok(Value::from(
            any.downcast_ref::<BooleanArray>().expect("Boolean").value(row),
        )),
        DataType::Struct(_) => {
            let value = any.downcast_ref::<StructArray>().expect("Struct");
            let date = value
                .column_by_name("date")
                .and_then(|date| date.as_any().downcast_ref::<Int64Array>());
            let time = value
                .column_by_name("time")
                .filter(|time| *time.data_type() == DataType::Time64(TimeUnit::Nanosecond))
                .and_then(|time| time.as_any().downcast_ref::<Time64NanosecondArray>());
            let offset = value
                .column_by_name("offset")
                .and_then(|offset| offset.as_any().downcast_ref::<Int32Array>());
            match (date, time, offset) {
                (Some(date), Some(time), Some(offset)) if offset.value(row) == 0 => Ok(
                    Value::from(render_datetime(date.value(row), time.value(row))),
                ),
                _ => Err(invalid(format!(
                    "unsupported struct result column {:?}",
                    column.data_type()
                ))),
            }
        }
        other => Err(invalid(format!("unsupported result column type {other:?}"))),
    }
}

/// Result rows in `query.columns` order, as JSON values.
pub fn result_rows(query: &BiQuery, batches: &[RecordBatch]) -> Result<Vec<Vec<Value>>, SuiteError> {
    let mut rows = Vec::new();
    for batch in batches {
        let names: Vec<&str> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        if names != query.columns {
            return Err(invalid(format!(
                "{} returned columns {names:?}, expected {:?}",
                query.operation, query.columns
            )));
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| cell_value(column.as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(rows)
}

/// Run one query through the public API.
pub fn execute_query(
    forge: &GraphForge,
    query: &BiQuery,
    parameters: &HashMap<String, IrLiteral>,
) -> Result<Vec<Vec<Value>>, SuiteError> {
    let result = forge
        .execute_with_params(query.cypher, parameters)
        .map_err(|error| invalid(format!("live_api_execution_failed:{}: {error}", query.operation)))?;
    result_rows(query, &result.batches)
}

fn cells_match(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Number(expected), Value::Number(actual)) if expected.is_f64() || actual.is_f64() => {
            match (expected.as_f64(), actual.as_f64()) {
                (Some(left), Some(right)) if expected.is_f64() && actual.is_f64() => {
                    (left - right).abs() <= FLOAT_TOLERANCE * left.abs().max(right.abs()).max(1.0)
                }
                _ => false,
            }
        }
        _ => expected == actual,
    }
}

/// Compare ordered result rows; floats compare within a relative 1e-9.
pub fn compare_rows(
    operation: Operation,
    expected: &[Vec<Value>],
    actual: &[Vec<Value>],
) -> Result<(), SuiteError> {
    let mismatch = || {
        SuiteError::ReferenceMismatch(format!(
            "{operation}: expected {} rows {expected:?}, got {} rows {actual:?}",
            expected.len(),
            actual.len()
        ))
    };
    if expected.len() != actual.len() {
        return Err(mismatch());
    }
    for (expected_row, actual_row) in expected.iter().zip(actual) {
        if expected_row.len() != actual_row.len()
            || !expected_row
                .iter()
                .zip(actual_row)
                .all(|(left, right)| cells_match(left, right))
        {
            return Err(mismatch());
        }
    }
    Ok(())
}

/// Read the independently derived expected rows for `operation`.
pub fn load_expected_rows(fixture: &Path, operation: Operation) -> Result<Vec<Vec<Value>>, SuiteError> {
    let path = fixture.join("expected").join(format!("{operation}.json"));
    let text = fs::read_to_string(&path)
        .map_err(|error| invalid(format!("failed to read {}: {error}", path.display())))?;
    let document: Value =
        serde_json::from_str(&text).map_err(|error| invalid(format!("{operation}.json: {error}")))?;
    if document.get("schema").and_then(Value::as_str) != Some(QUERY_EXPECTED_SCHEMA)
        || document.get("operation").and_then(Value::as_str) != Some(operation.code())
        || document.get("authority").and_then(Value::as_str) != Some(EXPECTED_AUTHORITY)
    {
        return Err(invalid(format!("{operation}.json: unexpected identity")));
    }
    document
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("{operation}.json: missing rows")))?
        .iter()
        .map(|row| {
            row.as_array()
                .cloned()
                .ok_or_else(|| invalid(format!("{operation}.json: a row is not an array")))
        })
        .collect()
}

/// Load the fixture, run every runnable read, and compare each with its
/// independently derived expectation. Refused reads are reported with their
/// typed cause. Returns the evidence document; `status` is `passed` only when
/// every runnable read matched.
pub fn run_query_fixture(fixture: &Path) -> Result<Value, SuiteError> {
    let parameters = load_query_parameters(fixture)?;
    let forge = GraphForge::new(None).map_err(|error| invalid(format!("initialize: {error}")))?;
    let loaded = load_query_graph(&forge, &fixture.join("graph"))?;
    let mut operations = Vec::new();
    let mut all_passed = true;
    for query in &BI_QUERIES {
        let expected = load_expected_rows(fixture, query.operation)?;
        let outcome = execute_query(&forge, query, &parameters[&query.operation])
            .and_then(|rows| compare_rows(query.operation, &expected, &rows).map(|()| rows));
        let entry = match outcome {
            Ok(rows) => serde_json::json!({
                "operation": query.operation.code(),
                "status": "passed",
                "rows": rows.len(),
                "upstream": query.upstream,
                "rewrite": query.rewrite,
            }),
            Err(error) => {
                all_passed = false;
                serde_json::json!({
                    "operation": query.operation.code(),
                    "status": "failed",
                    "cause": error.to_string(),
                })
            }
        };
        operations.push(entry);
    }
    for refusal in &REFUSED_READS {
        operations.push(serde_json::json!({
            "operation": refusal.operation.code(),
            "status": "semantic_incompatibility",
            "cause": refusal.cause,
        }));
    }
    Ok(serde_json::json!({
        "schema": QUERY_EVIDENCE_SCHEMA,
        "suite_id": crate::SUITE_ID,
        "lane": "query_fixture_in_memory",
        "status": if all_passed { "passed" } else { "failed" },
        "certification": false,
        "interface": "graphforge_api::GraphForge::execute_with_params",
        "upstream_queries": {
            "source": crate::queries::UPSTREAM_QUERY_SOURCE,
            "commit": crate::queries::UPSTREAM_QUERY_COMMIT,
        },
        "expected_authority": EXPECTED_AUTHORITY,
        "loaded": {"nodes": loaded.nodes, "edges": loaded.edges},
        "operations": operations,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_datetimes_round_trip_through_civil_days() {
        let IrLiteral::ZonedDateTime { days, nanos, .. } =
            parse_utc_datetime("2012-03-05T14:22:11.123+00:00").unwrap()
        else {
            panic!("expected a zoned datetime");
        };
        assert_eq!(days, 15_404);
        assert_eq!(render_datetime(days, nanos), "2012-03-05T14:22:11.123Z");
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        for bad in [
            "2012-02-30T00:00:00.000+00:00",
            "2012-03-05T14:22:11.000+01:00",
            "2012-03-05 14:22:11.000+00:00",
        ] {
            assert!(parse_utc_datetime(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn float_cells_compare_within_tolerance_and_never_match_integers() {
        let row = |value: Value| vec![vec![value]];
        compare_rows(
            Operation::Bi1,
            &row(Value::from(0.1 + 0.2)),
            &row(Value::from(0.3)),
        )
        .unwrap();
        assert!(compare_rows(Operation::Bi1, &row(Value::from(0.3)), &row(Value::from(0.31))).is_err());
        assert!(compare_rows(Operation::Bi1, &row(Value::from(3.0)), &row(Value::from(3))).is_err());
        assert!(compare_rows(Operation::Bi1, &row(Value::from(3)), &row(Value::from(4))).is_err());
    }
}
