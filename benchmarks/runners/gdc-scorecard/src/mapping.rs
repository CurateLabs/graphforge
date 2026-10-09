//! Declarative load mapping: which input files become which node and edge
//! tables, which column is the identity, and which columns become properties.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::error::{Cause, ConvertError};

pub const MAPPING_SCHEMA: &str = "graphforge-gdc-load-mapping/1";

const RESERVED: [&str; 6] = [
    "node_uuid",
    "label",
    "edge_uuid",
    "rel_type",
    "source_uuid",
    "target_uuid",
];

/// Input file format. A file whose name ends in `.gz` is gzip-decompressed
/// first, whatever its format.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    /// Pipe-delimited CSV with a header row and no quoting. A repeated header
    /// name gets a `.1`, `.2`, ... suffix on its second, third, ... occurrence,
    /// so `Person.id|Person.id` reads as `Person.id`, `Person.id.1`.
    LdbcCsv,
    /// One vertex id per line. Column: `id`.
    GraphalyticsVertices,
    /// `source target [weight]`, whitespace separated. Columns: `source`,
    /// `target` and, when present, `weight`.
    GraphalyticsEdges,
}

/// The property types the storage read path decodes, each written in
/// GraphForge's canonical persisted Arrow form. Narrower integer types are
/// deliberately absent: import accepts them but queries cannot read them.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    String,
    Int64,
    Float64,
    #[serde(alias = "bool")]
    Boolean,
    /// A Cypher `date`: `Struct{epoch_day: Int64}`.
    Date,
    /// A Cypher `datetime`: `Struct{date: Int64, time: Time64(ns), offset: Int32,
    /// zone: Utf8}`, the local date and time as written and its UTC offset.
    Datetime,
    /// A list of strings split on `separator`: `List<Utf8>`.
    List,
}

/// How a `date` or `datetime` column is written in the input.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TemporalFormat {
    /// `YYYY-MM-DD` for a date. For a datetime,
    /// `YYYY-MM-DDTHH:MM:SS[.fraction]` and a mandatory `Z`, `+HH:MM` or
    /// `+HHMM` offset.
    #[default]
    Iso8601,
    /// `YYYY-MM-DD HH:MM:SS[.fraction]` with no offset, read as UTC. A date in
    /// this format must be exactly midnight.
    NaiveUtc,
    /// Milliseconds since the Unix epoch. A date must be a whole UTC day.
    EpochMillis,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    pub column: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub kind: PropertyType,
    /// For `date` and `datetime`, how the text is written; defaults to
    /// [`TemporalFormat::Iso8601`]. An `int64` with a format stores the instant
    /// the text names as whole epoch milliseconds, the form a Cypher query that
    /// compares against epoch-millisecond integers needs.
    #[serde(default)]
    pub format: Option<TemporalFormat>,
    /// Required for `list` and refused otherwise: the one-character separator.
    #[serde(default)]
    pub separator: Option<String>,
}

impl Property {
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.column)
    }

    #[must_use]
    pub fn temporal_format(&self) -> TemporalFormat {
        self.format.unwrap_or_default()
    }

    /// The list separator. [`Mapping::parse`] guarantees a list has one.
    #[must_use]
    pub fn separator_char(&self) -> Option<char> {
        self.separator
            .as_deref()
            .and_then(|text| text.chars().next())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeTable {
    pub id: String,
    pub format: Format,
    /// Paths relative to the input root. Any component may use the `*` and `?`
    /// wildcards of [`crate::glob`].
    pub files: Vec<String>,
    /// The identity label: it derives every node UUID, it is the label edge
    /// endpoints name, and it is the stored label unless `label_column` is set.
    pub label: String,
    pub id_column: String,
    /// When set, a row's stored label is `label_values[row[label_column]]`, and
    /// a value outside `label_values` is an `invalid_value`. Identity still
    /// comes from `label`, so the stored label never changes a node's UUID.
    #[serde(default)]
    pub label_column: Option<String>,
    #[serde(default)]
    pub label_values: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub properties: Vec<Property>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub label: String,
    pub column: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTable {
    pub id: String,
    pub format: Format,
    /// As [`NodeTable::files`].
    pub files: Vec<String>,
    pub rel_type: String,
    pub source: Endpoint,
    pub target: Endpoint,
    #[serde(default)]
    pub properties: Vec<Property>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub schema: String,
    pub node_tables: Vec<NodeTable>,
    #[serde(default)]
    pub edge_tables: Vec<EdgeTable>,
}

fn invalid(message: impl Into<String>) -> ConvertError {
    ConvertError::new(Cause::InvalidMapping, message)
}

impl Mapping {
    /// # Errors
    /// `invalid_mapping` for malformed JSON or any structural violation.
    pub fn parse(bytes: &[u8]) -> Result<Self, ConvertError> {
        let mapping: Self = serde_json::from_slice(bytes)
            .map_err(|error| invalid(format!("mapping JSON: {error}")))?;
        mapping.validate()?;
        Ok(mapping)
    }

    fn validate(&self) -> Result<(), ConvertError> {
        if self.schema != MAPPING_SCHEMA {
            return Err(invalid(format!("schema must be {MAPPING_SCHEMA}")));
        }
        if self.node_tables.is_empty() {
            return Err(invalid("at least one node table is required"));
        }
        let mut ids = BTreeSet::new();
        let mut labels = BTreeSet::new();
        for table in &self.node_tables {
            check_table(&table.id, &table.files, &mut ids)?;
            check_name("label", &table.label)?;
            check_label_column(table)?;
            check_properties(&table.id, &table.properties)?;
            labels.insert(table.label.as_str());
        }
        for table in &self.edge_tables {
            check_table(&table.id, &table.files, &mut ids)?;
            check_name("rel_type", &table.rel_type)?;
            check_properties(&table.id, &table.properties)?;
            for endpoint in [&table.source, &table.target] {
                if !labels.contains(endpoint.label.as_str()) {
                    return Err(invalid(format!(
                        "edge table {} references label {} that no node table defines",
                        table.id, endpoint.label
                    )));
                }
            }
        }
        Ok(())
    }
}

fn check_name(what: &str, value: &str) -> Result<(), ConvertError> {
    if value.is_empty() || value.contains('\0') {
        return Err(invalid(format!("{what} must be a non-empty string")));
    }
    Ok(())
}

fn check_table<'a>(
    id: &'a str,
    files: &[String],
    ids: &mut BTreeSet<&'a str>,
) -> Result<(), ConvertError> {
    let valid_id = !id.is_empty()
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        });
    if !valid_id {
        return Err(invalid(format!("table id {id:?} must match [a-z0-9_-]+")));
    }
    if !ids.insert(id) {
        return Err(invalid(format!("table id {id} is used twice")));
    }
    if files.is_empty() {
        return Err(invalid(format!("table {id} lists no files")));
    }
    for file in files {
        let path = std::path::Path::new(file);
        let escapes = path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)));
        if file.is_empty() || escapes {
            return Err(invalid(format!(
                "table {id} file {file:?} must be a relative path inside the input root"
            )));
        }
    }
    Ok(())
}

fn check_label_column(table: &NodeTable) -> Result<(), ConvertError> {
    match (&table.label_column, &table.label_values) {
        (None, None) => Ok(()),
        (Some(column), Some(values)) if !values.is_empty() => {
            check_name("label_column", column)?;
            for (value, label) in values {
                check_name("label_values key", value)?;
                check_name("label_values label", label)?;
            }
            Ok(())
        }
        _ => Err(invalid(format!(
            "table {} must set label_column and a non-empty label_values together",
            table.id
        ))),
    }
}

fn check_property_options(table: &str, property: &Property) -> Result<(), ConvertError> {
    let name = property.output_name();
    let temporal = matches!(
        property.kind,
        PropertyType::Date | PropertyType::Datetime | PropertyType::Int64
    );
    if property.format.is_some() && !temporal {
        return Err(invalid(format!(
            "table {table} property {name}: format applies only to date, datetime and int64"
        )));
    }
    match (&property.separator, property.kind) {
        (None, PropertyType::List) => Err(invalid(format!(
            "table {table} property {name}: a list needs a separator"
        ))),
        (Some(_), kind) if kind != PropertyType::List => Err(invalid(format!(
            "table {table} property {name}: separator applies only to a list"
        ))),
        (Some(separator), _) if separator.chars().count() != 1 || separator == "|" => Err(invalid(
            format!("table {table} property {name}: separator must be one character other than |"),
        )),
        _ => Ok(()),
    }
}

fn check_properties(table: &str, properties: &[Property]) -> Result<(), ConvertError> {
    let mut names = BTreeSet::new();
    for property in properties {
        let name = property.output_name();
        check_name("property name", name)?;
        check_property_options(table, property)?;
        if RESERVED.contains(&name) {
            return Err(invalid(format!(
                "table {table} property {name} is reserved by the import contract"
            )));
        }
        if !names.insert(name) {
            return Err(invalid(format!(
                "table {table} property {name} is duplicated"
            )));
        }
    }
    Ok(())
}
