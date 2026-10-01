//! Declarative load mapping: which input files become which node and edge
//! tables, which column is the identity, and which columns become properties.

use std::collections::BTreeSet;

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    /// Pipe-delimited CSV with a header row and no quoting.
    LdbcCsv,
    /// One vertex id per line. Column: `id`.
    GraphalyticsVertices,
    /// `source target [weight]`, whitespace separated. Columns: `source`,
    /// `target` and, when present, `weight`.
    GraphalyticsEdges,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    String,
    Int32,
    Int64,
    Float64,
    #[serde(alias = "bool")]
    Boolean,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    pub column: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub kind: PropertyType,
}

impl Property {
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.column)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeTable {
    pub id: String,
    pub format: Format,
    pub files: Vec<String>,
    pub label: String,
    pub id_column: String,
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

fn check_properties(table: &str, properties: &[Property]) -> Result<(), ConvertError> {
    let mut names = BTreeSet::new();
    for property in properties {
        let name = property.output_name();
        check_name("property name", name)?;
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
