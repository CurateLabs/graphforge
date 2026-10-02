//! Mapping-driven conversion into the `gf import-session register-parquet`
//! layout: one Parquet file per table, plus `conversion-manifest.json`.

use std::fs::{self, File};
use std::io::{BufWriter, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::{Cause, ConvertError, io_error};
use crate::identity::{Uuid, edge_uuid, hex, node_uuid};
use crate::mapping::{EdgeTable, Mapping, NodeTable, Property, PropertyType};
use crate::source::read_table;
use crate::spill::{
    Budget, DEFAULT_MEMORY_BUDGET_BYTES, Duplicate, Key, KeySorter, SpillDir, check_nodes,
    find_dangling,
};

pub const MANIFEST_FILE: &str = "conversion-manifest.json";
pub const MANIFEST_SCHEMA: &str = "graphforge-gdc-conversion-manifest/1";

/// Result of a successful conversion.
#[derive(Debug)]
pub struct Conversion {
    pub manifest_path: PathBuf,
    pub manifest: Value,
}

/// Converts with [`DEFAULT_MEMORY_BUDGET_BYTES`]; see [`convert_with_budget`].
///
/// # Errors
/// As [`convert_with_budget`].
pub fn convert(
    mapping_bytes: &[u8],
    input_root: &Path,
    output_dir: &Path,
) -> Result<Conversion, ConvertError> {
    convert_with_budget(
        mapping_bytes,
        input_root,
        output_dir,
        DEFAULT_MEMORY_BUDGET_BYTES,
    )
}

/// Converts the files named by `mapping_bytes` under `input_root` into
/// `output_dir`, which must not exist or must be empty.
///
/// Rows stream in input order. Identity checks spill sorted key runs to
/// `output_dir/.spill/`, so the memory they use stays within
/// `memory_budget_bytes` whatever the input size. Row-level errors (malformed input, invalid values, missing
/// columns) are reported as each row is read. A duplicate (label, id) is
/// reported once every node table has been read, and a dangling endpoint once
/// every edge table has been read; each reports the occurrence the earliest in
/// input order (mapping table order, then file order, then row; a source
/// endpoint before the target on the same row).
///
/// # Errors
/// Any typed [`ConvertError`]. Tables are written to `*.partial` and renamed
/// only after both identity checks pass, the manifest is written last, and
/// the spill directory is removed on success and on failure.
pub fn convert_with_budget(
    mapping_bytes: &[u8],
    input_root: &Path,
    output_dir: &Path,
    memory_budget_bytes: u64,
) -> Result<Conversion, ConvertError> {
    let budget = Budget::new(memory_budget_bytes)?;
    let mapping = Mapping::parse(mapping_bytes)?;
    prepare_output(output_dir)?;
    let spill = SpillDir::create(output_dir)?;
    let mut inputs = Vec::new();
    let mut files = Vec::new();
    let mut pending = Vec::new();
    let mut labels: Vec<&str> = Vec::new();
    for table in &mapping.node_tables {
        if !labels.contains(&table.label.as_str()) {
            labels.push(table.label.as_str());
        }
    }
    u32::try_from(labels.len())
        .map_err(|_| ConvertError::new(Cause::InvalidMapping, "too many labels"))?;
    let label_index = |label: &str| {
        let index = labels.iter().position(|known| *known == label);
        u32::try_from(index.expect("mapping validated every label")).expect("label count fits u32")
    };

    let mut node_keys = KeySorter::new(&spill, budget, false);
    for table in &mapping.node_tables {
        record_inputs(&table.id, &table.files, input_root, &mut inputs)?;
        let label = label_index(&table.label);
        pending.push(convert_nodes(
            table,
            label,
            input_root,
            output_dir,
            &mut files,
            &mut node_keys,
        )?);
    }
    let node_runs = node_keys.finish()?;
    let (defined, duplicate) = check_nodes(&node_runs)?;
    if let Some(duplicate) = duplicate {
        return Err(duplicate_error(&duplicate, &files, &labels));
    }

    let mut endpoint_keys = KeySorter::new(&spill, budget, true);
    for table in &mapping.edge_tables {
        record_inputs(&table.id, &table.files, input_root, &mut inputs)?;
        let endpoints = (
            label_index(&table.source.label),
            label_index(&table.target.label),
        );
        pending.push(convert_edges(
            table,
            endpoints,
            input_root,
            output_dir,
            &mut files,
            &mut endpoint_keys,
        )?);
    }
    let endpoint_runs = endpoint_keys.finish()?;
    if let Some(dangling) = find_dangling(&endpoint_runs, &defined)? {
        return Err(dangling_error(&dangling, &files));
    }
    let spill_record = json!({
        "budget": budget.describe(),
        "node_keys": node_runs.stats().describe(),
        "endpoint_keys": endpoint_runs.stats().describe(),
    });
    spill.close()?;

    let mut outputs = Vec::with_capacity(pending.len());
    for table in pending {
        outputs.push(table.publish(output_dir)?);
    }
    let manifest = json!({
        "schema": MANIFEST_SCHEMA,
        "converter": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "identity_derivation": "sha256-prefix-uuidv7-shape/1",
        },
        "mapping_sha256": hex(&Sha256::digest(mapping_bytes)),
        "inputs": inputs,
        "outputs": outputs,
        "spill": spill_record,
    });
    let manifest_path = output_dir.join(MANIFEST_FILE);
    let mut text = serde_json::to_string_pretty(&manifest)
        .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
    text.push('\n');
    write_durable(&manifest_path, text.as_bytes())?;
    Ok(Conversion {
        manifest_path,
        manifest,
    })
}

/// One (table, file) occurrence in mapping order; a [`Key`] names it by index.
struct InputFile {
    table: String,
    path: PathBuf,
}

fn file_index(files: &mut Vec<InputFile>, table: &str, path: &Path) -> Result<u32, ConvertError> {
    let index = u32::try_from(files.len())
        .map_err(|_| ConvertError::new(Cause::InvalidMapping, "too many input files"))?;
    files.push(InputFile {
        table: table.to_owned(),
        path: path.to_path_buf(),
    });
    Ok(index)
}

fn duplicate_error(duplicate: &Duplicate, files: &[InputFile], labels: &[&str]) -> ConvertError {
    let second = &files[duplicate.second.file as usize];
    let first = &files[duplicate.first.file as usize];
    row_error(
        Cause::DuplicateNodeIdentity,
        &second.path,
        duplicate.second.position,
        &format!(
            "node ({}, {}) appears more than once (table {}); first defined at {} row {} (table {})",
            labels[duplicate.second.label as usize],
            duplicate.second.id,
            second.table,
            first.path.display(),
            duplicate.first.position,
            first.table,
        ),
    )
}

fn dangling_error(endpoint: &Key, files: &[InputFile]) -> ConvertError {
    let side = if endpoint.position & 1 == 0 {
        "source"
    } else {
        "target"
    };
    row_error(
        Cause::DanglingEndpoint,
        &files[endpoint.file as usize].path,
        endpoint.position >> 1,
        &format!(
            "{side} node {} is not defined by any node table",
            endpoint.id
        ),
    )
}

fn prepare_output(output_dir: &Path) -> Result<(), ConvertError> {
    if output_dir.exists() {
        let mut entries = fs::read_dir(output_dir)
            .map_err(|error| io_error(&output_dir.display().to_string(), &error))?;
        if entries.next().is_some() {
            return Err(ConvertError::new(
                Cause::OutputExists,
                format!("{} is not empty", output_dir.display()),
            ));
        }
    }
    for sub in ["nodes", "edges"] {
        fs::create_dir_all(output_dir.join(sub))
            .map_err(|error| io_error(&output_dir.display().to_string(), &error))?;
    }
    Ok(())
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), ConvertError> {
    use std::io::Write;
    let mut file =
        File::create(path).map_err(|error| io_error(&path.display().to_string(), &error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| io_error(&path.display().to_string(), &error))
}

fn sha256_file(path: &Path) -> Result<(String, u64), ConvertError> {
    let mut file = File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ConvertError::new(Cause::InputMissing, path.display().to_string())
        } else {
            io_error(&path.display().to_string(), &error)
        }
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 20];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error(&path.display().to_string(), &error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }
    Ok((hex(&hasher.finalize()), total))
}

fn record_inputs(
    table: &str,
    files: &[String],
    input_root: &Path,
    inputs: &mut Vec<Value>,
) -> Result<(), ConvertError> {
    for file in files {
        let (sha256, bytes) = sha256_file(&input_root.join(file))?;
        inputs.push(json!({"table": table, "path": file, "bytes": bytes, "sha256": sha256}));
    }
    Ok(())
}

fn arrow_type(kind: PropertyType) -> DataType {
    match kind {
        PropertyType::String => DataType::Utf8,
        PropertyType::Int64 => DataType::Int64,
        PropertyType::Float64 => DataType::Float64,
        PropertyType::Boolean => DataType::Boolean,
    }
}

/// Import requires property columns in lexicographic name order after the
/// fixed topology columns, so the output order is the sorted order.
fn sorted_properties(properties: &[Property]) -> Vec<Property> {
    let mut sorted = properties.to_vec();
    sorted.sort_by(|left, right| left.output_name().cmp(right.output_name()));
    sorted
}

fn property_fields(properties: &[Property]) -> Vec<Field> {
    properties
        .iter()
        .map(|property| Field::new(property.output_name(), arrow_type(property.kind), true))
        .collect()
}

enum Column {
    Text(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

impl Column {
    fn new(kind: PropertyType) -> Self {
        match kind {
            PropertyType::String => Self::Text(StringBuilder::new()),
            PropertyType::Int64 => Self::Int64(Int64Builder::new()),
            PropertyType::Float64 => Self::Float64(Float64Builder::new()),
            PropertyType::Boolean => Self::Boolean(BooleanBuilder::new()),
        }
    }

    /// Empty and missing fields are null. Anything else must parse exactly.
    fn append(&mut self, value: Option<&str>) -> Result<(), String> {
        let value = value.filter(|text| !text.is_empty());
        match self {
            Self::Text(builder) => builder.append_option(value),
            Self::Int64(builder) => builder.append_option(parse(value)?),
            Self::Float64(builder) => builder.append_option(parse(value)?),
            Self::Boolean(builder) => builder.append_option(parse(value)?),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Text(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
        }
    }
}

fn parse<T: std::str::FromStr>(value: Option<&str>) -> Result<Option<T>, String> {
    value
        .map(|text| {
            text.parse::<T>().map_err(|_| {
                format!(
                    "value {text:?} is not a valid {}",
                    std::any::type_name::<T>()
                )
            })
        })
        .transpose()
}

fn text_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .expect("required column was verified against the header")
}

fn row_error(cause: Cause, file: &Path, row: u64, message: &str) -> ConvertError {
    ConvertError::new(cause, format!("{} row {row}: {message}", file.display()))
}

fn id_value(
    column: &StringArray,
    index: usize,
    name: &str,
    file: &Path,
    row: u64,
) -> Result<i64, ConvertError> {
    if column.is_null(index) || column.value(index).is_empty() {
        return Err(row_error(
            Cause::InvalidValue,
            file,
            row,
            &format!("{name} is empty"),
        ));
    }
    column.value(index).parse::<i64>().map_err(|_| {
        row_error(
            Cause::InvalidValue,
            file,
            row,
            &format!("{name} {:?} is not an integer id", column.value(index)),
        )
    })
}

fn property_arrays(
    batch: &RecordBatch,
    properties: &[Property],
    file: &Path,
    first_row: u64,
) -> Result<Vec<ArrayRef>, ConvertError> {
    let mut arrays = Vec::with_capacity(properties.len());
    for property in properties {
        let source = text_column(batch, &property.column);
        let mut column = Column::new(property.kind);
        for index in 0..batch.num_rows() {
            let value = (!source.is_null(index)).then(|| source.value(index));
            column.append(value).map_err(|message| {
                row_error(
                    Cause::InvalidValue,
                    file,
                    first_row + index as u64,
                    &format!("column {}: {message}", property.column),
                )
            })?;
        }
        arrays.push(column.finish());
    }
    Ok(arrays)
}

fn uuid_array(values: &[Uuid]) -> Result<ArrayRef, ConvertError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder
            .append_value(value)
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
    }
    Ok(Arc::new(builder.finish()))
}

struct TableWriter {
    writer: ArrowWriter<BufWriter<File>>,
    partial: PathBuf,
    path: PathBuf,
    schema: Arc<Schema>,
    rows: u64,
}

impl TableWriter {
    fn create(path: PathBuf, schema: Arc<Schema>) -> Result<Self, ConvertError> {
        let mut partial = path.clone().into_os_string();
        partial.push(".partial");
        let partial = PathBuf::from(partial);
        let file = File::create(&partial)
            .map_err(|error| io_error(&partial.display().to_string(), &error))?;
        let writer = ArrowWriter::try_new(BufWriter::new(file), Arc::clone(&schema), None)
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
        Ok(Self {
            writer,
            partial,
            path,
            schema,
            rows: 0,
        })
    }

    fn write(&mut self, columns: Vec<ArrayRef>) -> Result<(), ConvertError> {
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
        self.rows += batch.num_rows() as u64;
        self.writer
            .write(&batch)
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))
    }

    /// Finishes the footer and syncs; the table keeps its `.partial` name
    /// until [`PendingTable::publish`].
    fn finish(self, description: Description) -> Result<PendingTable, ConvertError> {
        let buffered = self
            .writer
            .into_inner()
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
        let file = buffered
            .into_inner()
            .map_err(|error| io_error(&self.partial.display().to_string(), error.error()))?;
        file.sync_all()
            .map_err(|error| io_error(&self.partial.display().to_string(), &error))?;
        Ok(PendingTable {
            partial: self.partial,
            path: self.path,
            rows: self.rows,
            description,
        })
    }
}

/// Manifest fields of one output table.
struct Description {
    kind: &'static str,
    table: String,
    label_key: &'static str,
    label: String,
}

/// A complete table still under its `.partial` name.
struct PendingTable {
    partial: PathBuf,
    path: PathBuf,
    rows: u64,
    description: Description,
}

impl PendingTable {
    /// Renames to the final name and describes the published file.
    fn publish(self, root: &Path) -> Result<Value, ConvertError> {
        fs::rename(&self.partial, &self.path)
            .map_err(|error| io_error(&self.path.display().to_string(), &error))?;
        let (sha256, bytes) = sha256_file(&self.path)?;
        let relative = self.path.strip_prefix(root).unwrap_or(&self.path);
        let description = self.description;
        Ok(json!({
            "table": description.table,
            "kind": description.kind,
            description.label_key: description.label,
            "path": relative.to_string_lossy(),
            "rows": self.rows,
            "bytes": bytes,
            "sha256": sha256,
        }))
    }
}

fn convert_nodes(
    table: &NodeTable,
    label: u32,
    input_root: &Path,
    output_dir: &Path,
    files: &mut Vec<InputFile>,
    keys: &mut KeySorter<'_>,
) -> Result<PendingTable, ConvertError> {
    let properties = sorted_properties(&table.properties);
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ];
    fields.extend(property_fields(&properties));
    let mut writer = TableWriter::create(
        output_dir
            .join("nodes")
            .join(format!("{}.parquet", table.id)),
        Arc::new(Schema::new(fields)),
    )?;
    let mut required: Vec<&str> = vec![table.id_column.as_str()];
    required.extend(
        table
            .properties
            .iter()
            .map(|property| property.column.as_str()),
    );
    for file in &table.files {
        let path = input_root.join(file);
        let file = file_index(files, &table.id, &path)?;
        read_table(table.format, &path, &required, &mut |batch, first_row| {
            let ids = text_column(batch, &table.id_column);
            let mut uuids = Vec::with_capacity(batch.num_rows());
            for index in 0..batch.num_rows() {
                let row = first_row + index as u64;
                let id = id_value(ids, index, &table.id_column, &path, row)?;
                keys.push(Key {
                    label,
                    id,
                    file,
                    position: row,
                })?;
                uuids.push(node_uuid(&table.label, id));
            }
            let mut columns = vec![
                uuid_array(&uuids)?,
                Arc::new(StringArray::from(vec![table.label.as_str(); uuids.len()])) as ArrayRef,
            ];
            columns.extend(property_arrays(batch, &properties, &path, first_row)?);
            writer.write(columns)
        })?;
    }
    writer.finish(Description {
        kind: "nodes",
        table: table.id.clone(),
        label_key: "label",
        label: table.label.clone(),
    })
}

fn convert_edges(
    table: &EdgeTable,
    (source_label, target_label): (u32, u32),
    input_root: &Path,
    output_dir: &Path,
    files: &mut Vec<InputFile>,
    keys: &mut KeySorter<'_>,
) -> Result<PendingTable, ConvertError> {
    let properties = sorted_properties(&table.properties);
    let mut fields = vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ];
    fields.extend(property_fields(&properties));
    let mut writer = TableWriter::create(
        output_dir
            .join("edges")
            .join(format!("{}.parquet", table.id)),
        Arc::new(Schema::new(fields)),
    )?;
    let mut required: Vec<&str> = vec![table.source.column.as_str(), table.target.column.as_str()];
    required.extend(
        table
            .properties
            .iter()
            .map(|property| property.column.as_str()),
    );
    let mut ordinal = 0_u64;
    for file in &table.files {
        let path = input_root.join(file);
        let file = file_index(files, &table.id, &path)?;
        read_table(table.format, &path, &required, &mut |batch, first_row| {
            let sources = text_column(batch, &table.source.column);
            let targets = text_column(batch, &table.target.column);
            let rows = batch.num_rows();
            let (mut edge_ids, mut source_ids, mut target_ids) = (
                Vec::with_capacity(rows),
                Vec::with_capacity(rows),
                Vec::with_capacity(rows),
            );
            for index in 0..rows {
                let row = first_row + index as u64;
                let source = id_value(sources, index, &table.source.column, &path, row)?;
                let target = id_value(targets, index, &table.target.column, &path, row)?;
                for (label, id, side) in [(source_label, source, 0), (target_label, target, 1)] {
                    keys.push(Key {
                        label,
                        id,
                        file,
                        position: row << 1 | side,
                    })?;
                }
                source_ids.push(node_uuid(&table.source.label, source));
                target_ids.push(node_uuid(&table.target.label, target));
                edge_ids.push(edge_uuid(&table.id, ordinal));
                ordinal += 1;
            }
            let mut columns = vec![
                uuid_array(&edge_ids)?,
                Arc::new(StringArray::from(vec![table.rel_type.as_str(); rows])) as ArrayRef,
                uuid_array(&source_ids)?,
                uuid_array(&target_ids)?,
            ];
            columns.extend(property_arrays(batch, &properties, &path, first_row)?);
            writer.write(columns)
        })?;
    }
    writer.finish(Description {
        kind: "edges",
        table: table.id.clone(),
        label_key: "rel_type",
        label: table.rel_type.clone(),
    })
}
