//! Mapping-driven conversion into the `gf import-session register-parquet`
//! layout: one Parquet file per table, plus `conversion-manifest.json`.

use std::collections::HashMap;
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

pub const MANIFEST_FILE: &str = "conversion-manifest.json";
pub const MANIFEST_SCHEMA: &str = "graphforge-gdc-conversion-manifest/1";

/// Result of a successful conversion.
#[derive(Debug)]
pub struct Conversion {
    pub manifest_path: PathBuf,
    pub manifest: Value,
}

/// Converts the files named by `mapping_bytes` under `input_root` into
/// `output_dir`, which must not exist or must be empty.
///
/// # Errors
/// Any typed [`ConvertError`]; nothing is published under its final name
/// unless its table converted completely, and the manifest is written last.
pub fn convert(
    mapping_bytes: &[u8],
    input_root: &Path,
    output_dir: &Path,
) -> Result<Conversion, ConvertError> {
    let mapping = Mapping::parse(mapping_bytes)?;
    prepare_output(output_dir)?;
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut keys: HashMap<(u32, i64), Uuid> = HashMap::new();
    let mut label_index: HashMap<&str, u32> = HashMap::new();
    for table in &mapping.node_tables {
        let next = u32::try_from(label_index.len())
            .map_err(|_| ConvertError::new(Cause::InvalidMapping, "too many labels"))?;
        label_index.entry(table.label.as_str()).or_insert(next);
    }
    for table in &mapping.node_tables {
        record_inputs(&table.id, &table.files, input_root, &mut inputs)?;
        let output = convert_nodes(table, input_root, output_dir, &label_index, &mut keys)?;
        outputs.push(output);
    }
    for table in &mapping.edge_tables {
        record_inputs(&table.id, &table.files, input_root, &mut inputs)?;
        let output = convert_edges(table, input_root, output_dir, &label_index, &keys)?;
        outputs.push(output);
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

    /// Finishes the footer, syncs, and renames to the final name.
    fn finish(self) -> Result<(PathBuf, u64), ConvertError> {
        let buffered = self
            .writer
            .into_inner()
            .map_err(|error| ConvertError::new(Cause::Io, error.to_string()))?;
        let file = buffered
            .into_inner()
            .map_err(|error| io_error(&self.partial.display().to_string(), error.error()))?;
        file.sync_all()
            .map_err(|error| io_error(&self.partial.display().to_string(), &error))?;
        fs::rename(&self.partial, &self.path)
            .map_err(|error| io_error(&self.path.display().to_string(), &error))?;
        Ok((self.path, self.rows))
    }
}

fn describe(
    kind: &str,
    table: &str,
    label_key: &str,
    label: &str,
    path: &Path,
    root: &Path,
    rows: u64,
) -> Result<Value, ConvertError> {
    let (sha256, bytes) = sha256_file(path)?;
    let relative = path.strip_prefix(root).unwrap_or(path);
    Ok(json!({
        "table": table,
        "kind": kind,
        label_key: label,
        "path": relative.to_string_lossy(),
        "rows": rows,
        "bytes": bytes,
        "sha256": sha256,
    }))
}

fn convert_nodes(
    table: &NodeTable,
    input_root: &Path,
    output_dir: &Path,
    labels: &HashMap<&str, u32>,
    keys: &mut HashMap<(u32, i64), Uuid>,
) -> Result<Value, ConvertError> {
    let label_id = labels[table.label.as_str()];
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
        read_table(table.format, &path, &required, &mut |batch, first_row| {
            let ids = text_column(batch, &table.id_column);
            let mut uuids = Vec::with_capacity(batch.num_rows());
            for index in 0..batch.num_rows() {
                let row = first_row + index as u64;
                let id = id_value(ids, index, &table.id_column, &path, row)?;
                let uuid = node_uuid(&table.label, id);
                if keys.insert((label_id, id), uuid).is_some() {
                    return Err(row_error(
                        Cause::DuplicateNodeIdentity,
                        &path,
                        row,
                        &format!(
                            "node ({}, {id}) appears more than once (table {})",
                            table.label, table.id
                        ),
                    ));
                }
                uuids.push(uuid);
            }
            let mut columns = vec![
                uuid_array(&uuids)?,
                Arc::new(StringArray::from(vec![table.label.as_str(); uuids.len()])) as ArrayRef,
            ];
            columns.extend(property_arrays(batch, &properties, &path, first_row)?);
            writer.write(columns)
        })?;
    }
    let (path, rows) = writer.finish()?;
    describe(
        "nodes",
        &table.id,
        "label",
        &table.label,
        &path,
        output_dir,
        rows,
    )
}

fn convert_edges(
    table: &EdgeTable,
    input_root: &Path,
    output_dir: &Path,
    labels: &HashMap<&str, u32>,
    keys: &HashMap<(u32, i64), Uuid>,
) -> Result<Value, ConvertError> {
    let properties = sorted_properties(&table.properties);
    let source_label = labels[table.source.label.as_str()];
    let target_label = labels[table.target.label.as_str()];
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
                let lookup = |label: u32, name: &str, id: i64| {
                    keys.get(&(label, id)).copied().ok_or_else(|| {
                        row_error(
                            Cause::DanglingEndpoint,
                            &path,
                            row,
                            &format!("{name} node {id} is not defined by any node table"),
                        )
                    })
                };
                source_ids.push(lookup(source_label, "source", source)?);
                target_ids.push(lookup(target_label, "target", target)?);
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
    let (path, rows) = writer.finish()?;
    describe(
        "edges",
        &table.id,
        "rel_type",
        &table.rel_type,
        &path,
        output_dir,
        rows,
    )
}
