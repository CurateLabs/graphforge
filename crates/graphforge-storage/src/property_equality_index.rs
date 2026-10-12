//! Persisted equality index over a node property (#1983).
//!
//! `MATCH (n:L {id: $x})` must cost the matches, not the label's rows. The
//! index maps one indexed property column of one node route to the UUIDs whose
//! newest live snapshot holds each value. Every property named `id` is
//! indexed; the import session declares no key columns, so there is nothing
//! else to index by declaration.
//!
//! Layout, below `indexes/property/`, one pair per (route, column):
//!
//! - `<stem>.eq`: a manifest. An XXH64 line over the JSON body, then the JSON:
//!   format, route, column, value type, the digest of the route's source
//!   objects, and every shard's first and last value, length and XXH64.
//! - `<stem>.shards/<n>.eqs`: bounded Arrow IPC shards of `(value, node_uuid)`
//!   rows sorted by value then UUID.
//!
//! A lookup binary-searches the manifest, then reads and authenticates only the
//! shards whose range holds the value. The bytes are a pure function of the
//! route's content: they hold no clock, UUID or process identity.
//!
//! An index is current only while its source digest equals the route's. A
//! missing or stale index is not used and is never rebuilt by a read; the scan
//! answers instead. A manifest or shard that fails its checksum is refused.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_ir::IrLiteral;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::property_overlay::{
    AuthenticatedPropertyInventory, EqualityValue, PropertyEquality, PropertyOverlayLimits,
    PropertyRouteKind, RouteRead,
};

/// Manifest format identifier.
pub const FORMAT: &str = "graphforge-property-equality-index/1";
/// Rows per shard. Fixed, so a lookup's reads do not depend on the route size.
const SHARD_ENTRIES: usize = 4096;
/// Properties indexed on every node route.
const INDEXED_COLUMNS: [&str; 1] = ["id"];

/// Whether `column` is indexed.
#[must_use]
pub fn is_indexed_column(column: &str) -> bool {
    INDEXED_COLUMNS.contains(&column)
}

/// `<project>/indexes/property`.
#[must_use]
pub fn index_dir(project_dir: &Path) -> PathBuf {
    project_dir.join("indexes").join("property")
}

fn stem(route: &str, column: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(route.as_bytes());
    hasher.update([0]);
    hasher.update(column.as_bytes());
    let digest = hasher.finalize();
    hex(&digest[..16])
}

/// A digest of the exact objects a route's state derives from: each object's
/// path, length and XXH64 as the manifest names them, in path order.
pub(crate) fn source_digest(mut parts: Vec<(&str, u64, u64)>) -> String {
    parts.sort_unstable();
    let mut hasher = Sha256::new();
    for (path, length, xxh64) in parts {
        hasher.update(u64::try_from(path.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(path.as_bytes());
        hasher.update(length.to_le_bytes());
        hasher.update(xxh64.to_le_bytes());
    }
    hex(&hasher.finalize())
}

/// Lowercase hexadecimal text of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn manifest_path(project_dir: &Path, route: &str, column: &str) -> PathBuf {
    index_dir(project_dir).join(format!("{}.eq", stem(route, column)))
}

fn shard_dir(project_dir: &Path, route: &str, column: &str) -> PathBuf {
    index_dir(project_dir).join(format!("{}.shards", stem(route, column)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum Bound {
    Int(i64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ShardEntry {
    file: String,
    rows: u64,
    first: Bound,
    last: Bound,
    byte_length: u64,
    xxh64: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    format: String,
    route: String,
    column: String,
    value_type: String,
    source_digest: String,
    entries: u64,
    shards: Vec<ShardEntry>,
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(format!("property equality index: {}", message.into()))
}

fn storage(message: &str, error: &std::io::Error) -> GfError {
    GfError::Storage(format!("property equality index: {message}: {error}"))
}

pub(crate) fn value_type_name(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Int64 => Some("int64"),
        DataType::Utf8 => Some("utf8"),
        DataType::Boolean => Some("bool"),
        _ => None,
    }
}

pub(crate) enum Entries {
    Int(Vec<(i64, [u8; 16])>),
    Str(Vec<(String, [u8; 16])>),
    Bool(Vec<(bool, [u8; 16])>),
}

impl Entries {
    pub(crate) fn new(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int64 => Some(Self::Int(Vec::new())),
            DataType::Utf8 => Some(Self::Str(Vec::new())),
            DataType::Boolean => Some(Self::Bool(Vec::new())),
            _ => None,
        }
    }

    fn push(&mut self, value: &IrLiteral, uuid: [u8; 16]) {
        match (self, value) {
            (Self::Int(entries), IrLiteral::Int(value)) => entries.push((*value, uuid)),
            (Self::Str(entries), IrLiteral::Str(value)) => entries.push((value.clone(), uuid)),
            (Self::Bool(entries), IrLiteral::Bool(value)) => entries.push((*value, uuid)),
            // NULL, absent and differently typed values equal nothing.
            _ => {}
        }
    }

    pub(crate) fn value_type(&self) -> &'static str {
        match self {
            Self::Int(_) => "int64",
            Self::Str(_) => "utf8",
            Self::Bool(_) => "bool",
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int(entries) => entries.len(),
            Self::Str(entries) => entries.len(),
            Self::Bool(entries) => entries.len(),
        }
    }

    /// Sort by value then UUID, so the bytes do not depend on arrival order.
    pub(crate) fn sort(&mut self) {
        match self {
            Self::Int(entries) => entries.sort_unstable(),
            Self::Str(entries) => entries.sort_unstable(),
            Self::Bool(entries) => entries.sort_unstable(),
        }
    }

    /// Append the non-null rows of an Arrow column of this entries' type.
    /// Returns `false` when the column has a different type.
    pub(crate) fn push_array(&mut self, array: &dyn Array, uuids: &FixedSizeBinaryArray) -> bool {
        let uuid = |row: usize| -> Option<[u8; 16]> { uuids.value(row).try_into().ok() };
        match self {
            Self::Int(entries) => {
                let Some(values) = array.as_any().downcast_ref::<Int64Array>() else {
                    return false;
                };
                for row in 0..values.len() {
                    if let (false, Some(uuid)) = (values.is_null(row), uuid(row)) {
                        entries.push((values.value(row), uuid));
                    }
                }
            }
            Self::Str(entries) => {
                let Some(values) = array.as_any().downcast_ref::<StringArray>() else {
                    return false;
                };
                for row in 0..values.len() {
                    if let (false, Some(uuid)) = (values.is_null(row), uuid(row)) {
                        entries.push((values.value(row).to_owned(), uuid));
                    }
                }
            }
            Self::Bool(entries) => {
                let Some(values) = array.as_any().downcast_ref::<BooleanArray>() else {
                    return false;
                };
                for row in 0..values.len() {
                    if let (false, Some(uuid)) = (values.is_null(row), uuid(row)) {
                        entries.push((values.value(row), uuid));
                    }
                }
            }
        }
        true
    }

    fn shard(&self, range: std::ops::Range<usize>) -> Result<(RecordBatch, Bound, Bound), GfError> {
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(range.len(), 16);
        let arrow_err = |error: arrow::error::ArrowError| validation(error.to_string());
        let (values, first, last): (ArrayRef, Bound, Bound) = match self {
            Self::Int(entries) => {
                let slice = &entries[range];
                for (_, uuid) in slice {
                    uuids.append_value(uuid).map_err(arrow_err)?;
                }
                (
                    Arc::new(Int64Array::from_iter_values(slice.iter().map(|e| e.0))),
                    Bound::Int(slice[0].0),
                    Bound::Int(slice[slice.len() - 1].0),
                )
            }
            Self::Str(entries) => {
                let slice = &entries[range];
                for (_, uuid) in slice {
                    uuids.append_value(uuid).map_err(arrow_err)?;
                }
                (
                    Arc::new(StringArray::from_iter_values(
                        slice.iter().map(|e| e.0.as_str()),
                    )),
                    Bound::Str(slice[0].0.clone()),
                    Bound::Str(slice[slice.len() - 1].0.clone()),
                )
            }
            Self::Bool(entries) => {
                let slice = &entries[range];
                for (_, uuid) in slice {
                    uuids.append_value(uuid).map_err(arrow_err)?;
                }
                (
                    Arc::new(slice.iter().map(|e| Some(e.0)).collect::<BooleanArray>()),
                    Bound::Bool(slice[0].0),
                    Bound::Bool(slice[slice.len() - 1].0),
                )
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", values.data_type().clone(), false),
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![values, Arc::new(uuids.finish())])
            .map_err(arrow_err)?;
        Ok((batch, first, last))
    }
}

fn encode_shard(batch: &RecordBatch) -> Result<Vec<u8>, GfError> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
            .map_err(|e| validation(e.to_string()))?;
        writer.write(batch).map_err(|e| validation(e.to_string()))?;
        writer.finish().map_err(|e| validation(e.to_string()))?;
    }
    Ok(bytes)
}

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, GfError> {
    let body = serde_json::to_vec(manifest).map_err(|e| validation(e.to_string()))?;
    let mut bytes = format!("{:016x}\n", crate::corruption_checksum::checksum(&body)).into_bytes();
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8], path: &Path) -> Result<Manifest, GfError> {
    let corrupt = || validation(format!("manifest {} failed its checksum", path.display()));
    if bytes.len() < 17 || bytes[16] != b'\n' {
        return Err(corrupt());
    }
    let expected = std::str::from_utf8(&bytes[..16])
        .ok()
        .and_then(|text| u64::from_str_radix(text, 16).ok())
        .ok_or_else(corrupt)?;
    let body = &bytes[17..];
    if crate::corruption_checksum::checksum(body) != expected {
        return Err(corrupt());
    }
    let manifest: Manifest = serde_json::from_slice(body).map_err(|_| corrupt())?;
    if manifest.format != FORMAT {
        return Err(validation(format!(
            "manifest {} has unsupported format {:?}",
            path.display(),
            manifest.format
        )));
    }
    Ok(manifest)
}

/// One file of a built index, by its path relative to the graph root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyEqualityIndexFile {
    /// Normalized path below the graph root.
    pub relative_path: String,
    /// Exact bytes.
    pub bytes: Vec<u8>,
}

/// Build the equality index files of one node route: one manifest and its
/// shards for every indexed column the route stores as an integer, string or
/// boolean. A route without such a column yields no files and is answered by
/// the scan. The bytes depend only on the route's content.
///
/// # Errors
/// Propagates the route read or a cancellation.
pub fn property_equality_index_files(
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    cancelled: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<Vec<PropertyEqualityIndexFile>, GfError> {
    let kind = PropertyRouteKind::Node;
    let mut files = Vec::new();
    let Some(schema) = inventory.route_schema(kind, route)? else {
        return Ok(files);
    };
    let Some(source_digest) = inventory.route_source_digest(kind, route) else {
        return Ok(files);
    };
    for column in INDEXED_COLUMNS {
        cancelled()?;
        let Ok(field) = schema.field_with_name(column) else {
            continue;
        };
        let Some(value_type) = value_type_name(field.data_type()) else {
            continue;
        };
        let Some(mut entries) = Entries::new(field.data_type()) else {
            continue;
        };
        let selected = BTreeSet::from([column.to_owned()]);
        let read = RouteRead {
            kind,
            route,
            selected_properties: Some(&selected),
            uuids: None,
            limits: PropertyOverlayLimits::default(),
            collect: false,
        };
        let mut stopped = None;
        inventory.visit_route_streaming(&read, |row| {
            if let Err(error) = cancelled() {
                stopped = Some(error);
                return Ok(false);
            }
            if let Some(value) = row.values.get(column) {
                entries.push(value, row.uuid);
            }
            Ok(true)
        })?;
        if let Some(error) = stopped {
            return Err(error);
        }
        if entries.len() == 0 {
            continue;
        }
        entries.sort();
        index_files(
            &mut files,
            route,
            column,
            value_type,
            &source_digest,
            &entries,
        )?;
    }
    Ok(files)
}

/// What a build wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PropertyEqualityIndexBuild {
    /// Indexes written: one per route and indexed column.
    pub indexes: u64,
    /// Files written, manifests and shards.
    pub files: u64,
    /// Bytes written.
    pub bytes: u64,
}

/// Build the equality index of every node route the inventory holds, into
/// `stage_root/indexes/property/`. The caller owns the stage and publishes it;
/// this never touches a published tree.
///
/// # Errors
/// Propagates the route reads, a cancellation, or a write failure.
pub fn write_property_equality_indexes(
    stage_root: &Path,
    inventory: &AuthenticatedPropertyInventory,
    cancelled: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<PropertyEqualityIndexBuild, GfError> {
    let mut report = PropertyEqualityIndexBuild::default();
    std::fs::create_dir_all(index_dir(stage_root)).map_err(|e| storage("create stage", &e))?;
    for route in inventory.route_names(PropertyRouteKind::Node) {
        for file in property_equality_index_files(inventory, &route, cancelled)? {
            let path = stage_root.join(&file.relative_path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| storage("create directory", &e))?;
            }
            std::fs::write(&path, &file.bytes).map_err(|e| storage("write file", &e))?;
            if Path::new(&file.relative_path)
                .extension()
                .is_some_and(|extension| extension == "eq")
            {
                report.indexes += 1;
            }
            report.files += 1;
            report.bytes += file.bytes.len() as u64;
        }
    }
    Ok(report)
}

pub(crate) fn index_files(
    files: &mut Vec<PropertyEqualityIndexFile>,
    route: &str,
    column: &str,
    value_type: &str,
    source_digest: &str,
    entries: &Entries,
) -> Result<(), GfError> {
    let stem = stem(route, column);
    let total = entries.len();
    let mut shards = Vec::new();
    for (number, start) in (0..total).step_by(SHARD_ENTRIES).enumerate() {
        let end = (start + SHARD_ENTRIES).min(total);
        let (batch, first, last) = entries.shard(start..end)?;
        let bytes = encode_shard(&batch)?;
        let file = format!("{number:08}.eqs");
        shards.push(ShardEntry {
            file: file.clone(),
            rows: (end - start) as u64,
            first,
            last,
            byte_length: bytes.len() as u64,
            xxh64: crate::corruption_checksum::checksum(&bytes),
        });
        files.push(PropertyEqualityIndexFile {
            relative_path: format!("indexes/property/{stem}.shards/{file}"),
            bytes,
        });
    }
    let manifest = Manifest {
        format: FORMAT.to_owned(),
        route: route.to_owned(),
        column: column.to_owned(),
        value_type: value_type.to_owned(),
        source_digest: source_digest.to_owned(),
        entries: total as u64,
        shards,
    };
    files.push(PropertyEqualityIndexFile {
        relative_path: format!("indexes/property/{stem}.eq"),
        bytes: encode_manifest(&manifest)?,
    });
    Ok(())
}

fn compare(bound: &Bound, value: &EqualityValue) -> Option<std::cmp::Ordering> {
    match (bound, value) {
        (Bound::Int(bound), EqualityValue::Int(value)) => Some(bound.cmp(value)),
        (Bound::Str(bound), EqualityValue::Str(value)) => Some(bound.as_str().cmp(value.as_str())),
        (Bound::Bool(bound), EqualityValue::Bool(value)) => Some(bound.cmp(value)),
        _ => None,
    }
}

fn type_name_of(value: &EqualityValue) -> &'static str {
    match value {
        EqualityValue::Int(_) => "int64",
        EqualityValue::Str(_) => "utf8",
        EqualityValue::Bool(_) => "bool",
    }
}

/// The manifest of a current index for this route and column, `None` when no
/// current index answers the equality. A checksum failure is an error.
fn current_manifest(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    equality: &PropertyEquality,
) -> Result<Option<Manifest>, GfError> {
    if !is_indexed_column(&equality.column) {
        return Ok(None);
    }
    let path = manifest_path(project_dir, route, &equality.column);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage("open manifest", &error)),
    };
    crate::graph_admission::admit_file(&file)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| storage("read manifest", &e))?;
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes.len() as u64, 1);
    let manifest = decode_manifest(&bytes, &path)?;
    if manifest.route != route
        || manifest.column != equality.column
        || manifest.value_type != type_name_of(&equality.value)
    {
        return Ok(None);
    }
    let current = inventory.route_source_digest(PropertyRouteKind::Node, route);
    Ok((current.as_deref() == Some(manifest.source_digest.as_str())).then_some(manifest))
}

/// Whether a current index answers this equality on this route.
///
/// # Errors
/// A manifest that fails its checksum or does not decode.
pub(crate) fn is_current(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    equality: &PropertyEquality,
) -> Result<bool, GfError> {
    Ok(current_manifest(project_dir, inventory, route, equality)?.is_some())
}

/// The UUIDs whose newest live snapshot holds the value, from a current index;
/// `None` when no current index answers. Reads the manifest and only the
/// shards whose range holds the value, each checked against the manifest.
///
/// # Errors
/// A manifest or shard that fails its checksum, or is malformed.
pub(crate) fn lookup(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    equality: &PropertyEquality,
) -> Result<Option<BTreeSet<[u8; 16]>>, GfError> {
    let Some(manifest) = current_manifest(project_dir, inventory, route, equality)? else {
        return Ok(None);
    };
    let first = manifest.shards.partition_point(|shard| {
        compare(&shard.last, &equality.value).is_some_and(std::cmp::Ordering::is_lt)
    });
    let dir = shard_dir(project_dir, route, &equality.column);
    let mut found = BTreeSet::new();
    for shard in &manifest.shards[first..] {
        if compare(&shard.first, &equality.value).is_none_or(std::cmp::Ordering::is_gt) {
            break;
        }
        read_shard(&dir, shard, &equality.value, &mut found)?;
    }
    Ok(Some(found))
}

fn read_shard(
    dir: &Path,
    shard: &ShardEntry,
    wanted: &EqualityValue,
    found: &mut BTreeSet<[u8; 16]>,
) -> Result<(), GfError> {
    if shard.file.contains(['/', '\\']) || shard.file.starts_with('.') {
        return Err(validation("manifest names a shard outside its directory"));
    }
    let path = dir.join(&shard.file);
    let mut file = File::open(&path).map_err(|e| storage("open shard", &e))?;
    let mut bytes = Vec::new();
    // One byte past the declared length detects a longer file without reading it all.
    (&mut file)
        .take(shard.byte_length.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| storage("read shard", &e))?;
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes.len() as u64, 1);
    if bytes.len() as u64 != shard.byte_length
        || crate::corruption_checksum::checksum(&bytes) != shard.xxh64
    {
        return Err(validation(format!(
            "shard {} failed its checksum",
            path.display()
        )));
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
        .map_err(|e| validation(e.to_string()))?;
    let mut rows = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|e| validation(e.to_string()))?;
        rows += batch.num_rows() as u64;
        let uuids = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| validation("shard UUID column has the wrong type"))?;
        let values = batch.column(0);
        for row in 0..batch.num_rows() {
            let equal = match wanted {
                EqualityValue::Int(wanted) => values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .is_some_and(|a| a.value(row) == *wanted),
                EqualityValue::Str(wanted) => values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|a| a.value(row) == wanted),
                EqualityValue::Bool(wanted) => values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .is_some_and(|a| a.value(row) == *wanted),
            };
            if equal {
                let uuid: [u8; 16] = uuids
                    .value(row)
                    .try_into()
                    .map_err(|_| validation("shard UUID is not 16 bytes"))?;
                found.insert(uuid);
            }
        }
    }
    if rows != shard.rows {
        return Err(validation("shard row count does not match its manifest"));
    }
    Ok(())
}

/// Collects the `(value, node_uuid)` rows of one node route as its fragments
/// are encoded, so a bulk construction indexes the rows it already holds
/// instead of reading them back. An initial build writes each node once, with
/// no tombstone; a route that breaks either assumption is not indexed and the
/// scan answers it.
pub(crate) struct RouteIndexBuilder {
    columns: Vec<ColumnRows>,
    uuids: Vec<[u8; 16]>,
    unindexable: bool,
}

struct ColumnRows {
    column: &'static str,
    entries: Option<Entries>,
    unusable: bool,
}

impl RouteIndexBuilder {
    pub(crate) fn new() -> Self {
        Self {
            columns: INDEXED_COLUMNS
                .iter()
                .map(|column| ColumnRows {
                    column,
                    entries: None,
                    unusable: false,
                })
                .collect(),
            uuids: Vec::new(),
            unindexable: false,
        }
    }

    /// Observe one encoded property fragment of the route.
    pub(crate) fn observe(&mut self, fragment: &RecordBatch) {
        if self.unindexable {
            return;
        }
        let Some(uuids) = fragment
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        else {
            self.unindexable = true;
            return;
        };
        if let Some(tombstones) =
            fragment.column_by_name(crate::property_overlay::PROPERTY_TOMBSTONE_FIELD)
        {
            let live = tombstones
                .as_any()
                .downcast_ref::<BooleanArray>()
                .is_some_and(|flags| flags.true_count() == 0);
            if !live {
                self.unindexable = true;
                return;
            }
        }
        for row in 0..uuids.len() {
            let Ok(uuid) = uuids.value(row).try_into() else {
                self.unindexable = true;
                return;
            };
            self.uuids.push(uuid);
        }
        for rows in &mut self.columns {
            let Some(array) = fragment.column_by_name(rows.column) else {
                continue;
            };
            if rows.unusable {
                continue;
            }
            if rows.entries.is_none() {
                rows.entries = Entries::new(array.data_type());
            }
            let pushed = rows
                .entries
                .as_mut()
                .is_some_and(|entries| entries.push_array(array.as_ref(), uuids));
            if !pushed {
                rows.unusable = true;
            }
        }
    }

    /// The index files of the route, none when it cannot be indexed.
    ///
    /// # Errors
    /// Propagates an encoding failure.
    pub(crate) fn finish(
        mut self,
        route: &str,
        source_digest: &str,
    ) -> Result<Vec<PropertyEqualityIndexFile>, GfError> {
        let mut files = Vec::new();
        if self.unindexable {
            return Ok(files);
        }
        self.uuids.sort_unstable();
        if self.uuids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Ok(files);
        }
        for rows in self.columns {
            let Some(mut entries) = rows.entries.filter(|_| !rows.unusable) else {
                continue;
            };
            let value_type = entries.value_type();
            if entries.len() == 0 {
                continue;
            }
            entries.sort();
            index_files(
                &mut files,
                route,
                rows.column,
                value_type,
                source_digest,
                &entries,
            )?;
        }
        Ok(files)
    }
}
