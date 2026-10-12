//! Persisted lookup index over edge properties (#1984).
//!
//! A traversal that returns or filters an edge property needs the properties of
//! a few hundred edges out of millions. The property overlay stores them as
//! fragments, each authenticated as a whole object, so before this index every
//! statement opened and checksummed every fragment of the route and decoded its
//! UUID column to find the handful of rows it wanted. The index is the same
//! data cut into small column blobs that are authenticated one at a time.
//!
//! Layout, below `indexes/edge-properties/`, one set per edge property route:
//!
//! - `<stem>.epx`: a manifest. An XXH64 line over the JSON body, then the JSON:
//!   format, route, the digest of the route's source objects, the route's
//!   property columns, the directory of the packs, and for every shard its first
//!   and last edge UUID, its row count, and where each of its blobs is: the
//!   pack, the byte range, and the XXH64 of those bytes.
//! - `<pack directory>/<n>.epk`: a pack, consecutive blobs of at most 4 MiB. A
//!   blob is one column of one shard of at most 4096 rows: the sorted
//!   `edge_uuid` column, or property column `k` exactly as the route stores it,
//!   one value per UUID row, as a Zstd-compressed Arrow IPC stream. A shard
//!   holds only the columns its source fragment stored.
//!
//! The blobs are small so a lookup reads a few kilobytes per hit. They are
//! packed because every durable open links each artifact of the generation into
//! a private workspace: a file per blob would cost an open as many link and stat
//! calls as the index has blobs.
//!
//! A lookup compares each shard's UUID range with the sorted request, reads the
//! UUID blob of the shards whose range holds a requested UUID, and reads the
//! blobs of the demanded properties for the shards that hold a match, each byte
//! range checked against the manifest. Columns the statement does not demand are
//! never read. The bytes of a built index are a pure function of the route's
//! content: they hold no clock, UUID or process identity.
//!
//! An index is current only while its source digest equals the route's. A
//! missing or stale index is not used and is never rebuilt by a read; the
//! property overlay answers instead. A manifest or blob that fails its
//! checksum is refused. Routes that hold a tombstone, or whose fragments
//! disagree on the type of a column, are not indexed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, UInt32Array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::property_equality_index::hex;
use crate::property_overlay::{
    AuthenticatedPropertyInventory, MAX_PROPERTY_OBJECT_BYTES, PROPERTY_TOMBSTONE_FIELD,
    PropertyOverlayLimits, PropertyRouteKind, RouteRead,
};

/// Manifest format identifier.
pub const FORMAT: &str = "graphforge-edge-property-index/1";
/// Rows per shard. Fixed, so a lookup's reads do not depend on the route size.
const SHARD_ROWS: usize = 4096;
/// Bytes of one column of a shard, so a blob stays well inside the pack bound
/// whatever the values are.
const SHARD_BYTES: usize = 1 << 20;
/// Bytes of one pack: the bound on a property object, which a pack is.
const PACK_BYTES: usize = MAX_PROPERTY_OBJECT_BYTES;
/// The edge identity column of every edge property fragment and shard.
const UUID_COLUMN: &str = "edge_uuid";
const MANIFEST_EXTENSION: &str = "epx";
const PACK_EXTENSION: &str = "epk";

/// `<project>/indexes/edge-properties`.
#[must_use]
pub fn index_dir(project_dir: &Path) -> PathBuf {
    project_dir.join("indexes").join("edge-properties")
}

/// The manifest stem of a route: a hash, so no route name reaches a path.
pub(crate) fn stem(route: &str) -> String {
    let digest = Sha256::digest(route.as_bytes());
    hex(&digest[..16])
}

fn manifest_relative_path(route: &str) -> String {
    format!(
        "indexes/edge-properties/{}.{MANIFEST_EXTENSION}",
        stem(route)
    )
}

/// Where one blob is inside the packs of its route, and the XXH64 of its bytes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BlobSeal {
    /// Ordinal of the pack, which names `<ordinal>.epk` below the pack directory.
    pack: u32,
    offset: u64,
    byte_length: u64,
    xxh64: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnEntry {
    /// Index into the manifest's `columns`.
    column: usize,
    #[serde(flatten)]
    blob: BlobSeal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardEntry {
    rows: u64,
    first: String,
    last: String,
    uuids: BlobSeal,
    columns: Vec<ColumnEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    format: String,
    route: String,
    source_digest: String,
    rows: u64,
    pack_dir: String,
    columns: Vec<String>,
    shards: Vec<ShardEntry>,
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(format!("edge property index: {}", message.into()))
}

fn storage(message: &str, error: &std::io::Error) -> GfError {
    GfError::Storage(format!("edge property index: {message}: {error}"))
}

fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    if text.len() != 32 || !text.is_ascii() {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).ok()?;
    }
    Some(bytes)
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

/// Bytes the rows `start..start + length` of `column` occupy.
fn slice_bytes(column: &ArrayRef, start: usize, length: usize) -> usize {
    column
        .slice(start, length)
        .to_data()
        .get_slice_memory_size()
        .unwrap_or(usize::MAX)
}

/// One column as a Zstd-compressed Arrow IPC stream of a single batch.
fn encode_column(name: &str, data_type: &DataType, array: ArrayRef) -> Result<Vec<u8>, GfError> {
    // Field metadata is a hash map: leaving it out keeps the bytes a function
    // of the content. The UUID column is the only non-null one.
    let nullable = name != UUID_COLUMN;
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        data_type.clone(),
        nullable,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])
        .map_err(|e| validation(e.to_string()))?;
    let mut bytes = Vec::new();
    {
        // Zstd, as the CSR shards: the index must not cost more than the
        // compressed fragments it indexes.
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .map_err(|e| validation(e.to_string()))?;
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new_with_options(&mut bytes, &schema, options)
                .map_err(|e| validation(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| validation(e.to_string()))?;
        writer.finish().map_err(|e| validation(e.to_string()))?;
    }
    Ok(bytes)
}

/// One file of a built index, by its path relative to the graph root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePropertyIndexFile {
    /// Normalized path below the graph root.
    pub relative_path: String,
    /// Exact bytes.
    pub bytes: Vec<u8>,
}

/// Accumulates the manifest of one edge property route while its shards are
/// packed, so a bulk construction indexes the fragments as it encodes them
/// without holding the route: at most one pack is held.
///
/// An initial build writes each edge once, with no tombstone, and every
/// fragment is sorted by edge UUID. A route that breaks any of those
/// assumptions, or whose fragments disagree on the type of a column, is not
/// indexed and the property overlay answers it.
pub(crate) struct EdgeRouteIndexBuilder {
    pack_dir: String,
    shards: Vec<ShardEntry>,
    columns: Vec<(String, DataType)>,
    rows: u64,
    /// The pack being filled, which becomes pack number `packs_written`.
    pack: Vec<u8>,
    packs_written: u32,
    poisoned: bool,
}

impl EdgeRouteIndexBuilder {
    /// A builder whose packs are written below `indexes/edge-properties/<pack_dir>/`.
    pub(crate) fn new(pack_dir: String) -> Self {
        Self {
            pack_dir,
            shards: Vec::new(),
            columns: Vec::new(),
            rows: 0,
            pack: Vec::new(),
            packs_written: 0,
            poisoned: false,
        }
    }

    fn pack_relative_path(&self) -> String {
        format!(
            "indexes/edge-properties/{}/{:08}.{PACK_EXTENSION}",
            self.pack_dir, self.packs_written
        )
    }

    /// Hand the pack being filled to `write`, if it holds anything.
    fn flush_pack(
        &mut self,
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.pack.is_empty() {
            return Ok(());
        }
        write(self.pack_relative_path(), std::mem::take(&mut self.pack))?;
        self.packs_written += 1;
        Ok(())
    }

    /// Append one blob to the pack being filled, starting a new pack when it
    /// would pass `PACK_BYTES`. `None` when the blob cannot fit any pack, which
    /// leaves the route to the property overlay.
    fn append(
        &mut self,
        blob: &[u8],
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<Option<BlobSeal>, GfError> {
        if blob.len() > PACK_BYTES {
            self.poisoned = true;
            return Ok(None);
        }
        if self.pack.len() + blob.len() > PACK_BYTES {
            self.flush_pack(write)?;
        }
        let seal = BlobSeal {
            pack: self.packs_written,
            offset: self.pack.len() as u64,
            byte_length: blob.len() as u64,
            xxh64: crate::corruption_checksum::checksum(blob),
        };
        self.pack.extend_from_slice(blob);
        Ok(Some(seal))
    }

    /// Stop indexing this route.
    pub(crate) fn poison(&mut self) {
        self.poisoned = true;
    }

    /// The route-level index of a property column, registering it on first use.
    /// `None` when an earlier fragment stored the column under another type.
    fn column_index(&mut self, name: &str, data_type: &DataType) -> Option<usize> {
        if let Some(index) = self.columns.iter().position(|(known, _)| known == name) {
            return (&self.columns[index].1 == data_type).then_some(index);
        }
        self.columns.push((name.to_owned(), data_type.clone()));
        Some(self.columns.len() - 1)
    }

    /// Cut one encoded fragment, or one batch of merged rows, into shards,
    /// append their blobs to the pack being filled, and hand each pack that
    /// fills to `write` with its path.
    ///
    /// The batch is `edge_uuid`, optionally the tombstone flag, then the
    /// property columns. Rows must be strictly ascending by UUID.
    pub(crate) fn observe(
        &mut self,
        fragment: &RecordBatch,
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.poisoned || fragment.num_rows() == 0 {
            return Ok(());
        }
        let Some(uuid_column) = fragment.column_by_name(UUID_COLUMN) else {
            self.poisoned = true;
            return Ok(());
        };
        let Some(uuids) = uuid_column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .filter(|uuids| uuids.value_length() == 16 && uuids.null_count() == 0)
        else {
            self.poisoned = true;
            return Ok(());
        };
        if let Some(tombstones) = fragment.column_by_name(PROPERTY_TOMBSTONE_FIELD) {
            let live = tombstones
                .as_any()
                .downcast_ref::<BooleanArray>()
                .is_some_and(|flags| flags.true_count() == 0);
            if !live {
                self.poisoned = true;
                return Ok(());
            }
        }
        if (1..uuids.len()).any(|row| uuids.value(row - 1) >= uuids.value(row)) {
            self.poisoned = true;
            return Ok(());
        }
        let mut stored: Vec<(usize, &str, &ArrayRef)> = Vec::new();
        let fragment_schema = fragment.schema();
        for (field, column) in fragment_schema.fields().iter().zip(fragment.columns()) {
            let name = field.name();
            // A column the route no longer stores a value for is Null-typed and
            // reads as absent either way.
            if name == UUID_COLUMN
                || name == PROPERTY_TOMBSTONE_FIELD
                || column.data_type() == &DataType::Null
            {
                continue;
            }
            let Some(index) = self.column_index(name, column.data_type()) else {
                self.poisoned = true;
                return Ok(());
            };
            stored.push((index, name.as_str(), column));
        }
        let mut start = 0;
        while start < fragment.num_rows() {
            // A shard holds up to SHARD_ROWS rows, fewer when a column's slice
            // would pass SHARD_BYTES. A single value past the bound cannot be
            // sharded, and the route is left to the property overlay.
            let mut length = SHARD_ROWS.min(fragment.num_rows() - start);
            while stored
                .iter()
                .any(|(_, _, column)| slice_bytes(column, start, length) > SHARD_BYTES)
            {
                if length == 1 {
                    self.poisoned = true;
                    return Ok(());
                }
                length = (length / 2).max(1);
            }
            let uuid_bytes = encode_column(
                UUID_COLUMN,
                &DataType::FixedSizeBinary(16),
                uuid_column.slice(start, length),
            )?;
            let Some(uuid_seal) = self.append(&uuid_bytes, write)? else {
                return Ok(());
            };
            let mut columns = Vec::with_capacity(stored.len());
            for (index, name, column) in &stored {
                let bytes = encode_column(name, column.data_type(), column.slice(start, length))?;
                let Some(blob) = self.append(&bytes, write)? else {
                    return Ok(());
                };
                columns.push(ColumnEntry {
                    column: *index,
                    blob,
                });
            }
            let first: [u8; 16] = uuids.value(start).try_into().expect("16-byte UUID");
            let last: [u8; 16] = uuids
                .value(start + length - 1)
                .try_into()
                .expect("16-byte UUID");
            self.shards.push(ShardEntry {
                rows: length as u64,
                first: hex(&first),
                last: hex(&last),
                uuids: uuid_seal,
                columns,
            });
            self.rows += length as u64;
            start += length;
        }
        Ok(())
    }

    /// The files that complete the index of the route: the last pack and the
    /// manifest. None when the route cannot be indexed.
    ///
    /// # Errors
    /// Propagates an encoding failure.
    pub(crate) fn finish(
        mut self,
        route: &str,
        source_digest: &str,
    ) -> Result<Vec<EdgePropertyIndexFile>, GfError> {
        if self.poisoned || self.shards.is_empty() || self.columns.is_empty() {
            return Ok(Vec::new());
        }
        let mut files = Vec::with_capacity(2);
        if !self.pack.is_empty() {
            files.push(EdgePropertyIndexFile {
                relative_path: self.pack_relative_path(),
                bytes: std::mem::take(&mut self.pack),
            });
        }
        let manifest = Manifest {
            format: FORMAT.to_owned(),
            route: route.to_owned(),
            source_digest: source_digest.to_owned(),
            rows: self.rows,
            pack_dir: self.pack_dir,
            columns: self.columns.into_iter().map(|(name, _)| name).collect(),
            shards: self.shards,
        };
        files.push(EdgePropertyIndexFile {
            relative_path: manifest_relative_path(route),
            bytes: encode_manifest(&manifest)?,
        });
        Ok(files)
    }
}

/// What a build wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EdgePropertyIndexBuild {
    /// Indexes written: one per edge property route.
    pub indexes: u64,
    /// Files written, manifests and packs.
    pub files: u64,
    /// Bytes written.
    pub bytes: u64,
}

fn write_stage_file(stage_root: &Path, relative: &str, bytes: &[u8]) -> Result<(), GfError> {
    let path = stage_root.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| storage("create directory", &e))?;
    }
    std::fs::write(&path, bytes).map_err(|e| storage("write file", &e))
}

/// Build the index of every edge property route the inventory holds, into
/// `stage_root/indexes/edge-properties/`. The caller owns the stage and
/// publishes it; this never touches a published tree.
///
/// The rows are the route's newest-wins state in UUID order, so rebuilding an
/// unchanged generation writes the same bytes.
///
/// # Errors
/// Propagates the route reads, a cancellation, or a write failure.
pub fn write_edge_property_indexes(
    stage_root: &Path,
    inventory: &AuthenticatedPropertyInventory,
    cancelled: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<EdgePropertyIndexBuild, GfError> {
    let kind = PropertyRouteKind::Edge;
    let mut report = EdgePropertyIndexBuild::default();
    std::fs::create_dir_all(index_dir(stage_root)).map_err(|e| storage("create stage", &e))?;
    for route in inventory.route_names(kind) {
        cancelled()?;
        let Some(schema) = inventory.route_schema(kind, &route)? else {
            continue;
        };
        let Some(digest) = inventory.route_source_digest(kind, &route) else {
            continue;
        };
        let fields = schema
            .fields()
            .iter()
            .filter(|field| {
                field.name() != PROPERTY_TOMBSTONE_FIELD
                    && (field.name() == UUID_COLUMN || field.data_type() != &DataType::Null)
            })
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        if fields.len() < 2 || !fields.iter().any(|field| field.name() == UUID_COLUMN) {
            continue;
        }
        let projected = Schema::new(fields);
        let mut builder = EdgeRouteIndexBuilder::new(format!("{}.packs", stem(&route)));
        let mut rows: BTreeMap<[u8; 16], crate::PropertySnapshotRow> = BTreeMap::new();
        let mut files = 0_u64;
        let mut bytes = 0_u64;
        let mut previous: Option<[u8; 16]> = None;
        let mut stopped = None;
        let mut write = |relative: String, content: Vec<u8>| -> Result<(), GfError> {
            write_stage_file(stage_root, &relative, &content)?;
            files += 1;
            bytes += content.len() as u64;
            Ok(())
        };
        let read = RouteRead {
            kind,
            route: &route,
            selected_properties: None,
            uuids: None,
            limits: PropertyOverlayLimits::default(),
            collect: false,
        };
        inventory.visit_route_streaming(&read, |row| {
            if let Err(error) = cancelled() {
                stopped = Some(error);
                return Ok(false);
            }
            if row.tombstone || previous.is_some_and(|last| last >= row.uuid) {
                builder.poison();
                return Ok(false);
            }
            previous = Some(row.uuid);
            rows.insert(row.uuid, row);
            if rows.len() >= SHARD_ROWS {
                let batch = crate::writer::edge_property_snapshots_batch(&projected, &rows)?;
                rows.clear();
                builder.observe(&batch, &mut write)?;
            }
            Ok(true)
        })?;
        if let Some(error) = stopped {
            return Err(error);
        }
        if !rows.is_empty() {
            let batch = crate::writer::edge_property_snapshots_batch(&projected, &rows)?;
            builder.observe(&batch, &mut write)?;
        }
        let completed = builder.finish(&route, &digest)?;
        if !completed.is_empty() {
            for file in completed {
                write(file.relative_path, file.bytes)?;
            }
            report.indexes += 1;
        }
        report.files += files;
        report.bytes += bytes;
    }
    Ok(report)
}

#[cfg(test)]
mod tests;

/// A decoded manifest with its shard ranges parsed.
struct CurrentManifest {
    manifest: Manifest,
    ranges: Vec<([u8; 16], [u8; 16])>,
}

/// The manifest of a current index for this route, `None` when no current
/// index exists. A checksum failure is an error.
fn current_manifest(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
) -> Result<Option<CurrentManifest>, GfError> {
    let path = project_dir.join(manifest_relative_path(route));
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
    if manifest.route != route {
        return Err(validation(format!(
            "manifest {} describes route {:?}, not {route:?}",
            path.display(),
            manifest.route
        )));
    }
    let current = inventory.route_source_digest(PropertyRouteKind::Edge, route);
    if current.as_deref() != Some(manifest.source_digest.as_str()) {
        return Ok(None);
    }
    if manifest.pack_dir.contains(['/', '\\']) || manifest.pack_dir.starts_with('.') {
        return Err(validation("manifest names a pack directory outside its own"));
    }
    let ranges = manifest
        .shards
        .iter()
        .map(|shard| {
            Some((parse_uuid(&shard.first)?, parse_uuid(&shard.last)?))
                .filter(|(first, last)| first <= last)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| validation("manifest holds a malformed shard range"))?;
    Ok(Some(CurrentManifest { manifest, ranges }))
}

/// Whether a current index describes this route.
///
/// # Errors
/// A manifest that fails its checksum or does not decode.
pub fn is_current(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
) -> Result<bool, GfError> {
    Ok(current_manifest(project_dir, inventory, route)?.is_some())
}

/// The outcome of looking edges up in the index of one route.
#[derive(Debug)]
pub enum EdgePropertyLookup {
    /// The route holds no property fragment: no edge has a property in it.
    Absent,
    /// No current index describes the route; the property overlay answers.
    NotIndexed,
    /// The properties of the requested edges the route holds.
    Indexed(IndexedEdgeProperties),
}

/// The rows of one route that belong to the requested edges.
#[derive(Debug)]
pub struct IndexedEdgeProperties {
    /// UUIDs of the requested edges that have a row in the route, ascending.
    pub uuids: Vec<[u8; 16]>,
    /// For each requested property that a shard holding a requested edge
    /// stores, one value per UUID (`NULL` where the edge has none). A property
    /// no such shard stores is absent: every requested edge reads `NULL`.
    pub columns: Vec<(String, ArrayRef)>,
}

/// A shard that holds at least one requested edge, with the requested rows of
/// each demanded column it stores.
struct HitShard {
    /// The requested edges this shard holds, ascending.
    uuids: Vec<[u8; 16]>,
    columns: BTreeMap<usize, ArrayRef>,
}

/// Look the properties of `targets` up in the index of `route`. Reads the
/// manifest, the UUID blob of the shards whose UUID range holds a target, and
/// the demanded column blobs of the shards that hold a match, each byte range
/// checked against the manifest.
///
/// # Errors
/// A manifest or blob that fails its checksum or is malformed, or an edge that
/// two shards both hold.
pub fn lookup_edge_properties(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
    properties: &[String],
) -> Result<EdgePropertyLookup, GfError> {
    if !inventory.has_property_route(PropertyRouteKind::Edge, route) {
        return Ok(EdgePropertyLookup::Absent);
    }
    let Some(current) = current_manifest(project_dir, inventory, route)? else {
        return Ok(EdgePropertyLookup::NotIndexed);
    };
    let demanded: BTreeMap<usize, &String> = properties
        .iter()
        .filter_map(|property| {
            current
                .manifest
                .columns
                .iter()
                .position(|column| column == property)
                .map(|index| (index, property))
        })
        .collect();
    let sorted: Vec<[u8; 16]> = targets.iter().copied().collect();
    let mut packs = PackHandles::new(index_dir(project_dir).join(&current.manifest.pack_dir));
    let mut hits: Vec<HitShard> = Vec::new();
    let mut located: Vec<([u8; 16], usize, usize)> = Vec::new();
    for (shard, (first, last)) in current.manifest.shards.iter().zip(&current.ranges) {
        let begin = sorted.partition_point(|target| target < first);
        let end = sorted.partition_point(|target| target <= last);
        if begin == end {
            continue;
        }
        if let Some(hit) = read_hit_shard(&mut packs, shard, &sorted[begin..end], &demanded)? {
            located.extend(
                hit.uuids
                    .iter()
                    .enumerate()
                    .map(|(position, uuid)| (*uuid, hits.len(), position)),
            );
            hits.push(hit);
        }
    }
    located.sort_unstable_by_key(|entry| entry.0);
    if located.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let indices = located
        .iter()
        .map(|(_, slot, position)| (*slot, *position))
        .collect::<Vec<_>>();
    Ok(EdgePropertyLookup::Indexed(IndexedEdgeProperties {
        uuids: located.iter().map(|(uuid, _, _)| *uuid).collect(),
        columns: gather_columns(&hits, &indices, &demanded)?,
    }))
}

/// Read the UUID blob of one shard whose range holds a requested edge, and, if
/// the shard holds any of `targets`, the demanded column blobs. `None` when the
/// shard holds none of them.
fn read_hit_shard(
    packs: &mut PackHandles,
    shard: &ShardEntry,
    targets: &[[u8; 16]],
    demanded: &BTreeMap<usize, &String>,
) -> Result<Option<HitShard>, GfError> {
    let uuids = packs.read_column(shard.uuids, shard.rows)?;
    let wrong_type = || validation("shard UUID column has the wrong type");
    if uuids.data_type() != &DataType::FixedSizeBinary(16) || uuids.null_count() != 0 {
        return Err(wrong_type());
    }
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(wrong_type)?;
    if (1..uuids.len()).any(|row| uuids.value(row - 1) >= uuids.value(row)) {
        return Err(validation("shard UUIDs are not strictly ascending"));
    }
    let mut rows = Vec::new();
    let mut found = Vec::new();
    for target in targets {
        if let Some(row) = find_uuid(uuids, target) {
            rows.push(row);
            found.push(*target);
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    let indices = rows
        .iter()
        .map(|row| u32::try_from(*row))
        .collect::<Result<Vec<_>, _>>()
        .map(UInt32Array::from)
        .map_err(|_| validation("shard row index exceeds u32"))?;
    let mut columns = BTreeMap::new();
    for entry in &shard.columns {
        if !demanded.contains_key(&entry.column) {
            continue;
        }
        let array = packs.read_column(entry.blob, shard.rows)?;
        let taken = arrow::compute::take(array.as_ref(), &indices, None)
            .map_err(|e| validation(e.to_string()))?;
        columns.insert(entry.column, taken);
    }
    Ok(Some(HitShard {
        uuids: found,
        columns,
    }))
}

/// One array per demanded column some hit shard stores, with a value per
/// located edge in `indices` order (`(hit shard, position within it)`).
fn gather_columns(
    hits: &[HitShard],
    indices: &[(usize, usize)],
    demanded: &BTreeMap<usize, &String>,
) -> Result<Vec<(String, ArrayRef)>, GfError> {
    let mut columns = Vec::new();
    for (index, property) in demanded {
        // The shards that store the column fix its type; the others read NULL.
        let Some(data_type) = hits.iter().find_map(|hit| {
            hit.columns
                .get(index)
                .map(|array| array.data_type().clone())
        }) else {
            continue;
        };
        let sources: Vec<ArrayRef> = hits
            .iter()
            .map(|hit| {
                hit.columns
                    .get(index)
                    .map_or_else(|| new_null_array(&data_type, hit.uuids.len()), Arc::clone)
            })
            .collect();
        let references = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let gathered = arrow::compute::interleave(&references, indices)
            .map_err(|e| validation(e.to_string()))?;
        columns.push(((*property).clone(), gathered));
    }
    Ok(columns)
}

fn find_uuid(uuids: &FixedSizeBinaryArray, target: &[u8; 16]) -> Option<usize> {
    let (mut low, mut high) = (0, uuids.len());
    while low < high {
        let middle = low + (high - low) / 2;
        match uuids.value(middle).cmp(target.as_slice()) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Some(middle),
        }
    }
    None
}

/// The packs of one route, opened when a lookup first reads from them and kept
/// open for the rest of the lookup.
struct PackHandles {
    directory: PathBuf,
    open: BTreeMap<u32, (PathBuf, File)>,
}

impl PackHandles {
    fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            open: BTreeMap::new(),
        }
    }

    /// The bytes `seal` names, checked against its checksum.
    fn read_blob(&mut self, seal: BlobSeal) -> Result<Vec<u8>, GfError> {
        if let std::collections::btree_map::Entry::Vacant(slot) = self.open.entry(seal.pack) {
            let path = self
                .directory
                .join(format!("{:08}.{PACK_EXTENSION}", seal.pack));
            let file = File::open(&path).map_err(|e| storage("open pack", &e))?;
            slot.insert((path, file));
        }
        let (path, file) = self.open.get_mut(&seal.pack).expect("pack was just opened");
        let corrupt = || validation(format!("pack {} failed its checksum", path.display()));
        if seal.byte_length > PACK_BYTES as u64 {
            return Err(corrupt());
        }
        file.seek(SeekFrom::Start(seal.offset))
            .map_err(|e| storage("seek pack", &e))?;
        let mut bytes = Vec::new();
        file.take(seal.byte_length)
            .read_to_end(&mut bytes)
            .map_err(|e| storage("read pack", &e))?;
        crate::lifecycle_io::record_read(
            crate::StorageIoPhase::ReadPathScan,
            bytes.len() as u64,
            1,
        );
        if bytes.len() as u64 != seal.byte_length
            || crate::corruption_checksum::checksum(&bytes) != seal.xxh64
        {
            return Err(corrupt());
        }
        Ok(bytes)
    }

    /// Read one blob, check it against its seal and decode its single column.
    fn read_column(&mut self, seal: BlobSeal, rows: u64) -> Result<ArrayRef, GfError> {
        let bytes = self.read_blob(seal)?;
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
                .map_err(|e| validation(e.to_string()))?;
        let batch = reader
            .next()
            .ok_or_else(|| validation("blob holds no batch"))?
            .map_err(|e| validation(e.to_string()))?;
        if reader.next().is_some() || batch.num_columns() != 1 || batch.num_rows() as u64 != rows {
            return Err(validation("blob shape does not match its manifest"));
        }
        Ok(Arc::clone(batch.column(0)))
    }
}
