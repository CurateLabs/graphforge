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
//!   format, route, the digest of the route's source objects, the digest of the
//!   route's canonical edge topology when ids were published, the route's
//!   property columns, the directory of the packs, and for every shard its first
//!   and last edge UUID, its row count, and where each of its blobs is: the
//!   pack, the byte range, and the XXH64 of those bytes.
//! - `<pack directory>/<n>.epk`: a pack, consecutive blobs of at most 4 MiB. A
//!   blob is one column of one shard of at most 4096 rows: the sorted
//!   `edge_uuid` column, the `edge_id` column (the surrogate id of each row's
//!   topology edge, in the same row order), or property column `k` exactly as
//!   the route stores it, one value per UUID row, as a Zstd-compressed Arrow IPC
//!   stream. A shard holds only the columns its source fragment stored.
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
//! A traversal knows an edge by its surrogate id, not its UUID, so a read that
//! holds ids addresses the index by them ([`lookup_edge_properties_by_id`]): it
//! compares each shard's id range with the sorted request and reads the id blob
//! of the shards that can hold a requested id, and then the property blobs of the
//! shards that hold a match. Neither the 16-byte `edge_uuid` column of the
//! topology nor the UUID blobs of the index are read. A shard is cut by UUID, so
//! its id range is only tight when the ids follow the UUID order, as bulk
//! construction assigns them; on any other topology many shards overlap a
//! request and the by-id lookup reads more id blobs, still bounded by the
//! request's own spread.
//!
//! An index is current only while its source digest equals the route's. Its ids
//! are addressable only while the topology digest recorded beside them equals
//! the route's canonical edge topology digest as well, because an id is a
//! position in those files; an index without ids, or whose ids no longer match,
//! still answers UUID lookups ([`lookup_edge_properties`]). A missing or stale
//! index is not used and is never rebuilt by a read; the property overlay
//! answers instead. A manifest or blob that fails its checksum is refused, and
//! so is a manifest the reader cannot decode, such as one written before the
//! manifest named its page size (`page_rows`): the index is derived data, a read
//! never repairs it, and the refusal names `rebuild_property_indexes`.
//! Routes that hold a tombstone, or whose fragments disagree on the type of a
//! column, are not indexed. Ids are all-or-nothing per route: when any row
//! cannot be joined to a topology row, or the topology's UUID pages overlap too
//! widely to join cheaply, the route keeps its UUID-only index.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::property_equality_index::hex;
use crate::property_overlay::{
    AuthenticatedPropertyInventory, MAX_PROPERTY_OBJECT_BYTES, PROPERTY_TOMBSTONE_FIELD,
    PropertyOverlayLimits, PropertyRouteKind, RouteRead,
};
use crate::topology_identity::EdgeIdResolver;

mod pages;
use pages::{Keys, PAGE_ROWS, PackHandles, encode_paged};

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
/// The surrogate id of an edge: its position in the CSR and in the canonical
/// edge topology. Published per shard beside the UUIDs.
const ID_COLUMN: &str = "edge_id";
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
    /// The surrogate `edge_id` of every row, in the shard's UUID order, and the
    /// smallest and largest of them. Absent from an index built before ids
    /// were published, and from a route whose topology could not be joined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ids: Option<BlobSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id_min: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id_max: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    format: String,
    route: String,
    source_digest: String,
    /// The digest of the route's canonical edge topology files, which the
    /// shards' ids were joined from. Present exactly when every shard carries
    /// ids; the ids are addressable only while it equals the route's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    topology_digest: Option<String>,
    rows: u64,
    /// Rows per page of every blob, so a lookup decodes the pages that hold
    /// its rows. Required: a manifest written before paging has no such field,
    /// does not decode, and is refused until the index is rebuilt.
    page_rows: u32,
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

/// What a refused manifest tells the operator to do. The index is derived data:
/// a reader never repairs it, and an explicit rebuild replaces it.
const REBUILD_HINT: &str = "run rebuild_property_indexes to replace the index";

/// Decode and authenticate a manifest. One the reader cannot use is refused,
/// never read around: damage, a format it does not know, and an index built
/// before the manifest named `page_rows` all fail here, and all are repaired by
/// rebuilding the derived index.
fn decode_manifest(bytes: &[u8], path: &Path) -> Result<Manifest, GfError> {
    let corrupt = || {
        validation(format!(
            "manifest {} failed its checksum; {REBUILD_HINT}",
            path.display()
        ))
    };
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
    let manifest: Manifest = serde_json::from_slice(body).map_err(|error| {
        validation(format!(
            "manifest {} does not decode ({error}); {REBUILD_HINT}",
            path.display()
        ))
    })?;
    if manifest.format != FORMAT {
        return Err(validation(format!(
            "manifest {} has unsupported format {:?}; {REBUILD_HINT}",
            path.display(),
            manifest.format
        )));
    }
    if manifest.page_rows == 0 || manifest.page_rows as usize > SHARD_ROWS {
        return Err(validation(format!(
            "manifest {} has an invalid page size; {REBUILD_HINT}",
            path.display()
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
    /// Whether every shard so far carries its ids. One batch without them
    /// withdraws the ids of the whole route.
    ids_complete: bool,
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
            ids_complete: true,
        }
    }

    /// Whether the next batch is worth resolving ids for: false once the
    /// route has lost its ids, or is no longer indexed at all.
    pub(crate) fn wants_ids(&self) -> bool {
        self.ids_complete && !self.poisoned
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

    /// Encode one column of a shard as a paged blob and append it to the pack
    /// being filled. `None` when the blob cannot fit any pack.
    fn append_column(
        &mut self,
        name: &str,
        data_type: &DataType,
        array: &ArrayRef,
        keys: Keys,
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<Option<BlobSeal>, GfError> {
        let (blob, xxh64) = encode_paged(name, data_type, array, keys, PAGE_ROWS)?;
        self.append(&blob, xxh64, write)
    }

    /// Append one blob, sealed by `xxh64`, to the pack being filled, starting a
    /// new pack when it would pass `PACK_BYTES`. `None` when the blob cannot fit
    /// any pack, which leaves the route to the property overlay.
    fn append(
        &mut self,
        blob: &[u8],
        xxh64: u64,
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
            xxh64,
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
    ///
    /// `ids` is the surrogate `edge_id` of each row, in row order. A batch
    /// without them (`None`) leaves the whole route without addressable ids:
    /// ids that cover only some shards would make a lookup by id miss rows.
    ///
    /// # Errors
    /// Propagates an encoding or pack write failure, and reports `ids` whose
    /// length differs from the batch.
    pub(crate) fn observe(
        &mut self,
        fragment: &RecordBatch,
        ids: Option<&[u64]>,
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.poisoned || fragment.num_rows() == 0 {
            return Ok(());
        }
        if ids.is_some_and(|ids| ids.len() != fragment.num_rows()) {
            return Err(validation("edge ids do not match the rows of the batch"));
        }
        let ids = ids.filter(|_| self.ids_complete);
        self.ids_complete &= ids.is_some();
        let Some((uuid_column, uuids)) = indexable_uuids(fragment) else {
            self.poisoned = true;
            return Ok(());
        };
        let Some(stored) = self.stored_columns(fragment) else {
            self.poisoned = true;
            return Ok(());
        };
        let mut start = 0;
        while start < fragment.num_rows() {
            // A single value past the shard bound cannot be sharded, and the
            // route is left to the property overlay.
            let Some(length) = shard_length(&stored, start, fragment.num_rows() - start) else {
                self.poisoned = true;
                return Ok(());
            };
            let Some(uuid_seal) = self.append_column(
                UUID_COLUMN,
                &DataType::FixedSizeBinary(16),
                &uuid_column.slice(start, length),
                Keys::Uuid,
                write,
            )?
            else {
                return Ok(());
            };
            let id_blob = match ids {
                Some(ids) => {
                    let Some(blob) = self.append_ids(&ids[start..start + length], write)? else {
                        return Ok(());
                    };
                    Some(blob)
                }
                None => None,
            };
            let mut columns = Vec::with_capacity(stored.len());
            for (index, name, column) in &stored {
                let Some(blob) = self.append_column(
                    name,
                    column.data_type(),
                    &column.slice(start, length),
                    Keys::Values,
                    write,
                )?
                else {
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
                ids: id_blob.map(|(seal, _, _)| seal),
                id_min: id_blob.map(|(_, min, _)| min),
                id_max: id_blob.map(|(_, _, max)| max),
            });
            self.rows += length as u64;
            start += length;
        }
        Ok(())
    }

    /// The columns of `fragment` the index stores, each with its route-level
    /// index. `None` when an earlier fragment stored one under another type.
    fn stored_columns<'a>(&mut self, fragment: &'a RecordBatch) -> Option<Vec<StoredColumn<'a>>> {
        let mut stored = Vec::new();
        for (field, column) in fragment
            .schema_ref()
            .fields()
            .iter()
            .zip(fragment.columns())
        {
            let name = field.name();
            // A column the route no longer stores a value for is Null-typed and
            // reads as absent either way.
            if name == UUID_COLUMN
                || name == PROPERTY_TOMBSTONE_FIELD
                || column.data_type() == &DataType::Null
            {
                continue;
            }
            let index = self.column_index(name, column.data_type())?;
            stored.push((index, name.as_str(), column));
        }
        Some(stored)
    }

    /// Append the id blob of one shard window, with the least and greatest id it
    /// holds. `None` when the blob cannot fit any pack.
    fn append_ids(
        &mut self,
        window: &[u64],
        write: &mut dyn FnMut(String, Vec<u8>) -> Result<(), GfError>,
    ) -> Result<Option<IdBlob>, GfError> {
        let ids: ArrayRef = Arc::new(UInt64Array::from(window.to_vec()));
        let Some(seal) = self.append_column(ID_COLUMN, &DataType::UInt64, &ids, Keys::Id, write)?
        else {
            return Ok(None);
        };
        let min = window.iter().copied().min().expect("non-empty shard");
        let max = window.iter().copied().max().expect("non-empty shard");
        Ok(Some((seal, min, max)))
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
        topology_digest: Option<&str>,
    ) -> Result<Vec<EdgePropertyIndexFile>, GfError> {
        if self.poisoned || self.shards.is_empty() || self.columns.is_empty() {
            return Ok(Vec::new());
        }
        // Ids are published all or none, and only against the topology they
        // were joined from. Without that digest a reader cannot tell the ids
        // are current, so they are withdrawn.
        let topology_digest = topology_digest
            .filter(|_| self.ids_complete)
            .map(str::to_owned);
        if topology_digest.is_none() {
            for shard in &mut self.shards {
                shard.ids = None;
                shard.id_min = None;
                shard.id_max = None;
            }
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
            topology_digest,
            rows: self.rows,
            page_rows: u32::try_from(PAGE_ROWS).expect("page rows fit u32"),
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

/// The resolver that joins `route`'s edges to their topology ids, with the
/// digest of the topology it reads. `None` when the route has no topology the
/// inventory can name, or when the topology's UUID pages overlap so much that
/// a join would decode far more than the rows it resolves; the index is then
/// keyed by UUID only.
fn id_resolver(
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
) -> Result<Option<(EdgeIdResolver, String)>, GfError> {
    let Some(digest) = inventory.edge_topology_source_digest(route) else {
        return Ok(None);
    };
    let files = inventory.edge_topology_files(route);
    let resolver = EdgeIdResolver::open(&files)?;
    Ok(resolver.is_point_readable().then_some((resolver, digest)))
}

/// The topology `edge_id` of every row of the window, in UUID order, or `None`
/// when the route is not joined, has already lost its ids, or any row has no
/// topology row (the route's ids are then withdrawn, not guessed).
fn resolve_window(
    join: Option<&(EdgeIdResolver, String)>,
    builder: &EdgeRouteIndexBuilder,
    rows: &BTreeMap<[u8; 16], crate::PropertySnapshotRow>,
) -> Result<Option<Vec<u64>>, GfError> {
    let Some((resolver, _)) = join.filter(|_| builder.wants_ids()) else {
        return Ok(None);
    };
    let candidates: Vec<[u8; 16]> = rows.keys().copied().collect();
    Ok(resolver
        .resolve(&candidates)?
        .into_iter()
        .collect::<Option<Vec<u64>>>())
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
        let id_join = id_resolver(inventory, &route)?;
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
                let ids = resolve_window(id_join.as_ref(), &builder, &rows)?;
                rows.clear();
                builder.observe(&batch, ids.as_deref(), &mut write)?;
            }
            Ok(true)
        })?;
        if let Some(error) = stopped {
            return Err(error);
        }
        if !rows.is_empty() {
            let batch = crate::writer::edge_property_snapshots_batch(&projected, &rows)?;
            let ids = resolve_window(id_join.as_ref(), &builder, &rows)?;
            builder.observe(&batch, ids.as_deref(), &mut write)?;
        }
        let topology_digest = id_join.as_ref().map(|(_, digest)| digest.as_str());
        let completed = builder.finish(&route, &digest, topology_digest)?;
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
/// index exists (no manifest, or one for other source objects). A manifest that
/// fails its checksum, does not decode or has an unknown format is an error.
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
        return Err(validation(
            "manifest names a pack directory outside its own",
        ));
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
/// each demanded column it stores. `K` is how the request names an edge: its
/// UUID, or its surrogate id.
struct HitShard<K> {
    /// The requested edges this shard holds, ascending.
    keys: Vec<K>,
    columns: BTreeMap<usize, ArrayRef>,
}

/// Look the properties of `targets` up in the index of `route`. Reads the
/// manifest, the UUID blob of the shards whose UUID range holds a target, and
/// the demanded column blobs of the shards that hold a match, each byte range
/// checked against the manifest.
///
/// # Errors
/// A manifest that does not decode (including one written before paging, which
/// the error tells the caller to replace with `rebuild_property_indexes`), a
/// manifest or blob that fails its checksum or is malformed, or an edge that two
/// shards both hold.
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
    let demanded = demanded_columns(&current.manifest, properties);
    let sorted: Vec<[u8; 16]> = targets.iter().copied().collect();
    let mut packs = PackHandles::new(
        index_dir(project_dir).join(&current.manifest.pack_dir),
        current.manifest.page_rows,
    );
    let mut hits: Vec<HitShard<[u8; 16]>> = Vec::new();
    for (shard, (first, last)) in current.manifest.shards.iter().zip(&current.ranges) {
        let begin = sorted.partition_point(|target| target < first);
        let end = sorted.partition_point(|target| target <= last);
        if begin == end {
            continue;
        }
        if let Some(hit) = read_hit_shard(&mut packs, shard, &sorted[begin..end], &demanded)? {
            hits.push(hit);
        }
    }
    let (uuids, columns) = assemble(&hits, &demanded)?;
    Ok(EdgePropertyLookup::Indexed(IndexedEdgeProperties {
        uuids,
        columns,
    }))
}

/// The outcome of looking edges up by surrogate id.
#[derive(Debug)]
pub enum EdgePropertyIdLookup {
    /// The route holds no property fragment: no edge has a property in it.
    Absent,
    /// No current index addresses this route by edge id: none exists, it is
    /// stale, it predates ids, or its ids were not joined from the route's
    /// current topology. The UUID-keyed lookup answers.
    NotAddressable,
    /// The properties of the requested edges the route holds.
    Indexed(IndexedEdgeIds),
}

/// The rows of one route that belong to the requested edge ids.
#[derive(Debug)]
pub struct IndexedEdgeIds {
    /// Ids of the requested edges that have a row in the route, ascending.
    pub ids: Vec<u64>,
    /// As [`IndexedEdgeProperties::columns`], one value per id.
    pub columns: Vec<(String, ArrayRef)>,
}

/// Look the properties of the edges with surrogate ids `targets` up in the
/// index of `route`, without reading any `edge_uuid`. Reads the manifest, the id
/// blob of the shards whose id range holds a target, and the demanded column
/// blobs of the shards that hold a match, each byte range checked against the
/// manifest. [`EdgePropertyIdLookup::NotAddressable`] when the route's index
/// cannot answer by id.
///
/// # Errors
/// As [`lookup_edge_properties`].
pub fn lookup_edge_properties_by_id(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
    targets: &BTreeSet<u64>,
    properties: &[String],
) -> Result<EdgePropertyIdLookup, GfError> {
    if !inventory.has_property_route(PropertyRouteKind::Edge, route) {
        return Ok(EdgePropertyIdLookup::Absent);
    }
    let Some(current) = current_manifest(project_dir, inventory, route)? else {
        return Ok(EdgePropertyIdLookup::NotAddressable);
    };
    if !ids_are_current(&current.manifest, inventory, route) {
        return Ok(EdgePropertyIdLookup::NotAddressable);
    }
    let demanded = demanded_columns(&current.manifest, properties);
    let sorted: Vec<u64> = targets.iter().copied().collect();
    let mut packs = PackHandles::new(
        index_dir(project_dir).join(&current.manifest.pack_dir),
        current.manifest.page_rows,
    );
    let mut hits: Vec<HitShard<u64>> = Vec::new();
    for shard in &current.manifest.shards {
        let (Some(seal), Some(min), Some(max)) = (shard.ids, shard.id_min, shard.id_max) else {
            return Err(validation("manifest ids are incomplete"));
        };
        let begin = sorted.partition_point(|target| *target < min);
        let end = sorted.partition_point(|target| *target <= max);
        if begin == end {
            continue;
        }
        if let Some(hit) = read_hit_shard_by_id(
            &mut packs,
            shard,
            seal,
            (min, max),
            &sorted[begin..end],
            &demanded,
        )? {
            hits.push(hit);
        }
    }
    let (ids, columns) = assemble(&hits, &demanded)?;
    Ok(EdgePropertyIdLookup::Indexed(IndexedEdgeIds {
        ids,
        columns,
    }))
}

/// Whether the ids of a current manifest were joined from the topology the
/// inventory describes now: a topology digest equal to the route's, and an id
/// blob on every shard.
fn ids_are_current(
    manifest: &Manifest,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
) -> bool {
    let current = inventory.edge_topology_source_digest(route);
    current.is_some()
        && current == manifest.topology_digest
        && manifest
            .shards
            .iter()
            .all(|shard| shard.ids.is_some() && shard.id_min.is_some() && shard.id_max.is_some())
}

/// Whether the index of `route` is current and answers lookups by edge id.
///
/// # Errors
/// A manifest that fails its checksum or does not decode.
pub fn is_id_addressable(
    project_dir: &Path,
    inventory: &AuthenticatedPropertyInventory,
    route: &str,
) -> Result<bool, GfError> {
    Ok(current_manifest(project_dir, inventory, route)?
        .is_some_and(|current| ids_are_current(&current.manifest, inventory, route)))
}

/// The demanded properties the route stores, by column index.
fn demanded_columns<'a>(
    manifest: &Manifest,
    properties: &'a [String],
) -> BTreeMap<usize, &'a String> {
    properties
        .iter()
        .filter_map(|property| {
            manifest
                .columns
                .iter()
                .position(|column| column == property)
                .map(|index| (index, property))
        })
        .collect()
}

/// A stored property column of a fragment: its route-level index, name and array.
type StoredColumn<'a> = (usize, &'a str, &'a ArrayRef);

/// A shard's id blob and the least and greatest edge id it holds.
type IdBlob = (BlobSeal, u64, u64);

/// The property columns gathered for a lookup, by name, in demanded order.
type GatheredColumns = Vec<(String, ArrayRef)>;

/// The UUID column of a fragment when it can be indexed: 16-byte, non-null,
/// strictly ascending, and with no tombstoned row.
fn indexable_uuids(fragment: &RecordBatch) -> Option<(&ArrayRef, &FixedSizeBinaryArray)> {
    let column = fragment.column_by_name(UUID_COLUMN)?;
    let uuids = column
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|uuids| uuids.value_length() == 16 && uuids.null_count() == 0)?;
    if let Some(tombstones) = fragment.column_by_name(PROPERTY_TOMBSTONE_FIELD) {
        let live = tombstones
            .as_any()
            .downcast_ref::<BooleanArray>()
            .is_some_and(|flags| flags.true_count() == 0);
        if !live {
            return None;
        }
    }
    (1..uuids.len())
        .all(|row| uuids.value(row - 1) < uuids.value(row))
        .then_some((column, uuids))
}

/// The rows of the shard that starts at `start`: up to `SHARD_ROWS`, fewer when
/// a column's slice would pass `SHARD_BYTES`. `None` when one row alone does.
fn shard_length(stored: &[StoredColumn<'_>], start: usize, remaining: usize) -> Option<usize> {
    let mut length = SHARD_ROWS.min(remaining);
    while stored
        .iter()
        .any(|(_, _, column)| slice_bytes(column, start, length) > SHARD_BYTES)
    {
        if length == 1 {
            return None;
        }
        length = (length / 2).max(1);
    }
    Some(length)
}

/// The requested edges the hit shards hold, ascending, and one array per
/// demanded column in that order. Two shards holding one edge is corrupt.
fn assemble<K: Ord + Copy>(
    hits: &[HitShard<K>],
    demanded: &BTreeMap<usize, &String>,
) -> Result<(Vec<K>, GatheredColumns), GfError> {
    let mut located: Vec<(K, usize, usize)> = hits
        .iter()
        .enumerate()
        .flat_map(|(slot, hit)| {
            hit.keys
                .iter()
                .enumerate()
                .map(move |(position, key)| (*key, slot, position))
        })
        .collect();
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
    Ok((
        located.iter().map(|(key, _, _)| *key).collect(),
        gather_columns(hits, &indices, demanded)?,
    ))
}

/// Read the UUID blob of one shard whose range holds a requested edge, and, if
/// the shard holds any of `targets`, the demanded column blobs. `None` when the
/// shard holds none of them.
fn read_hit_shard(
    packs: &mut PackHandles,
    shard: &ShardEntry,
    targets: &[[u8; 16]],
    demanded: &BTreeMap<usize, &String>,
) -> Result<Option<HitShard<[u8; 16]>>, GfError> {
    let blob = packs.open_blob(shard.uuids, shard.rows, Keys::Uuid)?;
    let located = blob.uuid_pages(targets);
    if located.is_empty() {
        return Ok(None);
    }
    let wanted: Vec<usize> = located.iter().map(|(page, _)| *page).collect();
    let decoded = packs.read_pages(&blob, &wanted)?;
    let mut rows = Vec::new();
    let mut found = Vec::new();
    for ((page, range), array) in located.iter().zip(&decoded) {
        let uuids = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| validation("shard UUID column has the wrong type"))?;
        for target in &targets[range.clone()] {
            if let Some(row) = find_uuid(uuids, target) {
                rows.push(page * blob.page_rows() + row);
                found.push(*target);
            }
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    let columns = take_columns(packs, shard, &rows, demanded)?;
    Ok(Some(HitShard {
        keys: found,
        columns,
    }))
}

/// As [`read_hit_shard`], locating `targets` by the shard's id blob instead of
/// its UUID blob. The shard's ids are in UUID order, not id order, so the blob
/// is scanned once and each id is checked against the sorted `targets`.
fn read_hit_shard_by_id(
    packs: &mut PackHandles,
    shard: &ShardEntry,
    seal: BlobSeal,
    (min, max): (u64, u64),
    targets: &[u64],
    demanded: &BTreeMap<usize, &String>,
) -> Result<Option<HitShard<u64>>, GfError> {
    let blob = packs.open_blob(seal, shard.rows, Keys::Id)?;
    blob.check_id_range(min, max)?;
    let wanted = blob.id_pages(targets);
    let decoded = packs.read_pages(&blob, &wanted)?;
    let mut located: Vec<(u64, usize)> = Vec::new();
    for (page, array) in wanted.iter().zip(&decoded) {
        let ids = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| validation("shard id column has the wrong type"))?;
        for (offset, id) in ids.values().iter().enumerate() {
            if *id < min || *id > max {
                return Err(validation("shard id lies outside its manifest range"));
            }
            if targets.binary_search(id).is_ok() {
                located.push((*id, page * blob.page_rows() + offset));
            }
        }
    }
    if located.is_empty() {
        return Ok(None);
    }
    located.sort_unstable();
    if located.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let rows: Vec<usize> = located.iter().map(|(_, row)| *row).collect();
    let columns = take_columns(packs, shard, &rows, demanded)?;
    Ok(Some(HitShard {
        keys: located.into_iter().map(|(id, _)| id).collect(),
        columns,
    }))
}

/// The demanded column blobs of `shard`, reduced to `rows`.
fn take_columns(
    packs: &mut PackHandles,
    shard: &ShardEntry,
    rows: &[usize],
    demanded: &BTreeMap<usize, &String>,
) -> Result<BTreeMap<usize, ArrayRef>, GfError> {
    let mut columns = BTreeMap::new();
    for entry in &shard.columns {
        if !demanded.contains_key(&entry.column) {
            continue;
        }
        let blob = packs.open_blob(entry.blob, shard.rows, Keys::Values)?;
        let (wanted, positions) = blob.row_pages(rows);
        let decoded = packs.read_pages(&blob, &wanted)?;
        let sources = decoded.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let taken = arrow::compute::interleave(&sources, &positions)
            .map_err(|e| validation(e.to_string()))?;
        columns.insert(entry.column, taken);
    }
    Ok(columns)
}

/// One array per demanded column some hit shard stores, with a value per
/// located edge in `indices` order (`(hit shard, position within it)`).
fn gather_columns<K>(
    hits: &[HitShard<K>],
    indices: &[(usize, usize)],
    demanded: &BTreeMap<usize, &String>,
) -> Result<GatheredColumns, GfError> {
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
                    .map_or_else(|| new_null_array(&data_type, hit.keys.len()), Arc::clone)
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
