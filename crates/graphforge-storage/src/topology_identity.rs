//! Identity checks answered from the published topology Parquet (#1902).
//!
//! The canonical node and edge files already carry every live UUID, so an
//! append asks them directly instead of consulting a derived index. A probe
//! prunes row groups by their `node_uuid`/`edge_uuid` min/max statistics, then
//! prunes the pages of the surviving row groups by the Parquet column index,
//! decodes only the selected pages of the UUID (and, for nodes, `node_id`)
//! column, and binary-searches the sorted candidates against each decoded
//! value. A file without a page index is pruned by row group alone.
//!
//! Deleted entities are not rows any more, yet their UUIDs are never reusable.
//! `topology/deleted_identities.parquet` records them: a sorted, unique
//! `FixedSizeBinary(16)` column carried forward by each generation that
//! deletes, so its size follows deletions and not graph size.

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_core::GfError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::file::metadata::PageIndexPolicy;
use rayon::prelude::*;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

/// Relative path of the record of deleted entity UUIDs.
pub const DELETED_IDENTITIES_PATH: &str = "topology/deleted_identities.parquet";

const DELETED_IDENTITIES_COLUMN: &str = "uuid";
const READ_BATCH_ROWS: usize = 16 * 1024;
/// Footers retained process-wide. A footer is a few hundred bytes per row
/// group, so this bounds the cache to tens of megabytes.
const FOOTER_CACHE_ENTRIES: usize = 1 << 17;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Selects the canonical identity domain to probe.
pub enum UuidIndexKind {
    /// Canonical node UUIDs.
    Node,
    /// Canonical edge UUIDs.
    Edge,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Aggregate-only probe evidence; it never contains graph identities.
pub struct UuidProbeMetrics {
    /// Total requested identities, including duplicates.
    pub requested: u64,
    /// Distinct requested identities.
    pub unique_requested: u64,
    /// Distinct identities found.
    pub found: u64,
    /// Row groups decoded. Each is one positioned read of the identity
    /// column (and the surrogate column for node lookups).
    pub file_seeks: u64,
    /// Row groups decoded after min/max pruning.
    pub identity_blocks_read: u64,
    /// Compressed bytes of the identity (and surrogate) column pages decoded,
    /// with the dictionary pages of the chunks they belong to.
    pub identity_bytes_read: u64,
    /// Identity-column pages in every row group of every fragment considered,
    /// whether or not pruning kept them. A row group of a file without a page
    /// index counts as one page.
    pub pages_considered: u64,
    /// Identity-column pages decoded. `pages_considered - pages_read` is what
    /// the row-group and page-index pruning avoided.
    pub pages_read: u64,
    /// Always zero: node surrogates are read with their UUIDs.
    pub surrogate_blocks_read: u64,
    /// Always zero: node surrogates are read with their UUIDs.
    pub surrogate_bytes_read: u64,
    /// Fragments whose row groups were considered for pruning.
    pub runs_considered: u64,
    /// Always zero: a probe never seeks per requested record.
    pub per_record_seeks: u64,
}

impl UuidProbeMetrics {
    pub(crate) fn absorb(&mut self, other: &Self) {
        self.requested += other.requested;
        self.unique_requested += other.unique_requested;
        self.found += other.found;
        self.file_seeks += other.file_seeks;
        self.identity_blocks_read += other.identity_blocks_read;
        self.identity_bytes_read += other.identity_bytes_read;
        self.pages_considered += other.pages_considered;
        self.pages_read += other.pages_read;
        self.runs_considered += other.runs_considered;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FileKey {
    device: u64,
    inode: u64,
    length: u64,
    modified_nanos: i128,
}

/// One data page of the identity column, from the column and offset indexes.
struct PageBounds {
    /// First row of the page, relative to its row group.
    first_row: usize,
    /// Inclusive bounds of the page's non-null UUIDs, when recorded.
    bounds: Option<([u8; 16], [u8; 16])>,
}

/// The pages of one decoded column chunk, for byte accounting.
struct ChunkPages {
    /// Bytes of the dictionary page that precedes the data pages, if any.
    dictionary_bytes: u64,
    /// `(first row, compressed page bytes including the header)` per page.
    pages: Vec<(usize, u64)>,
}

struct RowGroupBounds {
    /// Inclusive bounds of the non-null UUIDs, when the writer recorded them.
    bounds: Option<([u8; 16], [u8; 16])>,
    rows: usize,
    /// Pages of the identity column. Empty when the file has no usable page
    /// index, in which case the group is one page.
    pages: Vec<PageBounds>,
    /// Page layout of each decoded column chunk (identity, then surrogate).
    /// Empty when the file has no usable offset index.
    chunks: Vec<ChunkPages>,
    /// Compressed bytes of the columns a probe decodes, whole.
    probe_bytes: u64,
}

/// What one row group contributes to a probe.
struct GroupPlan {
    fragment: usize,
    group: usize,
    /// Rows to decode, or `None` for the whole group.
    rows: Option<Vec<Range<usize>>>,
    pages: u64,
    bytes: u64,
}

/// The part of a [`GroupPlan`] a fragment decides by itself.
struct GroupSelection {
    rows: Option<Vec<Range<usize>>>,
    pages: u64,
    bytes: u64,
}

#[derive(Clone)]
struct Fragment {
    key: FileKey,
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    uuid_leaf: usize,
    id_leaf: Option<usize>,
    groups: Arc<Vec<RowGroupBounds>>,
    rows: u64,
}

fn footer_cache() -> &'static Mutex<HashMap<FileKey, Arc<Fragment>>> {
    static CACHE: OnceLock<Mutex<HashMap<FileKey, Arc<Fragment>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn file_key(path: &Path) -> Result<FileKey, GfError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| GfError::Storage(format!("topology fragment: {error}")))?;
    Ok(FileKey {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified_nanos: i128::from(metadata.mtime()) * 1_000_000_000
            + i128::from(metadata.mtime_nsec()),
    })
}

fn storage(message: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("topology identity: {message}"))
}

fn leaf_index(descriptor: &parquet::schema::types::SchemaDescriptor, name: &str) -> Option<usize> {
    descriptor
        .columns()
        .iter()
        .position(|column| column.path().string() == name)
}

/// The identity column's pages in row group `group`, when the column and offset
/// indexes agree on how many there are and every page starts where the
/// previous one ended.
fn page_bounds(
    metadata: &parquet::file::metadata::ParquetMetaData,
    group: usize,
    leaf: usize,
    rows: usize,
) -> Option<Vec<PageBounds>> {
    use parquet::file::page_index::column_index::ColumnIndexMetaData;

    let ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) =
        metadata.column_index()?.get(group)?.get(leaf)?
    else {
        return None;
    };
    let locations = metadata
        .offset_index()?
        .get(group)?
        .get(leaf)?
        .page_locations();
    if locations.is_empty() || usize::try_from(index.num_pages()).ok()? != locations.len() {
        return None;
    }
    let mut pages = Vec::with_capacity(locations.len());
    for (page, location) in locations.iter().enumerate() {
        let first_row = usize::try_from(location.first_row_index).ok()?;
        if first_row >= rows
            || pages
                .last()
                .is_some_and(|previous: &PageBounds| previous.first_row >= first_row)
        {
            return None;
        }
        let bounds = match (index.min_value(page), index.max_value(page)) {
            (Some(min), Some(max)) if !index.is_null_page(page) => {
                Some((min.try_into().ok()?, max.try_into().ok()?))
            }
            _ => None,
        };
        pages.push(PageBounds { first_row, bounds });
    }
    (pages.first()?.first_row == 0).then_some(pages)
}

/// Page layout of each decoded column chunk of row group `group`.
fn chunk_pages(
    metadata: &parquet::file::metadata::ParquetMetaData,
    group: usize,
    leaves: &[usize],
    rows: usize,
) -> Option<Vec<ChunkPages>> {
    let offsets = metadata.offset_index()?.get(group)?;
    let row_group = metadata.row_group(group);
    leaves
        .iter()
        .map(|leaf| {
            let locations = offsets.get(*leaf)?.page_locations();
            let first = locations.first()?;
            let dictionary_bytes = row_group
                .column(*leaf)
                .dictionary_page_offset()
                .map_or(Some(0), |dictionary| {
                    u64::try_from(first.offset - dictionary).ok()
                })?;
            let mut pages = Vec::with_capacity(locations.len());
            for location in locations {
                let first_row = usize::try_from(location.first_row_index).ok()?;
                if first_row >= rows {
                    return None;
                }
                pages.push((
                    first_row,
                    u64::try_from(location.compressed_page_size).ok()?,
                ));
            }
            Some(ChunkPages {
                dictionary_bytes,
                pages,
            })
        })
        .collect()
}

fn load_fragment(
    path: &Path,
    uuid_column: &str,
    id_column: Option<&str>,
) -> Result<Arc<Fragment>, GfError> {
    let key = file_key(path)?;
    if let Some(found) = footer_cache()
        .lock()
        .map_err(|_| storage("footer cache poisoned"))?
        .get(&key)
        && found.uuid_column_is(uuid_column)
    {
        // One object is hard-linked under many workspace paths, and a footer
        // read through one describes them all. The reader must open the path
        // it was asked for: the first workspace may be gone.
        return Ok(if found.path == path {
            Arc::clone(found)
        } else {
            Arc::new(Fragment {
                path: path.to_path_buf(),
                ..Fragment::clone(found)
            })
        });
    }
    // Optional: a file without a page index is still probed, by row group.
    let builder = crate::catalog::admitted_parquet_with_options(
        path,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )
    .map_err(|error| storage(format!("{}: {error}", path.display())))?;
    let parquet_metadata = Arc::clone(builder.metadata());
    let descriptor = parquet_metadata.file_metadata().schema_descr();
    let uuid_leaf = leaf_index(descriptor, uuid_column)
        .ok_or_else(|| storage(format!("{} lacks {uuid_column}", path.display())))?;
    let id_leaf = match id_column {
        Some(name) => Some(
            leaf_index(descriptor, name)
                .ok_or_else(|| storage(format!("{} lacks {name}", path.display())))?,
        ),
        None => None,
    };
    let mut groups = Vec::with_capacity(parquet_metadata.num_row_groups());
    let mut rows = 0_u64;
    let mut leaves = vec![uuid_leaf];
    leaves.extend(id_leaf);
    for (index, group) in parquet_metadata.row_groups().iter().enumerate() {
        let group_rows = usize::try_from(group.num_rows()).unwrap_or(0);
        rows += group_rows as u64;
        let column = group.column(uuid_leaf);
        let bounds = column.statistics().and_then(|statistics| {
            let min: [u8; 16] = statistics.min_bytes_opt()?.try_into().ok()?;
            let max: [u8; 16] = statistics.max_bytes_opt()?.try_into().ok()?;
            Some((min, max))
        });
        let probe_bytes = leaves
            .iter()
            .map(|leaf| u64::try_from(group.column(*leaf).compressed_size()).unwrap_or(0))
            .sum();
        groups.push(RowGroupBounds {
            bounds,
            rows: group_rows,
            pages: page_bounds(&parquet_metadata, index, uuid_leaf, group_rows).unwrap_or_default(),
            chunks: chunk_pages(&parquet_metadata, index, &leaves, group_rows).unwrap_or_default(),
            probe_bytes,
        });
    }
    let metadata = ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
        .map_err(storage)?;
    let fragment = Arc::new(Fragment {
        key,
        path: path.to_path_buf(),
        metadata,
        uuid_leaf,
        id_leaf,
        groups: Arc::new(groups),
        rows,
    });
    let mut cache = footer_cache()
        .lock()
        .map_err(|_| storage("footer cache poisoned"))?;
    if cache.len() >= FOOTER_CACHE_ENTRIES {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&fragment));
    Ok(fragment)
}

impl Fragment {
    fn uuid_column_is(&self, name: &str) -> bool {
        self.metadata
            .parquet_schema()
            .columns()
            .get(self.uuid_leaf)
            .is_some_and(|column| column.path().string() == name)
    }

    /// Whether any of the sorted `candidates` lies within `bounds`.
    fn range_may_hold(bounds: Option<([u8; 16], [u8; 16])>, candidates: &[[u8; 16]]) -> bool {
        let Some((min, max)) = bounds else {
            return true;
        };
        let first = candidates.partition_point(|candidate| *candidate < min);
        first < candidates.len() && candidates[first] <= max
    }

    /// Identity-column pages of row group `group`; one when there is no index.
    fn page_count(&self, group: usize) -> u64 {
        (self.groups[group].pages.len() as u64).max(1)
    }

    /// The rows of row group `group` that may hold any of the sorted
    /// `candidates`: `None` when none can, otherwise the page-aligned row
    /// ranges, the pages they cover and the compressed bytes decoding them
    /// reads.
    fn plan_group(&self, group: usize, candidates: &[[u8; 16]]) -> Option<GroupSelection> {
        let bounds = &self.groups[group];
        if !Self::range_may_hold(bounds.bounds, candidates) {
            return None;
        }
        if bounds.pages.is_empty() {
            return Some(GroupSelection {
                rows: None,
                pages: 1,
                bytes: bounds.probe_bytes,
            });
        }
        let mut ranges: Vec<Range<usize>> = Vec::new();
        let mut selected = 0_u64;
        for (index, page) in bounds.pages.iter().enumerate() {
            if !Self::range_may_hold(page.bounds, candidates) {
                continue;
            }
            selected += 1;
            let end = bounds
                .pages
                .get(index + 1)
                .map_or(bounds.rows, |next| next.first_row);
            match ranges.last_mut() {
                Some(last) if last.end == page.first_row => last.end = end,
                _ => ranges.push(page.first_row..end),
            }
        }
        if ranges.is_empty() {
            return None;
        }
        let bytes = if bounds.chunks.is_empty() {
            bounds.probe_bytes
        } else {
            bounds
                .chunks
                .iter()
                .map(|chunk| chunk.bytes_for(&ranges, bounds.rows))
                .sum()
        };
        let whole = ranges.len() == 1 && ranges[0] == (0..bounds.rows);
        Some(GroupSelection {
            rows: if whole { None } else { Some(ranges) },
            pages: selected,
            bytes,
        })
    }

    /// Decode `rows` of row group `group` (all of it when `None`), reporting
    /// `(candidate index, surrogate)` for every row whose UUID is a candidate.
    fn scan_group(
        &self,
        group: usize,
        rows: Option<&[Range<usize>]>,
        candidates: &[[u8; 16]],
        found: &mut Vec<(usize, u64)>,
    ) -> Result<(), GfError> {
        let file = std::fs::File::open(&self.path)
            .map_err(|error| storage(format!("{}: {error}", self.path.display())))?;
        let file = crate::catalog::admitted_path_file(file)
            .map_err(|error| storage(format!("{}: {error}", self.path.display())))?;
        let mut leaves = vec![self.uuid_leaf];
        leaves.extend(self.id_leaf);
        let mask = ProjectionMask::leaves(self.metadata.parquet_schema(), leaves);
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.metadata.clone())
                .with_row_groups(vec![group])
                .with_projection(mask)
                .with_batch_size(READ_BATCH_ROWS);
        if let Some(rows) = rows {
            builder = builder.with_row_selection(RowSelection::from_consecutive_ranges(
                rows.iter().cloned(),
                self.groups[group].rows,
            ));
        }
        let reader = builder
            .build()
            .map_err(|error| storage(format!("{}: {error}", self.path.display())))?;
        for batch in reader {
            let batch =
                batch.map_err(|error| storage(format!("{}: {error}", self.path.display())))?;
            collect_matches(&batch, self.id_leaf.is_some(), candidates, found)?;
        }
        Ok(())
    }
}

impl ChunkPages {
    /// Compressed bytes of the pages overlapping the sorted, disjoint `rows`,
    /// and the chunk's dictionary page when any page is read.
    fn bytes_for(&self, rows: &[Range<usize>], group_rows: usize) -> u64 {
        let mut total = 0_u64;
        let mut read_any = false;
        for (index, (first_row, bytes)) in self.pages.iter().enumerate() {
            let end = self
                .pages
                .get(index + 1)
                .map_or(group_rows, |(next, _)| *next);
            let start = rows.partition_point(|range| range.end <= *first_row);
            if rows.get(start).is_some_and(|range| range.start < end) {
                total += bytes;
                read_any = true;
            }
        }
        total + if read_any { self.dictionary_bytes } else { 0 }
    }
}

fn collect_matches(
    batch: &RecordBatch,
    with_ids: bool,
    candidates: &[[u8; 16]],
    found: &mut Vec<(usize, u64)>,
) -> Result<(), GfError> {
    let uuids = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|array| array.value_length() == 16)
        .ok_or_else(|| storage("identity column is not FixedSizeBinary(16)"))?;
    let ids = if with_ids {
        Some(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| storage("surrogate column is not UInt64"))?,
        )
    } else {
        None
    };
    for row in 0..uuids.len() {
        if uuids.is_null(row) {
            continue;
        }
        let value: &[u8; 16] = uuids
            .value(row)
            .try_into()
            .map_err(|_| storage("identity value is not 16 bytes"))?;
        if let Ok(index) = candidates.binary_search(value) {
            let surrogate = match ids {
                Some(ids) if ids.is_null(row) => return Err(storage("null surrogate")),
                Some(ids) => ids.value(row),
                None => 0,
            };
            found.push((index, surrogate));
        }
    }
    Ok(())
}

/// Identity lookups over one topology generation's published Parquet.
#[derive(Clone)]
pub struct TopologyIdentityProbe {
    nodes: Vec<Arc<Fragment>>,
    edges: Vec<Arc<Fragment>>,
    deleted: Arc<Vec<[u8; 16]>>,
    generation: u64,
    /// CAS objects a compact parent's fragments live in; they stay readable
    /// for as long as the probe does.
    _leases: Arc<Vec<crate::graph_object_store::AuthenticatedGraphObject>>,
    authenticated_bytes: u64,
    authenticated_objects: u64,
}

impl std::fmt::Debug for TopologyIdentityProbe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TopologyIdentityProbe")
            .field("generation", &self.generation)
            .field("node_fragments", &self.nodes.len())
            .field("edge_fragments", &self.edges.len())
            .field("deleted", &self.deleted.len())
            .finish_non_exhaustive()
    }
}

impl TopologyIdentityProbe {
    /// Open the probe over `files`, reading (or reusing) every fragment footer
    /// and the record of deleted identities under `root`.
    ///
    /// # Errors
    /// Returns an error when a fragment fails Parquet admission or lacks its
    /// identity column, or when the deleted-identity record is malformed.
    pub fn open(
        root: &Path,
        files: &crate::TopologyFiles,
        topology_generation: u64,
    ) -> Result<Self, GfError> {
        let nodes = files
            .node_fragments()
            .par_iter()
            .map(|(path, _)| load_fragment(path, "node_uuid", Some("node_id")))
            .collect::<Result<Vec<_>, _>>()?;
        let edges = files
            .edge_fragments()
            .par_iter()
            .map(|(_, path, _)| load_fragment(path, "edge_uuid", None))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            nodes,
            edges,
            deleted: Arc::new(read_deleted_identities(root)?),
            generation: topology_generation,
            _leases: Arc::new(Vec::new()),
            authenticated_bytes: 0,
            authenticated_objects: 0,
        })
    }

    /// Open the probe over a compact parent generation whose fragments are
    /// content-addressed objects. Every object is authenticated against its
    /// inventory checksum and retained for the life of the probe.
    ///
    /// # Errors
    /// Returns an error when an object fails authentication or admission.
    pub(crate) fn open_compact(
        container_root: &Path,
        inventory: &crate::GraphFilesInventory,
        topology_generation: u64,
    ) -> Result<Self, GfError> {
        let mut node_entries = Vec::new();
        let mut edge_entries = Vec::new();
        let mut deleted_entry = None;
        for entry in &inventory.files {
            if crate::topology_files::is_node(&entry.relative_path) {
                node_entries.push(entry);
            } else if entry.relative_path.starts_with("topology/edges/")
                && entry.relative_path.ends_with(".parquet")
            {
                edge_entries.push(entry);
            } else if entry.relative_path == DELETED_IDENTITIES_PATH {
                deleted_entry = Some(entry);
            }
        }
        let authenticated_bytes = node_entries
            .iter()
            .chain(edge_entries.iter())
            .copied()
            .chain(deleted_entry)
            .map(|entry| entry.byte_length)
            .sum::<u64>();
        let authenticated_objects =
            (node_entries.len() + edge_entries.len() + usize::from(deleted_entry.is_some())) as u64;
        let leases = node_entries
            .iter()
            .chain(edge_entries.iter())
            .copied()
            .chain(deleted_entry)
            .collect::<Vec<_>>()
            .par_iter()
            .map(|entry| {
                crate::graph_object_store::open_graph_object_with_checksum(container_root, entry)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let path_of = |entry: &crate::GraphFileEntry| {
            crate::graph_object_path(container_root, &entry.content_sha256)
        };
        let nodes = node_entries
            .par_iter()
            .map(|entry| load_fragment(&path_of(entry)?, "node_uuid", Some("node_id")))
            .collect::<Result<Vec<_>, _>>()?;
        let edges = edge_entries
            .par_iter()
            .map(|entry| load_fragment(&path_of(entry)?, "edge_uuid", None))
            .collect::<Result<Vec<_>, _>>()?;
        let deleted = match deleted_entry {
            Some(entry) => {
                parse_deleted_identities(std::fs::File::open(path_of(entry)?).map_err(storage)?)?
            }
            None => Vec::new(),
        };
        Ok(Self {
            nodes,
            edges,
            deleted: Arc::new(deleted),
            generation: topology_generation,
            _leases: Arc::new(leases),
            authenticated_bytes,
            authenticated_objects,
        })
    }

    /// Bytes of content-addressed fragment payload this probe authenticated
    /// against inventory checksums when it was opened (zero for a directory
    /// open, which reads footers only).
    #[must_use]
    pub fn authenticated_bytes(&self) -> u64 {
        self.authenticated_bytes
    }

    /// Content-addressed objects authenticated at open.
    #[must_use]
    pub fn authenticated_objects(&self) -> u64 {
        self.authenticated_objects
    }

    /// Confirm every fragment is still the file this probe read.
    ///
    /// # Errors
    /// Returns an error when a fragment was replaced or removed.
    pub fn revalidate(&self) -> Result<(), GfError> {
        for fragment in self.nodes.iter().chain(self.edges.iter()) {
            if file_key(&fragment.path)? != fragment.key {
                return Err(storage("topology fragment changed under a retained probe"));
            }
        }
        Ok(())
    }

    /// Open the probe over the topology files found under `root`: a
    /// materialized graph root with no declared inventory.
    ///
    /// # Errors
    /// Returns an error when discovery or any fragment fails.
    pub fn open_dir(root: &Path) -> Result<Self, GfError> {
        let files = crate::TopologyFiles::discover_legacy(root)?;
        Self::open(root, &files, crate::read_topology_generation(root)?)
    }

    /// Topology generation this probe was opened at.
    #[must_use]
    pub fn topology_generation(&self) -> u64 {
        self.generation
    }

    /// Live entities of `kind`, from the row counts the footers record.
    #[must_use]
    pub fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.nodes.iter().map(|fragment| fragment.rows).sum(),
            UuidIndexKind::Edge => self.edges.iter().map(|fragment| fragment.rows).sum(),
        }
    }

    fn scan(
        fragments: &[Arc<Fragment>],
        requested: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        let mut sorted: Vec<[u8; 16]> = requested.iter().map(|uuid| *uuid.as_bytes()).collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            unique_requested: sorted.len() as u64,
            ..UuidProbeMetrics::default()
        };
        let mut tasks = Vec::new();
        for (fragment_index, fragment) in fragments.iter().enumerate() {
            let mut considered = false;
            for group in 0..fragment.groups.len() {
                considered = true;
                metrics.pages_considered += fragment.page_count(group);
                if let Some(selection) = fragment.plan_group(group, &sorted) {
                    tasks.push(GroupPlan {
                        fragment: fragment_index,
                        group,
                        rows: selection.rows,
                        pages: selection.pages,
                        bytes: selection.bytes,
                    });
                }
            }
            metrics.runs_considered += u64::from(considered);
        }
        // Rayon workers carry the caller's requested I/O measurement, so the
        // bytes a probe reads are attributed to the operation that asked.
        let capture = crate::lifecycle_io::CaptureContext::current();
        let hits = tasks
            .par_iter()
            .map(|plan| {
                let _scope = capture.attach();
                let mut found = Vec::new();
                fragments[plan.fragment].scan_group(
                    plan.group,
                    plan.rows.as_deref(),
                    &sorted,
                    &mut found,
                )?;
                Ok((plan.pages, plan.bytes, found))
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        let mut resolved: Vec<Option<u64>> = vec![None; sorted.len()];
        for (pages, bytes, found) in hits {
            metrics.file_seeks += 1;
            metrics.identity_blocks_read += 1;
            metrics.pages_read += pages;
            metrics.identity_bytes_read += bytes;
            for (index, surrogate) in found {
                resolved[index] = Some(surrogate);
            }
        }
        metrics.found = resolved.iter().filter(|value| value.is_some()).count() as u64;
        // Restore caller order: each request maps to its distinct candidate.
        let out = requested
            .iter()
            .map(|uuid| {
                sorted
                    .binary_search(uuid.as_bytes())
                    .ok()
                    .and_then(|index| resolved[index])
            })
            .collect();
        Ok((out, metrics))
    }

    /// Whether each of `uuids` is a live entity of `kind`, in caller order.
    ///
    /// # Errors
    /// Returns an error when a fragment cannot be decoded.
    pub fn probe(
        &mut self,
        kind: UuidIndexKind,
        uuids: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let fragments = match kind {
            UuidIndexKind::Node => &self.nodes,
            UuidIndexKind::Edge => &self.edges,
        };
        let (values, metrics) = Self::scan(fragments, uuids)?;
        Ok((
            values.into_iter().map(|value| value.is_some()).collect(),
            metrics,
        ))
    }

    /// Resolve live node UUIDs to their `node_id`, in caller order.
    ///
    /// # Errors
    /// Returns an error when a fragment cannot be decoded.
    pub fn lookup_node_surrogates(
        &mut self,
        uuids: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        Self::scan(&self.nodes, uuids)
    }

    /// Whether each of `uuids` names an entity that was deleted: a UUID that
    /// can never be reused.
    #[must_use]
    pub fn deleted(&self, uuids: &[Uuid]) -> Vec<bool> {
        uuids
            .iter()
            .map(|uuid| self.deleted.binary_search(uuid.as_bytes()).is_ok())
            .collect()
    }

    /// Whether each of `uuids` is already spent: a live node, a live edge or a
    /// deleted entity. Node and edge UUIDs share one namespace.
    ///
    /// # Errors
    /// Returns an error when a fragment cannot be decoded.
    pub fn taken(&mut self, uuids: &[Uuid]) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let (nodes, node_metrics) = Self::scan(&self.nodes, uuids)?;
        let (edges, edge_metrics) = Self::scan(&self.edges, uuids)?;
        let deleted = self.deleted(uuids);
        let mut metrics = node_metrics;
        metrics.absorb(&edge_metrics);
        metrics.requested = uuids.len() as u64;
        metrics.unique_requested = edge_metrics.unique_requested;
        let taken = nodes
            .iter()
            .zip(&edges)
            .zip(&deleted)
            .map(|((node, edge), deleted)| node.is_some() || edge.is_some() || *deleted)
            .collect::<Vec<_>>();
        metrics.found = taken.iter().filter(|value| **value).count() as u64;
        Ok((taken, metrics))
    }
}

fn deleted_identities_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        DELETED_IDENTITIES_COLUMN,
        DataType::FixedSizeBinary(16),
        false,
    )]))
}

/// Read the sorted record of deleted entity UUIDs under `root`; empty when no
/// generation has deleted anything.
///
/// # Errors
/// Returns an error when the file is unreadable, unsorted or not unique.
pub fn read_deleted_identities(root: &Path) -> Result<Vec<[u8; 16]>, GfError> {
    let path = root.join(DELETED_IDENTITIES_PATH);
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(storage(format!("{}: {error}", path.display()))),
    };
    parse_deleted_identities(file)
}

fn parse_deleted_identities(file: std::fs::File) -> Result<Vec<[u8; 16]>, GfError> {
    let file = crate::catalog::admitted_path_file(file).map_err(storage)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(storage)?
        .build()
        .map_err(storage)?;
    let mut identities: Vec<[u8; 16]> = Vec::new();
    for batch in reader {
        let batch = batch.map_err(storage)?;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .filter(|array| array.value_length() == 16 && array.null_count() == 0)
            .ok_or_else(|| storage("deleted identities are not FixedSizeBinary(16)"))?;
        for row in 0..column.len() {
            let value: [u8; 16] = column.value(row).try_into().expect("width checked");
            if identities.last().is_some_and(|last| *last >= value) {
                return Err(storage("deleted identities are not sorted and unique"));
            }
            identities.push(value);
        }
    }
    Ok(identities)
}

/// Stage the record of deleted UUIDs with `additions` merged in. Writes
/// nothing when `additions` is empty, so a graph that never deletes never
/// carries the file.
///
/// # Errors
/// Returns an error when the existing record is unreadable or staging fails.
pub fn stage_deleted_identities(
    staged: &mut crate::RewriteBatch,
    root: &Path,
    additions: &[Uuid],
) -> Result<(), GfError> {
    if additions.is_empty() {
        return Ok(());
    }
    let mut merged = read_deleted_identities(root)?;
    let before = merged.len();
    merged.extend(additions.iter().map(|uuid| *uuid.as_bytes()));
    merged.sort_unstable();
    merged.dedup();
    if merged.len() == before {
        return Ok(());
    }
    let column = FixedSizeBinaryArray::try_from_iter(merged.iter().map(<[u8; 16]>::as_slice))
        .map_err(storage)?;
    let batch = RecordBatch::try_new(deleted_identities_schema(), vec![Arc::new(column)])
        .map_err(storage)?;
    staged.restage(
        &root.join(DELETED_IDENTITIES_PATH),
        deleted_identities_schema(),
        &batch,
    )
}

#[cfg(test)]
mod tests;
