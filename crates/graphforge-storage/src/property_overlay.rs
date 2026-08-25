//! Authenticated immutable property snapshot overlays (#940).
//!
//! A fragment row is a complete property snapshot for one UUID. Fragment
//! authority is the numeric `(generation, ordinal)` encoded in its canonical
//! filename; directory order and mtimes never select a winner.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, BooleanArray, RecordBatch};
use bytes::Bytes;
use graphforge_core::GfError;
use graphforge_ir::IrLiteral;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// On-disk property overlay format marker.
pub const PROPERTY_OVERLAY_FORMAT: &str = "full-snapshot-v1";
/// Schema metadata key carrying [`PROPERTY_OVERLAY_FORMAT`].
pub const PROPERTY_OVERLAY_FORMAT_KEY: &str = "graphforge.property_overlay";
/// Reserved non-user column marking whole-row deletion.
pub const PROPERTY_TOMBSTONE_FIELD: &str = "__gf_property_tombstone";
pub(crate) const PROPERTY_ROUTE_KEY: &str = "graphforge.property_route";
pub(crate) const PROPERTY_KIND_KEY: &str = "graphforge.property_kind";
pub(crate) const PROPERTY_GENERATION_KEY: &str = "graphforge.property_generation";
pub(crate) const PROPERTY_ORDINAL_KEY: &str = "graphforge.property_ordinal";

/// Node and edge property namespaces are disjoint authorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PropertyRouteKind {
    /// `properties/<route>/...`
    Node,
    /// `edge_properties/<route>/...`
    Edge,
}

impl PropertyRouteKind {
    fn subdir(self) -> &'static str {
        match self {
            Self::Node => "properties",
            Self::Edge => "edge_properties",
        }
    }

    pub(crate) fn metadata_value(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }

    fn uuid_field(self) -> &'static str {
        match self {
            Self::Node => "node_uuid",
            Self::Edge => "edge_uuid",
        }
    }
}

#[derive(Debug, Default)]
struct ReadCounts {
    bytes: AtomicU64,
    blocks: AtomicU64,
    range_seeks: AtomicU64,
}

#[derive(Debug)]
struct CountingChunkReader {
    file: File,
    length: u64,
    counts: Arc<ReadCounts>,
}

struct CountingRead<R> {
    inner: R,
    counts: Arc<ReadCounts>,
}

impl<R: std::io::Read> std::io::Read for CountingRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        const BLOCK_BYTES: usize = 64 * 1024;
        let limit = buffer.len().min(BLOCK_BYTES);
        let read = self.inner.read(&mut buffer[..limit])?;
        if read != 0 {
            self.counts
                .bytes
                .fetch_add(u64::try_from(read).unwrap_or(u64::MAX), Ordering::Relaxed);
            self.counts.blocks.fetch_add(1, Ordering::Relaxed);
        }
        Ok(read)
    }
}

impl Length for CountingChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for CountingChunkReader {
    type T = CountingRead<BufReader<File>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        use std::io::{Seek, SeekFrom};
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        self.counts.range_seeks.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: BufReader::new(file),
            counts: Arc::clone(&self.counts),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut reader = self.get_read(start)?;
        let mut buffer = vec![0; length];
        std::io::Read::read_exact(&mut reader, &mut buffer)?;
        Ok(Bytes::from(buffer))
    }
}

/// Canonical newest-wins authority of one immutable fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyFragmentId {
    /// Committed topology generation associated with the write window.
    pub generation: u64,
    /// Route-local ordinal within that generation.
    pub ordinal: u64,
}

impl PropertyFragmentId {
    /// Parse exactly `GGGGGGGGGGGGGGGGGGGG-OOOOOOOOOOOOOOOOOOOO.parquet`.
    pub fn parse(name: &str) -> Result<Self, GfError> {
        let body = name
            .strip_suffix(".parquet")
            .ok_or_else(|| corrupt("property fragment name lacks the canonical .parquet suffix"))?;
        let (generation, ordinal) = body.split_once('-').ok_or_else(|| {
            corrupt("property fragment name lacks canonical generation and ordinal")
        })?;
        if generation.len() != 20
            || ordinal.len() != 20
            || !generation.bytes().all(|byte| byte.is_ascii_digit())
            || !ordinal.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(corrupt("property fragment identity is not canonical"));
        }
        let id = Self {
            generation: generation
                .parse()
                .map_err(|_| corrupt("property fragment generation overflows u64"))?,
            ordinal: ordinal
                .parse()
                .map_err(|_| corrupt("property fragment ordinal overflows u64"))?,
        };
        if id.file_name() != name {
            return Err(corrupt("property fragment identity is not canonical"));
        }
        Ok(id)
    }

    /// Render the sole accepted filename representation.
    #[must_use]
    pub fn file_name(self) -> String {
        format!("{:020}-{:020}.parquet", self.generation, self.ordinal)
    }
}

/// Strictly admitted immutable property fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyFragment {
    /// Numeric authority parsed from the filename.
    pub id: PropertyFragmentId,
    /// Canonical fragment path.
    pub path: PathBuf,
}

/// Property fragments resolved from a committed graph-files inventory and
/// retained through no-follow directory capabilities.
#[derive(Debug)]
pub struct AuthenticatedPropertyInventory {
    root: Option<graphforge_filesystem::StableDirectory>,
    routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>>,
    schemas: BTreeMap<(PropertyRouteKind, String), arrow::datatypes::SchemaRef>,
}

#[derive(Debug)]
struct AuthenticatedPropertyFragment {
    id: PropertyFragmentId,
    entry: crate::GraphFileEntry,
    physical_relative: PathBuf,
}

#[derive(Debug)]
struct RouteSchemaBuilder {
    uuid: arrow::datatypes::FieldRef,
    fields: BTreeMap<String, arrow::datatypes::FieldRef>,
    metadata: HashMap<String, String>,
}

impl AuthenticatedPropertyInventory {
    /// Resolve property authority from a pinned project generation.
    ///
    /// Expanded V1 generations retain files beneath their authenticated graph
    /// tree. Compact V2 generations retain the exact digest-addressed CAS
    /// object named by each authenticated manifest entry.
    pub fn from_resolved_generation(
        generation: &crate::ResolvedProjectGeneration,
    ) -> Result<Self, GfError> {
        let Some(participant) = generation.declared_graph_files_participant()? else {
            return Ok(Self {
                root: None,
                routes: BTreeMap::new(),
                schemas: BTreeMap::new(),
            });
        };
        let inventory = generation.graph_files_inventory()?.ok_or_else(|| {
            corrupt("declared graph-files participant has no authenticated inventory")
        })?;
        match participant {
            crate::graph_files::GraphFilesParticipant::V1(_) => {
                Self::from_entries_at_root(&generation.graph_tree_root(), inventory.files)
            }
            crate::graph_files::GraphFilesParticipant::V2(_) => {
                let root = generation.container_root();
                let entries = inventory
                    .files
                    .into_iter()
                    .map(|entry| {
                        let path = crate::graph_object_path(root, &entry.content_sha256)?;
                        let relative = path
                            .strip_prefix(root)
                            .map_err(|_| corrupt("graph object path escaped its container"))?;
                        Ok((entry, relative.to_path_buf()))
                    })
                    .collect::<Result<Vec<_>, GfError>>()?;
                Self::admit_entries(root, entries)
            }
        }
    }

    fn from_entries_at_root(
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
    ) -> Result<Self, GfError> {
        let entries = entries
            .into_iter()
            .map(|entry| {
                let relative = PathBuf::from(&entry.relative_path);
                (entry, relative)
            })
            .collect();
        Self::admit_entries(root, entries)
    }

    fn admit_entries(
        root_path: &Path,
        entries: Vec<(crate::GraphFileEntry, PathBuf)>,
    ) -> Result<Self, GfError> {
        let root = graphforge_filesystem::StableDirectory::open(root_path).map_err(io_error)?;
        let mut routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>> =
            BTreeMap::new();
        let mut schemas = BTreeMap::new();
        for (entry, physical_relative) in entries {
            let parsed = parse_inventory_property_path(&entry.relative_path)?;
            if entry.role != crate::GraphFileRole::Properties {
                if parsed.is_some() {
                    return Err(corrupt("property inventory entry has the wrong role"));
                }
                continue;
            }
            let Some((kind, route, id)) = parsed else {
                return Err(corrupt("properties role names a non-property path"));
            };
            let file = open_retained_under(&root, &physical_relative)?;
            authenticate_inventory_file(&file, &entry)?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file.try_clone().map_err(io_error)?)
                    .map_err(parquet_error)?;
            validate_fragment_schema(builder.schema().as_ref(), id, kind, &route)?;
            merge_route_schema(&mut schemas, kind, &route, builder.schema().as_ref())?;
            let fragments = routes.entry((kind, route)).or_default();
            if fragments.iter().any(|fragment| fragment.id == id) {
                return Err(corrupt("property inventory contains duplicate authority"));
            }
            fragments.push(AuthenticatedPropertyFragment {
                id,
                entry,
                physical_relative,
            });
        }
        for fragments in routes.values_mut() {
            fragments.sort_unstable_by_key(|fragment| fragment.id);
        }
        Ok(Self {
            root: Some(root),
            routes,
            schemas: schemas
                .into_iter()
                .map(|(key, schema): (_, RouteSchemaBuilder)| {
                    let mut fields = vec![schema.uuid];
                    fields.extend(schema.fields.into_values());
                    (
                        key,
                        Arc::new(arrow::datatypes::Schema::new_with_metadata(
                            fields,
                            schema.metadata,
                        )),
                    )
                })
                .collect(),
        })
    }

    /// Canonical logical schema authenticated across every fragment in a route.
    #[must_use]
    pub fn route_schema(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Option<arrow::datatypes::SchemaRef> {
        self.schemas.get(&(kind, route.to_owned())).cloned()
    }

    /// Visit one authenticated route through the retained generation
    /// capability, opening and closing one Parquet decoder at a time.
    pub fn visit_route<F>(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        scratch: &Path,
        limits: PropertyOverlayLimits,
        emit: F,
    ) -> Result<PropertyOverlayMetrics, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(PropertyOverlayMetrics::default());
        };
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| corrupt("property inventory lost its retained root"))?;
        let counts = Arc::new(ReadCounts::default());
        let failed = Arc::new(Mutex::new(None));
        let decoded = Arc::new(Mutex::new(DecodedRetention::default()));
        let inputs = fragments.iter().map(|fragment| {
            let reader = (|| {
                let file = open_retained_under(root, &fragment.physical_relative)?;
                authenticate_inventory_file(&file, &fragment.entry)?;
                let source = CountingChunkReader {
                    length: fragment.entry.byte_length,
                    file,
                    counts: Arc::clone(&counts),
                };
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?;
                validate_fragment_schema(builder.schema().as_ref(), fragment.id, kind, route)?;
                builder.with_batch_size(4096).build().map_err(parquet_error)
            })();
            let reader = match reader {
                Ok(reader) => Some(reader),
                Err(error) => {
                    *failed.lock().expect("property failure lock") = Some(error);
                    None
                }
            };
            (
                fragment.id,
                0,
                0,
                PropertyParquetRows {
                    reader,
                    fragment: None,
                    kind,
                    route: route.to_owned(),
                    counts: Arc::clone(&counts),
                    current: Vec::new().into_iter(),
                    uuid_field: kind.uuid_field(),
                    failed: Arc::clone(&failed),
                    decoded: Arc::clone(&decoded),
                    #[cfg(test)]
                    opened: false,
                },
            )
        });
        let mut metrics = visit_newest_property_snapshots(inputs, scratch, limits, emit)?;
        if let Some(error) = failed.lock().expect("property failure lock").take() {
            return Err(error);
        }
        metrics.physical_bytes = counts.bytes.load(Ordering::Relaxed);
        metrics.blocks_read = counts.blocks.load(Ordering::Relaxed);
        metrics.range_seeks = counts.range_seeks.load(Ordering::Relaxed);
        let decoded = decoded.lock().expect("property retention lock");
        metrics.decoder_peak_rows = decoded.peak_rows;
        metrics.decoder_peak_bytes = decoded.peak_bytes;
        metrics.emitted_batches = decoded.batches;
        metrics.merge_peak_rows = metrics.peak_buffered_rows;
        metrics.merge_peak_bytes = metrics.peak_buffered_bytes;
        metrics.peak_buffered_rows = metrics
            .decoder_peak_rows
            .saturating_add(metrics.merge_peak_rows);
        metrics.peak_buffered_bytes = metrics
            .decoder_peak_bytes
            .saturating_add(metrics.merge_peak_bytes);
        Ok(metrics)
    }
}

fn merge_route_schema(
    schemas: &mut BTreeMap<(PropertyRouteKind, String), RouteSchemaBuilder>,
    kind: PropertyRouteKind,
    route: &str,
    fragment: &arrow::datatypes::Schema,
) -> Result<(), GfError> {
    const IDENTITY_KEYS: [&str; 5] = [
        PROPERTY_OVERLAY_FORMAT_KEY,
        PROPERTY_ROUTE_KEY,
        PROPERTY_KIND_KEY,
        PROPERTY_GENERATION_KEY,
        PROPERTY_ORDINAL_KEY,
    ];
    let uuid = Arc::clone(&fragment.fields()[0]);
    let schema = schemas
        .entry((kind, route.to_owned()))
        .or_insert_with(|| RouteSchemaBuilder {
            uuid: Arc::clone(&uuid),
            fields: BTreeMap::new(),
            metadata: HashMap::new(),
        });
    if schema.uuid.as_ref() != uuid.as_ref() {
        return Err(corrupt("property route UUID schemas conflict"));
    }
    for (name, value) in fragment.metadata() {
        if IDENTITY_KEYS.contains(&name.as_str()) {
            continue;
        }
        if schema
            .metadata
            .insert(name.clone(), value.clone())
            .is_some_and(|prior| prior != *value)
        {
            return Err(corrupt("property route semantic metadata conflicts"));
        }
    }
    for field in fragment.fields().iter().skip(1) {
        if field.name() == PROPERTY_TOMBSTONE_FIELD {
            continue;
        }
        if let Some(prior) = schema.fields.get(field.name()) {
            if prior.as_ref() != field.as_ref() {
                return Err(corrupt(
                    "property route field type or semantic metadata conflicts",
                ));
            }
        } else {
            schema
                .fields
                .insert(field.name().clone(), Arc::clone(field));
        }
    }
    Ok(())
}

fn parse_inventory_property_path(
    relative: &str,
) -> Result<Option<(PropertyRouteKind, String, PropertyFragmentId)>, GfError> {
    let parts = relative.split('/').collect::<Vec<_>>();
    let kind = match parts.first().copied() {
        Some("properties") => PropertyRouteKind::Node,
        Some("edge_properties") => PropertyRouteKind::Edge,
        _ => return Ok(None),
    };
    match parts.as_slice() {
        [_, legacy] => {
            let route = legacy
                .strip_suffix(".parquet")
                .filter(|route| !route.is_empty())
                .ok_or_else(|| corrupt("legacy property inventory path is malformed"))?;
            Ok(Some((
                kind,
                route.to_owned(),
                PropertyFragmentId {
                    generation: 0,
                    ordinal: 0,
                },
            )))
        }
        [_, route, name] if !route.is_empty() => Ok(Some((
            kind,
            (*route).to_owned(),
            PropertyFragmentId::parse(name)?,
        ))),
        _ => Err(corrupt("property inventory path is not canonical")),
    }
}

fn open_retained_under(
    root: &graphforge_filesystem::StableDirectory,
    relative: &Path,
) -> Result<File, GfError> {
    let mut components = relative.components().peekable();
    let mut retained = Vec::new();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(corrupt("property inventory path is not contained"));
        };
        let directory = retained.last().unwrap_or(root);
        if components.peek().is_none() {
            return directory.open_child_file(name).map_err(io_error);
        }
        let child = directory.open_child_directory(name).map_err(io_error)?;
        retained.push(child);
    }
    Err(corrupt("property inventory path is empty"))
}

fn authenticate_inventory_file(file: &File, entry: &crate::GraphFileEntry) -> Result<(), GfError> {
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(corrupt(
            "property handle length or kind conflicts with inventory",
        ));
    }
    let mut clone = file.try_clone().map_err(io_error)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut clone, &mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if digest_hex(&digest.finalize()) != entry.content_sha256 {
        return Err(corrupt("property handle digest conflicts with inventory"));
    }
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Exact work performed by one property overlay operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyOverlayMetrics {
    /// Physical snapshot rows decoded.
    pub physical_rows: u64,
    /// Authenticated fragment bytes read.
    pub physical_bytes: u64,
    /// Non-empty input reads, each capped at 64 KiB.
    pub blocks_read: u64,
    /// Retained-handle range starts requested by the Parquet decoder.
    pub range_seeks: u64,
    /// Parquet row groups whose authenticated statistics were considered.
    pub row_groups_considered: u64,
    /// Parquet row groups selected for decode.
    pub row_groups_selected: u64,
    /// Bounded Arrow batches emitted to the consumer.
    pub emitted_batches: u64,
    /// Canonical fragments considered for authority.
    pub fragments_considered: u64,
    /// Live newest snapshot rows emitted.
    pub logical_rows: u64,
    /// Older rows suppressed by newer snapshots or tombstones.
    pub shadowed_rows: u64,
    /// Newest tombstone rows observed.
    pub tombstones: u64,
    /// Bytes written to external merge runs.
    pub spill_bytes: u64,
    /// External merge runs written.
    pub spill_runs: u64,
    /// Bounded fan-in merge passes performed.
    pub merge_passes: u64,
    /// Maximum decoded rows retained at once.
    pub peak_buffered_rows: u64,
    /// Maximum charged decoded bytes retained at once.
    pub peak_buffered_bytes: u64,
    /// Maximum rows retained by one decoded Parquet batch.
    pub decoder_peak_rows: u64,
    /// Maximum charged row bytes retained by one decoded Parquet batch.
    pub decoder_peak_bytes: u64,
    /// Maximum rows retained by spill sorting or k-way cursors.
    pub merge_peak_rows: u64,
    /// Maximum charged bytes retained by spill sorting or k-way cursors.
    pub merge_peak_bytes: u64,
    /// Random/per-record seeks are forbidden and remain zero.
    pub per_record_seeks: u64,
}

/// Complete logical property state for one UUID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertySnapshotRow {
    /// Node or edge UUID.
    pub uuid: [u8; 16],
    /// Whole-property-row deletion marker.
    pub tombstone: bool,
    /// Complete live property map. Tombstones must carry no values.
    pub values: BTreeMap<String, IrLiteral>,
}

/// Explicit bounded merge limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyOverlayLimits {
    /// Maximum rows retained while producing one sorted spill run.
    pub max_buffered_rows: usize,
    /// Maximum runs opened in one merge pass.
    pub max_open_runs: usize,
    /// Maximum charged decoded bytes in one spill buffer.
    pub max_buffered_bytes: u64,
    /// Maximum charged bytes for one snapshot row.
    pub max_row_bytes: u64,
}

struct PropertyParquetRows {
    reader: Option<ParquetRecordBatchReader>,
    fragment: Option<PropertyFragment>,
    kind: PropertyRouteKind,
    route: String,
    counts: Arc<ReadCounts>,
    current: std::vec::IntoIter<PropertySnapshotRow>,
    uuid_field: &'static str,
    failed: Arc<Mutex<Option<GfError>>>,
    decoded: Arc<Mutex<DecodedRetention>>,
    #[cfg(test)]
    opened: bool,
}

#[cfg(test)]
static OPEN_FRAGMENT_READERS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static PEAK_OPEN_FRAGMENT_READERS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
impl Drop for PropertyParquetRows {
    fn drop(&mut self) {
        if self.opened {
            OPEN_FRAGMENT_READERS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug, Default)]
struct DecodedRetention {
    current_rows: u64,
    current_bytes: u64,
    peak_rows: u64,
    peak_bytes: u64,
    batches: u64,
}

impl Iterator for PropertyParquetRows {
    type Item = PropertySnapshotRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_none() {
            let fragment = self.fragment.take()?;
            let opened =
                open_fragment_reader(&fragment, self.kind, &self.route, Arc::clone(&self.counts));
            match opened {
                Ok(reader) => {
                    self.reader = Some(reader);
                    #[cfg(test)]
                    {
                        self.opened = true;
                        let current = OPEN_FRAGMENT_READERS.fetch_add(1, Ordering::SeqCst) + 1;
                        PEAK_OPEN_FRAGMENT_READERS.fetch_max(current, Ordering::SeqCst);
                    }
                }
                Err(error) => {
                    *self.failed.lock().expect("property failure lock") = Some(error);
                    return None;
                }
            }
        }
        loop {
            if let Some(row) = self.current.next() {
                let mut decoded = self.decoded.lock().expect("property retention lock");
                decoded.current_rows = decoded.current_rows.saturating_sub(1);
                decoded.current_bytes = decoded.current_bytes.saturating_sub(snapshot_charge(&row));
                return Some(row);
            }
            let batch = match self.reader.as_mut()?.next()? {
                Ok(batch) => batch,
                Err(error) => {
                    *self.failed.lock().expect("property failure lock") =
                        Some(GfError::Storage(format!("property overlay Arrow: {error}")));
                    return None;
                }
            };
            match decode_snapshot_batch(&batch, self.uuid_field) {
                Ok(rows) => {
                    let bytes = rows.iter().fold(0_u64, |total, row| {
                        total.saturating_add(snapshot_charge(row))
                    });
                    let mut decoded = self.decoded.lock().expect("property retention lock");
                    decoded.current_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
                    decoded.current_bytes = bytes;
                    decoded.peak_rows = decoded.peak_rows.max(decoded.current_rows);
                    decoded.peak_bytes = decoded.peak_bytes.max(decoded.current_bytes);
                    decoded.batches = decoded.batches.saturating_add(1);
                    drop(decoded);
                    self.current = rows.into_iter();
                }
                Err(error) => {
                    *self.failed.lock().expect("property failure lock") = Some(error);
                    return None;
                }
            }
        }
    }
}

struct AuthenticatedFragmentRows {
    id: PropertyFragmentId,
    rows: PropertyParquetRows,
    counts: Arc<ReadCounts>,
    failed: Arc<Mutex<Option<GfError>>>,
    decoded: Arc<Mutex<DecodedRetention>>,
}

/// Scan a route through authenticated retained file handles and emit its
/// full-snapshot-v1 newest live rows. This is the production authority; the
/// generic external merge helper remains crate-internal.
pub fn visit_authenticated_property_snapshots<F>(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
    scratch: &Path,
    limits: PropertyOverlayLimits,
    emit: F,
) -> Result<PropertyOverlayMetrics, GfError>
where
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    let fragments = enumerate_property_fragments(project, kind, route)?;
    let mut admitted = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        admitted.push(open_authenticated_fragment(fragment, kind, route)?);
    }
    let counters = admitted
        .iter()
        .map(|fragment| Arc::clone(&fragment.counts))
        .collect::<Vec<_>>();
    let failures = admitted
        .iter_mut()
        .map(|fragment| Arc::clone(&fragment.failed))
        .collect::<Vec<_>>();
    let decoded = admitted
        .iter()
        .map(|fragment| Arc::clone(&fragment.decoded))
        .collect::<Vec<_>>();
    let inputs = admitted
        .into_iter()
        .map(|fragment| (fragment.id, 0, 0, fragment.rows));
    let mut metrics = visit_newest_property_snapshots(inputs, scratch, limits, emit)?;
    if let Some(error) = failures
        .iter()
        .find_map(|failure| failure.lock().expect("property failure lock").take())
    {
        return Err(error);
    }
    metrics.physical_bytes = counters
        .iter()
        .map(|counts| counts.bytes.load(Ordering::Relaxed))
        .sum();
    metrics.blocks_read = counters
        .iter()
        .map(|counts| counts.blocks.load(Ordering::Relaxed))
        .sum();
    metrics.range_seeks = counters
        .iter()
        .map(|counts| counts.range_seeks.load(Ordering::Relaxed))
        .sum();
    metrics.merge_peak_rows = metrics.peak_buffered_rows;
    metrics.merge_peak_bytes = metrics.peak_buffered_bytes;
    for retention in decoded {
        let retention = retention.lock().expect("property retention lock");
        metrics.decoder_peak_rows = metrics.decoder_peak_rows.max(retention.peak_rows);
        metrics.decoder_peak_bytes = metrics.decoder_peak_bytes.max(retention.peak_bytes);
        metrics.emitted_batches = metrics.emitted_batches.saturating_add(retention.batches);
    }
    metrics.peak_buffered_rows = metrics
        .decoder_peak_rows
        .saturating_add(metrics.merge_peak_rows);
    metrics.peak_buffered_bytes = metrics
        .decoder_peak_bytes
        .saturating_add(metrics.merge_peak_bytes);
    Ok(metrics)
}

/// Resolve the canonical authenticated logical schema for one route.
pub(crate) fn authenticated_property_route_schema(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<Option<arrow::datatypes::SchemaRef>, GfError> {
    let (inventory, _) = crate::capture_graph_files(project)?;
    AuthenticatedPropertyInventory::from_entries_at_root(project, inventory.files)
        .map(|inventory| inventory.route_schema(kind, route))
}

/// Resolve a bounded UUID batch newest-first without decoding unrelated row
/// groups. Caller order is restored by the returned map lookup.
pub fn read_authenticated_property_snapshots_for(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
    targets: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<
    (
        BTreeMap<[u8; 16], PropertySnapshotRow>,
        PropertyOverlayMetrics,
    ),
    GfError,
> {
    let mut unresolved = targets.clone();
    let mut found = BTreeMap::new();
    let mut metrics = PropertyOverlayMetrics::default();
    let mut fragments = enumerate_property_fragments(project, kind, route)?;
    fragments.reverse();
    for fragment in fragments {
        if unresolved.is_empty() {
            break;
        }
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&fragment.path).map_err(io_error)?;
        let metadata = file.metadata().map_err(io_error)?;
        if !metadata.file_type().is_file() {
            return Err(corrupt("property fragment handle is not a regular file"));
        }
        let counts = Arc::new(ReadCounts::default());
        let source = CountingChunkReader {
            file,
            length: metadata.len(),
            counts: Arc::clone(&counts),
        };
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?;
        validate_fragment_schema(builder.schema().as_ref(), fragment.id, kind, route)?;
        // Canonical overlay schemas require the non-nested UUID as leaf zero;
        // never substitute an Arrow top-level index for a Parquet leaf index.
        let uuid_index = 0;
        metrics.row_groups_considered = metrics
            .row_groups_considered
            .saturating_add(u64::try_from(builder.metadata().num_row_groups()).unwrap_or(u64::MAX));
        let mut row_groups = Vec::new();
        for (index, group) in builder.metadata().row_groups().iter().enumerate() {
            let statistics = group
                .column(uuid_index)
                .statistics()
                .ok_or_else(|| corrupt("property UUID row group lacks statistics"))?;
            let min: [u8; 16] = statistics
                .min_bytes_opt()
                .ok_or_else(|| corrupt("property UUID statistics lack minimum"))?
                .try_into()
                .map_err(|_| corrupt("property UUID statistics have wrong width"))?;
            let max: [u8; 16] = statistics
                .max_bytes_opt()
                .ok_or_else(|| corrupt("property UUID statistics lack maximum"))?
                .try_into()
                .map_err(|_| corrupt("property UUID statistics have wrong width"))?;
            if unresolved.range(min..=max).next().is_some() {
                row_groups.push(index);
            }
        }
        if !row_groups.is_empty() {
            metrics.row_groups_selected = metrics
                .row_groups_selected
                .saturating_add(u64::try_from(row_groups.len()).unwrap_or(u64::MAX));
            builder = builder.with_row_groups(row_groups);
            let reader = builder
                .with_batch_size(4096)
                .build()
                .map_err(parquet_error)?;
            for batch in reader {
                let batch = batch.map_err(|error| GfError::Storage(error.to_string()))?;
                for row in decode_snapshot_batch(&batch, kind.uuid_field())? {
                    metrics.physical_rows = metrics.physical_rows.saturating_add(1);
                    if unresolved.remove(&row.uuid) {
                        if !row.tombstone {
                            found.insert(row.uuid, row);
                        } else {
                            metrics.tombstones = metrics.tombstones.saturating_add(1);
                        }
                    }
                }
            }
        }
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics
            .physical_bytes
            .saturating_add(counts.bytes.load(Ordering::Relaxed));
        metrics.blocks_read = metrics
            .blocks_read
            .saturating_add(counts.blocks.load(Ordering::Relaxed));
        metrics.range_seeks = metrics
            .range_seeks
            .saturating_add(counts.range_seeks.load(Ordering::Relaxed));
    }
    metrics.logical_rows = u64::try_from(found.len()).unwrap_or(u64::MAX);
    Ok((found, metrics))
}

fn open_authenticated_fragment(
    fragment: PropertyFragment,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<AuthenticatedFragmentRows, GfError> {
    let id = fragment.id;
    let counts = Arc::new(ReadCounts::default());
    let failed = Arc::new(Mutex::new(None));
    let decoded = Arc::new(Mutex::new(DecodedRetention::default()));
    Ok(AuthenticatedFragmentRows {
        id,
        rows: PropertyParquetRows {
            reader: None,
            fragment: Some(fragment),
            kind,
            route: route.to_owned(),
            counts: Arc::clone(&counts),
            current: Vec::new().into_iter(),
            uuid_field: kind.uuid_field(),
            failed: Arc::clone(&failed),
            decoded: Arc::clone(&decoded),
            #[cfg(test)]
            opened: false,
        },
        counts,
        failed,
        decoded,
    })
}

fn open_fragment_reader(
    fragment: &PropertyFragment,
    kind: PropertyRouteKind,
    route: &str,
    counts: Arc<ReadCounts>,
) -> Result<ParquetRecordBatchReader, GfError> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(&fragment.path).map_err(io_error)?;
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.file_type().is_file() {
        return Err(corrupt("property fragment handle is not a regular file"));
    }
    let source = CountingChunkReader {
        file,
        length: metadata.len(),
        counts: Arc::clone(&counts),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?;
    validate_fragment_schema(builder.schema().as_ref(), fragment.id, kind, route)?;
    let reader = builder
        .with_batch_size(4096)
        .build()
        .map_err(parquet_error)?;
    Ok(reader)
}

fn validate_fragment_schema(
    schema: &arrow::datatypes::Schema,
    id: PropertyFragmentId,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<(), GfError> {
    let uuid = schema
        .field_with_name(kind.uuid_field())
        .map_err(|_| corrupt("property fragment lacks its UUID field"))?;
    if uuid.is_nullable() || uuid.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16) {
        return Err(corrupt(
            "property UUID field is nullable or not fixed binary(16)",
        ));
    }
    if schema.fields().first().map(|field| field.name().as_str()) != Some(kind.uuid_field()) {
        return Err(corrupt("property UUID field is not canonical first field"));
    }
    if id.generation == 0 && id.ordinal == 0 {
        return Ok(());
    }
    let expected = [
        (
            PROPERTY_OVERLAY_FORMAT_KEY,
            PROPERTY_OVERLAY_FORMAT.to_owned(),
        ),
        (PROPERTY_ROUTE_KEY, route.to_owned()),
        (PROPERTY_KIND_KEY, kind.metadata_value().to_owned()),
        (PROPERTY_GENERATION_KEY, id.generation.to_string()),
        (PROPERTY_ORDINAL_KEY, id.ordinal.to_string()),
    ];
    for (key, value) in expected {
        if schema.metadata().get(key) != Some(&value) {
            return Err(corrupt(
                "property fragment metadata conflicts with its identity",
            ));
        }
    }
    let tombstone = schema
        .field_with_name(PROPERTY_TOMBSTONE_FIELD)
        .map_err(|_| corrupt("property snapshot fragment lacks tombstone field"))?;
    if tombstone.is_nullable() || tombstone.data_type() != &arrow::datatypes::DataType::Boolean {
        return Err(corrupt(
            "property tombstone field is nullable or not boolean",
        ));
    }
    if schema.fields().get(1).map(|field| field.name().as_str()) != Some(PROPERTY_TOMBSTONE_FIELD) {
        return Err(corrupt("property tombstone is not canonical second field"));
    }
    Ok(())
}

pub(crate) fn decode_snapshot_batch(
    batch: &RecordBatch,
    uuid_field: &str,
) -> Result<Vec<PropertySnapshotRow>, GfError> {
    let uuid = batch
        .column_by_name(uuid_field)
        .ok_or_else(|| corrupt("property batch lacks UUID column"))?;
    if uuid.null_count() != 0 {
        return Err(corrupt("property UUID column contains null slots"));
    }
    let tombstones = batch
        .column_by_name(PROPERTY_TOMBSTONE_FIELD)
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| corrupt("property tombstone column is not boolean"))
        })
        .transpose()?;
    if tombstones.is_some_and(|values| values.null_count() != 0) {
        return Err(corrupt("property tombstone column contains null slots"));
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    crate::writer::decode_property_batch(batch, uuid_field, |uuid, mut values| {
        values.remove(PROPERTY_TOMBSTONE_FIELD);
        let index = rows.len();
        rows.push(PropertySnapshotRow {
            uuid,
            tombstone: tombstones.is_some_and(|values| values.value(index)),
            values: values.into_iter().collect(),
        });
    })?;
    Ok(rows)
}

fn parquet_error(error: ParquetError) -> GfError {
    GfError::Storage(format!("property overlay Parquet: {error}"))
}

impl Default for PropertyOverlayLimits {
    fn default() -> Self {
        Self {
            max_buffered_rows: 4096,
            max_open_runs: 32,
            max_buffered_bytes: 64 * 1024 * 1024,
            max_row_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolRecord {
    uuid: [u8; 16],
    generation: u64,
    ordinal: u64,
    tombstone: bool,
    values: BTreeMap<String, IrLiteral>,
}

impl SpoolRecord {
    fn sort_key(&self) -> ([u8; 16], Reverse<(u64, u64)>) {
        (self.uuid, Reverse((self.generation, self.ordinal)))
    }
}

/// Bounded disk-backed newest-snapshot merge shared by property consumers.
///
/// Input rows may arrive in any fragment order. Runs are externally sorted by
/// UUID and descending numeric fragment authority. The final pass emits at
/// most one live row per UUID and suppresses a newest tombstone. No input path
/// is sought per record.
pub(crate) fn visit_newest_property_snapshots<I, R, F>(
    inputs: I,
    scratch: &Path,
    limits: PropertyOverlayLimits,
    mut emit: F,
) -> Result<PropertyOverlayMetrics, GfError>
where
    I: IntoIterator<Item = (PropertyFragmentId, u64, u64, R)>,
    R: IntoIterator<Item = PropertySnapshotRow>,
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    if limits.max_buffered_rows == 0
        || limits.max_open_runs < 2
        || limits.max_buffered_bytes == 0
        || limits.max_row_bytes == 0
        || limits.max_row_bytes > limits.max_buffered_bytes
    {
        return Err(corrupt("property overlay merge limits are invalid"));
    }
    fs::create_dir_all(scratch).map_err(io_error)?;
    let temp = tempfile::Builder::new()
        .prefix("property-overlay-")
        .tempdir_in(scratch)
        .map_err(io_error)?;
    let mut metrics = PropertyOverlayMetrics::default();
    let mut buffer = Vec::with_capacity(limits.max_buffered_rows);
    let mut runs = Vec::new();
    let mut buffered_bytes = 0_u64;
    for (id, physical_bytes, blocks_read, rows) in inputs {
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics.physical_bytes.saturating_add(physical_bytes);
        metrics.blocks_read = metrics.blocks_read.saturating_add(blocks_read);
        let mut prior = None;
        for row in rows {
            if row.tombstone && !row.values.is_empty() {
                return Err(corrupt("property tombstone carries live values"));
            }
            if prior.is_some_and(|uuid| uuid >= row.uuid) {
                return Err(corrupt("property fragment UUIDs are duplicate or unsorted"));
            }
            prior = Some(row.uuid);
            metrics.physical_rows = metrics.physical_rows.saturating_add(1);
            let record = SpoolRecord {
                uuid: row.uuid,
                generation: id.generation,
                ordinal: id.ordinal,
                tombstone: row.tombstone,
                values: row.values,
            };
            let charge = record_charge(&record);
            if charge > limits.max_row_bytes {
                return Err(corrupt("property snapshot row exceeds byte limit"));
            }
            if !buffer.is_empty()
                && buffered_bytes
                    .checked_add(charge)
                    .is_none_or(|bytes| bytes > limits.max_buffered_bytes)
            {
                runs.push(write_sorted_run(
                    temp.path(),
                    runs.len(),
                    &mut buffer,
                    &mut metrics,
                )?);
                buffered_bytes = 0;
            }
            buffered_bytes = buffered_bytes
                .checked_add(charge)
                .ok_or_else(|| corrupt("property snapshot byte charge overflows"))?;
            buffer.push(record);
            metrics.peak_buffered_rows = metrics
                .peak_buffered_rows
                .max(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
            metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(buffered_bytes);
            if buffer.len() == limits.max_buffered_rows {
                runs.push(write_sorted_run(
                    temp.path(),
                    runs.len(),
                    &mut buffer,
                    &mut metrics,
                )?);
                buffered_bytes = 0;
            }
        }
    }
    if !buffer.is_empty() {
        runs.push(write_sorted_run(
            temp.path(),
            runs.len(),
            &mut buffer,
            &mut metrics,
        )?);
    }
    while runs.len() > limits.max_open_runs {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(limits.max_open_runs).enumerate() {
            let path = temp
                .path()
                .join(format!("pass-{}-{group}.jsonl", metrics.merge_passes));
            merge_runs::<F>(chunk, &path, None, &mut metrics)?;
            next.push(path);
        }
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
        runs = next;
    }
    if !runs.is_empty() {
        merge_runs(
            &runs,
            &temp.path().join("final.jsonl"),
            Some(&mut emit),
            &mut metrics,
        )?;
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
    }
    Ok(metrics)
}

fn write_sorted_run(
    root: &Path,
    ordinal: usize,
    rows: &mut Vec<SpoolRecord>,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<PathBuf, GfError> {
    rows.sort_unstable_by_key(SpoolRecord::sort_key);
    let path = root.join(format!("run-{ordinal}.jsonl"));
    let mut writer = BufWriter::new(File::create(&path).map_err(io_error)?);
    for row in rows.drain(..) {
        serde_json::to_writer(&mut writer, &row).map_err(json_error)?;
        writer.write_all(b"\n").map_err(io_error)?;
    }
    writer.flush().map_err(io_error)?;
    let bytes = writer.get_ref().metadata().map_err(io_error)?.len();
    metrics.spill_runs = metrics.spill_runs.saturating_add(1);
    metrics.spill_bytes = metrics.spill_bytes.saturating_add(bytes);
    Ok(path)
}

fn record_charge(record: &SpoolRecord) -> u64 {
    let values = serde_json::to_vec(&record.values).map_or(u64::MAX, |encoded| {
        u64::try_from(encoded.len()).unwrap_or(u64::MAX)
    });
    16_u64
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(1)
        .saturating_add(values)
}

fn snapshot_charge(row: &PropertySnapshotRow) -> u64 {
    let values = serde_json::to_vec(&row.values).map_or(u64::MAX, |encoded| {
        u64::try_from(encoded.len()).unwrap_or(u64::MAX)
    });
    16_u64.saturating_add(1).saturating_add(values)
}

fn merge_runs<F>(
    runs: &[PathBuf],
    output: &Path,
    mut emit: Option<&mut F>,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<(), GfError>
where
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    let mut readers = runs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(io_error))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        current.push(read_spool(reader)?);
        if let Some(row) = &current[index] {
            heap.push(Reverse((row.sort_key(), index)));
        }
    }
    let cursor_bytes = current
        .iter()
        .flatten()
        .fold(0_u64, |total, row| total.saturating_add(record_charge(row)));
    metrics.peak_buffered_rows = metrics
        .peak_buffered_rows
        .max(u64::try_from(current.iter().flatten().count()).unwrap_or(u64::MAX));
    metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(cursor_bytes);
    let mut writer = (emit.is_none())
        .then(|| File::create(output).map(BufWriter::new).map_err(io_error))
        .transpose()?;
    let mut resolved_uuid = None;
    while let Some(Reverse((_, index))) = heap.pop() {
        let row = current[index].take().expect("heap row exists");
        if emit.is_some() {
            let newest = resolved_uuid != Some(row.uuid);
            if newest {
                resolved_uuid = Some(row.uuid);
                if row.tombstone {
                    metrics.tombstones = metrics.tombstones.saturating_add(1);
                } else if let Some(visitor) = emit.as_deref_mut() {
                    visitor(PropertySnapshotRow {
                        uuid: row.uuid,
                        tombstone: false,
                        values: row.values.clone(),
                    })?;
                    metrics.logical_rows = metrics.logical_rows.saturating_add(1);
                }
            } else {
                metrics.shadowed_rows = metrics.shadowed_rows.saturating_add(1);
            }
        }
        if let Some(out) = writer.as_mut() {
            serde_json::to_writer(&mut *out, &row).map_err(json_error)?;
            out.write_all(b"\n").map_err(io_error)?;
        }
        current[index] = read_spool(&mut readers[index])?;
        let cursor_bytes = current.iter().flatten().fold(0_u64, |total, candidate| {
            total.saturating_add(record_charge(candidate))
        });
        metrics.peak_buffered_rows = metrics
            .peak_buffered_rows
            .max(u64::try_from(current.iter().flatten().count()).unwrap_or(u64::MAX));
        metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(cursor_bytes);
        if let Some(next) = &current[index] {
            heap.push(Reverse((next.sort_key(), index)));
        }
    }
    if let Some(out) = writer.as_mut() {
        out.flush().map_err(io_error)?;
        let bytes = out.get_ref().metadata().map_err(io_error)?.len();
        metrics.spill_runs = metrics.spill_runs.saturating_add(1);
        metrics.spill_bytes = metrics.spill_bytes.saturating_add(bytes);
    }
    Ok(())
}

fn read_spool(reader: &mut BufReader<File>) -> Result<Option<SpoolRecord>, GfError> {
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(io_error)? == 0 {
        return Ok(None);
    }
    serde_json::from_str(&line).map(Some).map_err(json_error)
}

fn io_error(error: std::io::Error) -> GfError {
    GfError::Storage(format!("property overlay I/O: {error}"))
}

fn json_error(error: serde_json::Error) -> GfError {
    GfError::Storage(format!("property overlay spool: {error}"))
}

/// Enumerate a route's immutable fragments in oldest-to-newest authority order.
///
/// The legacy flat file is admitted only as `(0, 0)`. Every entry in the
/// immutable directory must be a canonical regular file; near misses fail
/// closed instead of disappearing from the authority set.
pub fn enumerate_property_fragments(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<Vec<PropertyFragment>, GfError> {
    validate_route(route)?;
    let root = project.join(kind.subdir());
    let mut fragments = Vec::new();
    let legacy = root.join(format!("{route}.parquet"));
    match fs::symlink_metadata(&legacy) {
        Ok(metadata) if metadata.file_type().is_file() => fragments.push(PropertyFragment {
            id: PropertyFragmentId {
                generation: 0,
                ordinal: 0,
            },
            path: legacy,
        }),
        Ok(_) => return Err(corrupt("legacy property fragment is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(GfError::Storage(error.to_string())),
    }
    let directory = root.join(route);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(fragments),
        Err(error) => return Err(GfError::Storage(error.to_string())),
    };
    for entry in entries {
        let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
        if crate::staging::is_staged_temp_name(&entry.file_name()) {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if !metadata.file_type().is_file() {
            return Err(corrupt("property fragment is not a regular file"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| corrupt("property fragment identity is not canonical UTF-8"))?;
        fragments.push(PropertyFragment {
            id: PropertyFragmentId::parse(&name)?,
            path: entry.path(),
        });
    }
    fragments.sort_unstable_by_key(|fragment| fragment.id);
    if fragments.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(corrupt("duplicate property fragment identity"));
    }
    Ok(fragments)
}

fn validate_route(route: &str) -> Result<(), GfError> {
    if route.is_empty() || route == "." || route == ".." || route.contains(['/', '\0']) {
        return Err(corrupt("property route is not canonical"));
    }
    Ok(())
}

fn corrupt(message: &str) -> GfError {
    GfError::Storage(format!("property overlay: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn fragment_identity_is_numeric_canonical_and_total() {
        let id = PropertyFragmentId {
            generation: 2,
            ordinal: 10,
        };
        assert_eq!(PropertyFragmentId::parse(&id.file_name()).unwrap(), id);
        for invalid in [
            "2-10.parquet",
            "00000000000000000002-00000000000000000010.PARQUET",
            "00000000000000000002-00000000000000000010.parquet.tmp",
            "00000000000000000002-00000000000000000010-0.parquet",
        ] {
            assert!(PropertyFragmentId::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn enumeration_uses_numeric_authority_and_rejects_near_misses() {
        let dir = TempDir::new().unwrap();
        let route = dir.path().join("properties/Person");
        fs::create_dir_all(&route).unwrap();
        for id in [
            PropertyFragmentId {
                generation: 10,
                ordinal: 0,
            },
            PropertyFragmentId {
                generation: 2,
                ordinal: 0,
            },
        ] {
            fs::write(route.join(id.file_name()), b"x").unwrap();
        }
        let found =
            enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").unwrap();
        assert_eq!(found[0].id.generation, 2);
        assert_eq!(found[1].id.generation, 10);
        fs::write(route.join("junk.parquet"), b"x").unwrap();
        assert!(
            enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").is_err()
        );
    }

    #[test]
    fn bounded_external_merge_emits_exact_newest_snapshot_and_tombstone() {
        let dir = TempDir::new().unwrap();
        let (uuid_a, uuid_b, uuid_c) = ([1; 16], [2; 16], [3; 16]);
        let inputs = vec![
            (
                PropertyFragmentId {
                    generation: 1,
                    ordinal: 0,
                },
                101,
                2,
                vec![
                    PropertySnapshotRow {
                        uuid: uuid_a,
                        tombstone: false,
                        values: BTreeMap::from([("name".into(), IrLiteral::Str("old".into()))]),
                    },
                    PropertySnapshotRow {
                        uuid: uuid_b,
                        tombstone: false,
                        values: BTreeMap::from([("keep".into(), IrLiteral::Int(1))]),
                    },
                ],
            ),
            (
                PropertyFragmentId {
                    generation: 2,
                    ordinal: 0,
                },
                202,
                3,
                vec![
                    PropertySnapshotRow {
                        uuid: uuid_a,
                        tombstone: false,
                        values: BTreeMap::from([("name".into(), IrLiteral::Str("new".into()))]),
                    },
                    PropertySnapshotRow {
                        uuid: uuid_c,
                        tombstone: false,
                        values: BTreeMap::new(),
                    },
                ],
            ),
            (
                PropertyFragmentId {
                    generation: 3,
                    ordinal: 0,
                },
                303,
                4,
                vec![PropertySnapshotRow {
                    uuid: uuid_b,
                    tombstone: true,
                    values: BTreeMap::new(),
                }],
            ),
        ];
        let mut rows = Vec::new();
        let metrics = visit_newest_property_snapshots(
            inputs,
            dir.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 1,
                max_open_runs: 2,
                max_buffered_bytes: 1024,
                max_row_bytes: 512,
            },
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.uuid).collect::<Vec<_>>(),
            vec![uuid_a, uuid_c]
        );
        assert_eq!(rows[0].values["name"], IrLiteral::Str("new".into()));
        assert!(rows[1].values.is_empty());
        assert_eq!(metrics.physical_rows, 5);
        assert_eq!(metrics.physical_bytes, 606);
        assert_eq!(metrics.blocks_read, 9);
        assert_eq!(metrics.logical_rows, 2);
        assert_eq!(metrics.shadowed_rows, 2);
        assert_eq!(metrics.tombstones, 1);
        assert!(metrics.spill_runs >= 5);
        assert!(metrics.merge_passes >= 2);
        // One decoded spill row or one cursor per open run, whichever is larger.
        assert_eq!(metrics.peak_buffered_rows, 2);
        assert!(metrics.peak_buffered_bytes > 33);
        assert!(metrics.peak_buffered_bytes < metrics.spill_bytes);
        assert_eq!(metrics.per_record_seeks, 0);
    }

    #[test]
    fn authenticated_reader_reports_actual_io_and_decodes_snapshot() {
        OPEN_FRAGMENT_READERS.store(0, Ordering::SeqCst);
        PEAK_OPEN_FRAGMENT_READERS.store(0, Ordering::SeqCst);
        let dir = TempDir::new().unwrap();
        let scratch = TempDir::new().unwrap();
        let id = PropertyFragmentId {
            generation: 7,
            ordinal: 3,
        };
        let route_dir = dir.path().join("properties/Person");
        fs::create_dir_all(&route_dir).unwrap();
        let metadata = HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "7".into()),
            (PROPERTY_ORDINAL_KEY.into(), "3".into()),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Utf8, true),
            ],
            metadata,
        ));
        let large_name = "A".repeat(100_000);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![vec![4; 16]].into_iter()).unwrap(),
                ) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(large_name.as_str())])) as ArrayRef,
            ],
        )
        .unwrap();
        let path = route_dir.join(id.file_name());
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut rows = Vec::new();
        let metrics = visit_authenticated_property_snapshots(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values.get("name"),
            Some(&IrLiteral::Str(large_name))
        );
        assert_eq!(metrics.physical_rows, 1);
        assert!(metrics.physical_bytes > 0);
        assert!(metrics.blocks_read > 0);
        assert!(metrics.decoder_peak_bytes > 100_000);
        assert!(metrics.peak_buffered_bytes >= metrics.decoder_peak_bytes);
        assert_eq!(metrics.per_record_seeks, 0);
        assert_eq!(OPEN_FRAGMENT_READERS.load(Ordering::SeqCst), 0);
        assert_eq!(PEAK_OPEN_FRAGMENT_READERS.load(Ordering::SeqCst), 1);

        let bytes = fs::read(&path).unwrap();
        let entry = crate::GraphFileEntry {
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        };
        let inventory =
            AuthenticatedPropertyInventory::from_entries_at_root(dir.path(), vec![entry.clone()])
                .unwrap();
        let mut inventory_rows = Vec::new();
        inventory
            .visit_route(
                PropertyRouteKind::Node,
                "Person",
                scratch.path(),
                PropertyOverlayLimits::default(),
                |row| {
                    inventory_rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(inventory_rows.len(), 1);

        let conflicting_id = PropertyFragmentId {
            generation: 8,
            ordinal: 0,
        };
        let conflicting_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Int64, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "8".into()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let conflicting_batch = RecordBatch::try_new(
            Arc::clone(&conflicting_schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![vec![5; 16]].into_iter()).unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(Int64Array::from(vec![Some(9)])),
            ],
        )
        .unwrap();
        let conflicting_path = route_dir.join(conflicting_id.file_name());
        let mut conflicting_writer = ArrowWriter::try_new(
            File::create(&conflicting_path).unwrap(),
            conflicting_schema,
            None,
        )
        .unwrap();
        conflicting_writer.write(&conflicting_batch).unwrap();
        conflicting_writer.close().unwrap();
        let conflicting_bytes = fs::read(&conflicting_path).unwrap();
        let conflicting_entry = crate::GraphFileEntry {
            relative_path: format!("properties/Person/{}", conflicting_id.file_name()),
            byte_length: u64::try_from(conflicting_bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&conflicting_bytes)),
            role: crate::GraphFileRole::Properties,
        };
        assert!(
            AuthenticatedPropertyInventory::from_entries_at_root(
                dir.path(),
                vec![entry, conflicting_entry],
            )
            .is_err()
        );

        fs::write(&path, b"same-name attacker replacement").unwrap();
        assert!(
            inventory
                .visit_route(
                    PropertyRouteKind::Node,
                    "Person",
                    scratch.path(),
                    PropertyOverlayLimits::default(),
                    |_| Ok(()),
                )
                .is_err()
        );
    }
}
