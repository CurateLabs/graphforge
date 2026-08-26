//! Authenticated immutable property snapshot overlays (#940).
//!
//! A fragment row is a complete property snapshot for one UUID. Fragment
//! authority is the numeric `(generation, ordinal)` encoded in its canonical
//! filename; directory order and mtimes never select a winner.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, RecordBatch};
use bytes::Bytes;
use graphforge_core::GfError;
use graphforge_ir::IrLiteral;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use parquet::thrift::TSerializable;
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
    file: Arc<File>,
    length: u64,
    counts: Arc<ReadCounts>,
}

struct CountingRead<R> {
    inner: R,
    counts: Arc<ReadCounts>,
}

struct HeaderRead {
    file: Arc<File>,
    position: u64,
    remaining: usize,
    consumed: Arc<AtomicU64>,
    counts: Arc<ReadCounts>,
}

impl Read for HeaderRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let limit = buffer.len().min(self.remaining);
        if limit == 0 {
            return Ok(0);
        }
        let read = retained_read_at(&self.file, &mut buffer[..limit], self.position)?;
        self.position = self
            .position
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        self.remaining -= read;
        if read != 0 {
            let read = u64::try_from(read).unwrap_or(u64::MAX);
            self.consumed.fetch_add(read, Ordering::Relaxed);
            self.counts.bytes.fetch_add(read, Ordering::Relaxed);
            self.counts.blocks.fetch_add(1, Ordering::Relaxed);
        }
        Ok(read)
    }
}

struct PositionedRead {
    file: Arc<File>,
    position: u64,
}

impl Read for PositionedRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = retained_read_at(&self.file, buffer, self.position)?;
        self.position = self
            .position
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("retained read offset overflow"))?;
        Ok(read)
    }
}

#[cfg(unix)]
fn retained_read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
fn retained_read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
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
    type T = CountingRead<BufReader<PositionedRead>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.counts.range_seeks.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: BufReader::new(PositionedRead {
                file: Arc::clone(&self.file),
                position: start,
            }),
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
    generation_lease: Option<crate::ResolvedProjectGeneration>,
    _root: Option<graphforge_filesystem::StableDirectory>,
    routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>>,
    schemas: BTreeMap<(PropertyRouteKind, String), arrow::datatypes::SchemaRef>,
    authority_bytes: u64,
    authority_blocks: u64,
}

/// One-time I/O performed while admitting and authenticating an immutable
/// property inventory. Cached scans never repeat or re-report this work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyInventoryOpenMetrics {
    /// Bytes read to capture and authenticate inventory authority.
    pub authentication_bytes: u64,
    /// Fixed 64 KiB authentication reads.
    pub authentication_blocks: u64,
}

#[derive(Debug)]
struct AuthenticatedPropertyFragment {
    id: PropertyFragmentId,
    entry: crate::GraphFileEntry,
    _physical_relative: PathBuf,
    file: File,
    physical_rows: usize,
    schema: arrow::datatypes::SchemaRef,
    authentication_bytes: u64,
    authentication_blocks: u64,
}

#[derive(Debug)]
struct RouteSchemaBuilder {
    uuid: arrow::datatypes::FieldRef,
    fields: BTreeMap<String, arrow::datatypes::FieldRef>,
    metadata: HashMap<String, String>,
}

impl AuthenticatedPropertyInventory {
    /// Committed generation retained by this inventory, when generation-backed.
    #[must_use]
    pub fn generation_uuid(&self) -> Option<uuid::Uuid> {
        self.generation_lease
            .as_ref()
            .map(crate::ResolvedProjectGeneration::generation_uuid)
    }

    /// Canonical property routes admitted into this immutable snapshot.
    pub fn routes(&self, kind: PropertyRouteKind) -> impl Iterator<Item = &str> {
        self.routes
            .keys()
            .filter(move |(candidate, _)| *candidate == kind)
            .map(|(_, route)| route.as_str())
    }

    /// Exact one-time admission evidence for this cached inventory.
    #[must_use]
    pub fn open_metrics(&self) -> PropertyInventoryOpenMetrics {
        PropertyInventoryOpenMetrics {
            authentication_bytes: self.authority_bytes.saturating_add(
                self.routes.values().flatten().fold(0_u64, |sum, fragment| {
                    sum.saturating_add(fragment.authentication_bytes)
                }),
            ),
            authentication_blocks: self.authority_blocks.saturating_add(
                self.routes.values().flatten().fold(0_u64, |sum, fragment| {
                    sum.saturating_add(fragment.authentication_blocks)
                }),
            ),
        }
    }

    /// Resolve property authority from a pinned project generation.
    ///
    /// Expanded V1 generations retain files beneath their authenticated graph
    /// tree. Compact V2 generations retain the exact digest-addressed CAS
    /// object named by each authenticated manifest entry.
    pub fn from_resolved_generation(
        generation: &crate::ResolvedProjectGeneration,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route(generation, None)
    }

    /// Admit the private replay workspace of a pinned delta-bearing generation.
    ///
    /// The retained files come from `root`, while the generation lease remains
    /// the immutable authority that owns the verified base plus delta run.
    pub fn from_materialized_generation(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_entries_at_root(root, entries)?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    /// Resolve one route without opening or hashing authenticated entries for
    /// unrelated routes in the pinned generation inventory.
    pub fn from_resolved_generation_for_route(
        generation: &crate::ResolvedProjectGeneration,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route(generation, Some((kind, route)))
    }

    fn from_resolved_generation_route(
        generation: &crate::ResolvedProjectGeneration,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        let Some(participant) = generation.declared_graph_files_participant()? else {
            return Ok(Self {
                _root: None,
                generation_lease: Some(generation.clone()),
                routes: BTreeMap::new(),
                schemas: BTreeMap::new(),
                authority_bytes: 0,
                authority_blocks: 0,
            });
        };
        let inventory = generation.graph_files_inventory()?.ok_or_else(|| {
            corrupt("declared graph-files participant has no authenticated inventory")
        })?;
        let mut admitted = match participant {
            crate::graph_files::GraphFilesParticipant::V1(_) => {
                let root = generation.graph_tree_root();
                let entries = inventory
                    .files
                    .into_iter()
                    .map(|entry| {
                        let relative = PathBuf::from(&entry.relative_path);
                        (entry, relative)
                    })
                    .collect();
                Self::admit_entries(&root, entries, requested_route)
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
                Self::admit_entries(root, entries, requested_route)
            }
        }?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    pub(crate) fn from_entries_at_root(
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
        Self::admit_entries(root, entries, None)
    }

    fn from_entries_at_root_for_route(
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Result<Self, GfError> {
        let entries = entries
            .into_iter()
            .map(|entry| {
                let relative = PathBuf::from(&entry.relative_path);
                (entry, relative)
            })
            .collect();
        Self::admit_entries(root, entries, Some((kind, route)))
    }

    fn admit_entries(
        root_path: &Path,
        entries: Vec<(crate::GraphFileEntry, PathBuf)>,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        let root = graphforge_filesystem::StableDirectory::open(root_path).map_err(io_error)?;
        let mut routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>> =
            BTreeMap::new();
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
            if requested_route.is_some_and(|requested| (kind, route.as_str()) != requested) {
                continue;
            }
            let file = open_retained_under(&root, &physical_relative)?;
            let (authentication_bytes, authentication_blocks) =
                authenticate_inventory_file(&file, &entry)?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file.try_clone().map_err(io_error)?)
                    .map_err(parquet_error)?;
            let physical_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
                .map_err(|_| corrupt("property fragment row count is not representable"))?;
            validate_fragment_schema(builder.schema().as_ref(), id, kind, &route)?;
            let schema = builder.schema().clone();
            let fragments = routes.entry((kind, route)).or_default();
            if fragments.iter().any(|fragment| fragment.id == id) {
                return Err(corrupt("property inventory contains duplicate authority"));
            }
            fragments.push(AuthenticatedPropertyFragment {
                id,
                entry,
                _physical_relative: physical_relative,
                file,
                physical_rows,
                schema,
                authentication_bytes,
                authentication_blocks,
            });
        }
        let mut schemas = BTreeMap::new();
        for ((kind, route), fragments) in &mut routes {
            fragments.sort_unstable_by_key(|fragment| fragment.id);
            validate_fragment_id_sequence(fragments.iter().map(|fragment| fragment.id))?;
            if let Some(newest) = fragments.last() {
                merge_route_schema(&mut schemas, *kind, route, newest.schema.as_ref())?;
            }
        }
        Ok(Self {
            generation_lease: None,
            _root: Some(root),
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
            authority_bytes: 0,
            authority_blocks: 0,
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

    /// Sound upper bound on logical rows for one route.
    ///
    /// The newest-wins merge and tombstones can only remove physical fragment
    /// rows, so their admitted footer counts are a safe planning estimate. It
    /// is deliberately not advertised as an exact logical count.
    #[must_use]
    pub fn route_row_upper_bound(&self, kind: PropertyRouteKind, route: &str) -> usize {
        self.routes
            .get(&(kind, route.to_owned()))
            .into_iter()
            .flatten()
            .fold(0usize, |rows, fragment| {
                rows.saturating_add(fragment.physical_rows)
            })
    }

    /// Visit one authenticated route through the retained generation
    /// capability, opening and closing one Parquet decoder at a time.
    #[allow(
        clippy::too_many_lines,
        reason = "authenticated decoder admission and exact accounting stay co-located"
    )]
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
        let counts = Arc::new(ReadCounts::default());
        let budget = Arc::new(LiveByteBudget::new(limits.max_buffered_bytes));
        let failed = Arc::new(Mutex::new(None));
        let decoded = Arc::new(Mutex::new(DecodedRetention::default()));
        let inputs = fragments.iter().map(|fragment| {
            let reader = (|| {
                let source = CountingChunkReader {
                    length: fragment.entry.byte_length,
                    file: Arc::new(fragment.file.try_clone().map_err(io_error)?),
                    counts: Arc::clone(&counts),
                };
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?;
                validate_fragment_schema(builder.schema().as_ref(), fragment.id, kind, route)?;
                let page_reservation_bytes = validate_parquet_resource_admission(
                    builder.metadata(),
                    limits,
                    &fragment.file,
                    &counts,
                )?;
                budget.charge(page_reservation_bytes)?;
                {
                    let mut retention = decoded.lock().expect("property retention lock");
                    retention.page_peak = retention.page_peak.max(page_reservation_bytes);
                }
                let reader = builder
                    .with_batch_size(admitted_batch_rows(limits))
                    .build()
                    .map_err(parquet_error)?;
                Ok((reader, page_reservation_bytes))
            })();
            let (reader, page_reservation_bytes) = match reader {
                Ok((reader, page_reservation_bytes)) => {
                    #[cfg(test)]
                    {
                        let current = OPEN_FRAGMENT_READERS.fetch_add(1, Ordering::SeqCst) + 1;
                        PEAK_OPEN_FRAGMENT_READERS.fetch_max(current, Ordering::SeqCst);
                    }
                    (Some(reader), page_reservation_bytes)
                }
                Err(error) => {
                    *failed.lock().expect("property failure lock") = Some(error);
                    (None, 0)
                }
            };
            #[cfg(test)]
            let opened = reader.is_some();
            (
                fragment.id,
                0,
                0,
                PropertyParquetRows {
                    reader,
                    current: Vec::new().into_iter(),
                    uuid_field: kind.uuid_field(),
                    failed: Arc::clone(&failed),
                    decoded: Arc::clone(&decoded),
                    budget: Arc::clone(&budget),
                    max_row_bytes: limits.max_row_bytes,
                    page_reservation_bytes,
                    batch_reservation_bytes: limits.max_buffered_bytes / 4,
                    #[cfg(test)]
                    opened,
                },
            )
        });
        let mut metrics =
            visit_newest_property_snapshots(inputs, scratch, limits, budget.as_ref(), emit)?;
        if let Some(error) = failed.lock().expect("property failure lock").take() {
            return Err(error);
        }
        metrics.validation_bytes = counts.bytes.load(Ordering::Relaxed);
        metrics.physical_bytes = metrics.validation_bytes;
        metrics.read_calls = counts.blocks.load(Ordering::Relaxed);
        metrics.validation_read_calls = metrics.read_calls;
        metrics.physical_blocks = metrics.read_calls;
        metrics.range_seeks = counts.range_seeks.load(Ordering::Relaxed);
        let decoded = decoded.lock().expect("property retention lock");
        metrics.decoder_peak_rows = decoded.peak_rows;
        metrics.decoder_peak_bytes = decoded.peak_bytes;
        metrics.decoder_page_reservation_bytes = decoded.page_peak;
        metrics.emitted_batches = decoded.batches;
        metrics.merge_peak_rows = metrics.peak_buffered_rows;
        metrics.merge_peak_bytes = metrics.peak_buffered_bytes;
        metrics.peak_buffered_rows = metrics
            .decoder_peak_rows
            .saturating_add(metrics.merge_peak_rows);
        metrics.peak_buffered_bytes = metrics
            .decoder_peak_bytes
            .saturating_add(metrics.merge_peak_bytes);
        metrics.peak_buffered_bytes = budget.peak();
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
                let compatible_scalar = |data_type: &arrow::datatypes::DataType| {
                    matches!(
                        data_type,
                        arrow::datatypes::DataType::Int64
                            | arrow::datatypes::DataType::Float64
                            | arrow::datatypes::DataType::Boolean
                            | arrow::datatypes::DataType::Utf8
                    ) || data_type
                        == &arrow::datatypes::DataType::Struct(
                            crate::writer::heterogeneous_scalar_fields(),
                        )
                };
                if prior.name() != field.name()
                    || prior.metadata() != field.metadata()
                    || !compatible_scalar(prior.data_type())
                    || !compatible_scalar(field.data_type())
                {
                    return Err(corrupt(
                        "property route field type or semantic metadata conflicts",
                    ));
                }
                schema.fields.insert(
                    field.name().clone(),
                    Arc::new(
                        arrow::datatypes::Field::new(
                            field.name(),
                            arrow::datatypes::DataType::Struct(
                                crate::writer::heterogeneous_scalar_fields(),
                            ),
                            prior.is_nullable() || field.is_nullable(),
                        )
                        .with_metadata(prior.metadata().clone()),
                    ),
                );
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

fn authenticate_inventory_file(
    file: &File,
    entry: &crate::GraphFileEntry,
) -> Result<(u64, u64), GfError> {
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(corrupt(
            "property handle length or kind conflicts with inventory",
        ));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut bytes = 0_u64;
    loop {
        let read = retained_read_at(file, &mut buffer, bytes).map_err(io_error)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).map_err(|_| corrupt("authentication byte overflow"))?)
            .ok_or_else(|| corrupt("authentication byte overflow"))?;
        digest.update(&buffer[..read]);
    }
    if digest_hex(&digest.finalize()) != entry.content_sha256 {
        return Err(corrupt("property handle digest conflicts with inventory"));
    }
    Ok((bytes, bytes.div_ceil(64 * 1024)))
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
    /// Total one-time admission plus decoder bytes read. Cached inventory scans
    /// contain decoder work only.
    pub physical_bytes: u64,
    /// Full-file bytes read exactly once for SHA-256 authentication by a raw
    /// one-shot adapter. Cached inventory scans report zero.
    pub authentication_bytes: u64,
    /// Bytes read while validating canonical UUID/tombstone authority.
    pub validation_bytes: u64,
    /// Bytes read while decoding values from selected row groups.
    pub selected_value_bytes: u64,
    /// Total one-time admission blocks plus decoder reads.
    pub physical_blocks: u64,
    /// Decoder-only non-empty reads, each capped at 64 KiB.
    pub read_calls: u64,
    /// Fixed-size 64 KiB authentication blocks from a raw one-shot adapter.
    pub authentication_blocks: u64,
    /// Read calls used by the validation pass.
    pub validation_read_calls: u64,
    /// Read calls used by selected value decoding.
    pub selected_value_read_calls: u64,
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
    /// Maximum in-memory spill-run path references retained.
    pub peak_run_references: u64,
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
    /// Maximum declared uncompressed page bytes reserved before decode.
    pub decoder_page_reservation_bytes: u64,
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

#[derive(Debug)]
pub(crate) struct LiveByteBudget {
    max: u64,
    state: Mutex<(u64, u64)>,
}

impl LiveByteBudget {
    fn new(max: u64) -> Self {
        Self {
            max,
            state: Mutex::new((0, 0)),
        }
    }

    fn charge(&self, bytes: u64) -> Result<(), GfError> {
        let mut state = self.state.lock().expect("property byte budget lock");
        let next = state
            .0
            .checked_add(bytes)
            .ok_or_else(|| corrupt("property live-byte charge overflows"))?;
        if next > self.max {
            return Err(corrupt("property live-byte budget exceeded"));
        }
        state.0 = next;
        state.1 = state.1.max(next);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let mut state = self.state.lock().expect("property byte budget lock");
        state.0 = state.0.saturating_sub(bytes);
    }

    fn can_charge(&self, bytes: u64) -> bool {
        self.state
            .lock()
            .expect("property byte budget lock")
            .0
            .checked_add(bytes)
            .is_some_and(|next| next <= self.max)
    }

    fn peak(&self) -> u64 {
        self.state.lock().expect("property byte budget lock").1
    }
}

struct PropertyParquetRows {
    reader: Option<ParquetRecordBatchReader>,
    current: std::vec::IntoIter<PropertySnapshotRow>,
    uuid_field: &'static str,
    failed: Arc<Mutex<Option<GfError>>>,
    decoded: Arc<Mutex<DecodedRetention>>,
    budget: Arc<LiveByteBudget>,
    max_row_bytes: u64,
    page_reservation_bytes: u64,
    batch_reservation_bytes: u64,
    #[cfg(test)]
    opened: bool,
}

#[cfg(test)]
static OPEN_FRAGMENT_READERS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static PEAK_OPEN_FRAGMENT_READERS: AtomicU64 = AtomicU64::new(0);

impl Drop for PropertyParquetRows {
    fn drop(&mut self) {
        self.budget.release(self.page_reservation_bytes);
        #[cfg(test)]
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
    page_peak: u64,
}

impl Iterator for PropertyParquetRows {
    type Item = PropertySnapshotRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.next() {
                self.budget.release(snapshot_charge(&row));
                let mut decoded = self.decoded.lock().expect("property retention lock");
                decoded.current_rows = decoded.current_rows.saturating_sub(1);
                decoded.current_bytes = decoded.current_bytes.saturating_sub(snapshot_charge(&row));
                return Some(row);
            }
            self.reader.as_ref()?;
            if let Err(error) = self.budget.charge(self.batch_reservation_bytes) {
                *self.failed.lock().expect("property failure lock") = Some(error);
                return None;
            }
            let Some(next_batch) = self.reader.as_mut().expect("reader checked").next() else {
                self.budget.release(self.batch_reservation_bytes);
                return None;
            };
            let batch = match next_batch {
                Ok(batch) => batch,
                Err(error) => {
                    self.budget.release(self.batch_reservation_bytes);
                    *self.failed.lock().expect("property failure lock") =
                        Some(GfError::Storage(format!("property overlay Arrow: {error}")));
                    return None;
                }
            };
            let arrow_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
            if arrow_bytes > self.batch_reservation_bytes {
                self.budget.release(self.batch_reservation_bytes);
                *self.failed.lock().expect("property failure lock") = Some(corrupt(
                    "property Arrow batch exceeds pre-decode live-byte admission",
                ));
                return None;
            }
            let decode_reservation = self.batch_reservation_bytes;
            if let Err(error) = self.budget.charge(decode_reservation) {
                self.budget.release(self.batch_reservation_bytes);
                *self.failed.lock().expect("property failure lock") = Some(error);
                return None;
            }
            match decode_snapshot_batch(&batch, self.uuid_field) {
                Ok(rows) => {
                    if rows
                        .iter()
                        .any(|row| snapshot_charge(row) > self.max_row_bytes)
                    {
                        self.budget.release(decode_reservation);
                        self.budget.release(self.batch_reservation_bytes);
                        *self.failed.lock().expect("property failure lock") =
                            Some(corrupt("property snapshot row exceeds byte limit"));
                        return None;
                    }
                    let bytes = rows.iter().fold(0_u64, |total, row| {
                        total.saturating_add(snapshot_charge(row))
                    });
                    self.budget.release(decode_reservation);
                    if let Err(error) = self.budget.charge(bytes) {
                        self.budget.release(self.batch_reservation_bytes);
                        *self.failed.lock().expect("property failure lock") = Some(error);
                        return None;
                    }
                    self.budget.release(self.batch_reservation_bytes);
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
                    self.budget.release(decode_reservation);
                    self.budget.release(self.batch_reservation_bytes);
                    *self.failed.lock().expect("property failure lock") = Some(error);
                    return None;
                }
            }
        }
    }
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
    let inventory = authenticated_property_inventory_for_route(project, kind, route)?;
    let open = inventory.open_metrics();
    let mut metrics = inventory.visit_route(kind, route, scratch, limits, emit)?;
    add_open_metrics(&mut metrics, open);
    Ok(metrics)
}

fn add_open_metrics(metrics: &mut PropertyOverlayMetrics, open: PropertyInventoryOpenMetrics) {
    metrics.authentication_bytes = open.authentication_bytes;
    metrics.authentication_blocks = open.authentication_blocks;
    metrics.physical_bytes = metrics
        .physical_bytes
        .saturating_add(open.authentication_bytes);
    metrics.physical_blocks = metrics
        .physical_blocks
        .saturating_add(open.authentication_blocks);
}

pub(crate) fn authenticated_property_inventory_for_route(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        let generation = crate::resolve_project_generation(project)?;
        return AuthenticatedPropertyInventory::from_resolved_generation_for_route(
            &generation,
            kind,
            route,
        );
    }
    let (inventory, _) = crate::capture_graph_files(project)?;
    let authority_bytes = inventory.total_byte_length;
    let authority_blocks = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted = AuthenticatedPropertyInventory::from_entries_at_root_for_route(
        project,
        inventory.files,
        kind,
        route,
    )?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_blocks = authority_blocks;
    Ok(admitted)
}

/// Admit one complete property authority for a raw project tree.
///
/// Catalog construction uses this once and shares the retained inventory with
/// every property provider, avoiding route-count-multiplied authentication.
pub(crate) fn authenticated_property_inventory(
    project: &Path,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        let generation = crate::resolve_project_generation(project)?;
        return AuthenticatedPropertyInventory::from_resolved_generation(&generation);
    }
    let (inventory, _) = crate::capture_graph_files(project)?;
    let authority_bytes = inventory.total_byte_length;
    let authority_blocks = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted =
        AuthenticatedPropertyInventory::from_entries_at_root(project, inventory.files)?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_blocks = authority_blocks;
    Ok(admitted)
}

/// Resolve a bounded UUID batch newest-first without decoding unrelated row
/// groups. Caller order is restored by the returned map lookup.
#[allow(
    clippy::too_many_lines,
    reason = "validation and selected decode share exact counters"
)]
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
    let inventory = authenticated_property_inventory_for_route(project, kind, route)?;
    let open = inventory.open_metrics();
    let (rows, mut metrics) =
        read_authenticated_property_snapshots_for_inventory(&inventory, kind, route, targets)?;
    add_open_metrics(&mut metrics, open);
    Ok((rows, metrics))
}

/// Resolve a bounded UUID batch from an already authenticated generation
/// inventory. This is the mutation-baseline path used by a live session.
pub fn read_authenticated_property_snapshots_for_inventory(
    inventory: &AuthenticatedPropertyInventory,
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
    let Some(fragments) = inventory.routes.get(&(kind, route.to_owned())) else {
        return Ok((found, metrics));
    };
    for fragment in fragments.iter().rev() {
        let counts = Arc::new(ReadCounts::default());
        let builder = open_counted_retained_property_builder(fragment, Arc::clone(&counts))?;
        validate_fragment_schema(builder.schema().as_ref(), fragment.id, kind, route)?;
        let page_reservation_bytes = validate_parquet_resource_admission(
            builder.metadata(),
            PropertyOverlayLimits::default(),
            &fragment.file,
            &counts,
        )?;
        metrics.decoder_page_reservation_bytes = metrics
            .decoder_page_reservation_bytes
            .max(page_reservation_bytes);
        let targeted_batch_rows = admitted_batch_rows(PropertyOverlayLimits::default());
        metrics.row_groups_considered = metrics
            .row_groups_considered
            .saturating_add(u64::try_from(builder.metadata().num_row_groups()).unwrap_or(u64::MAX));
        let row_groups = select_target_row_groups(
            fragment,
            kind,
            &unresolved,
            &counts,
            &mut metrics,
            page_reservation_bytes,
            targeted_batch_rows,
        )?;
        let validation_bytes = counts.bytes.load(Ordering::Relaxed);
        let validation_read_calls = counts.blocks.load(Ordering::Relaxed);
        if !row_groups.is_empty() {
            metrics.row_groups_selected = metrics
                .row_groups_selected
                .saturating_add(u64::try_from(row_groups.len()).unwrap_or(u64::MAX));
            decode_target_row_groups(
                TargetDecodeOptions {
                    fragment,
                    kind,
                    row_groups,
                    batch_rows: targeted_batch_rows,
                    page_reservation_bytes,
                },
                &counts,
                &mut unresolved,
                &mut found,
                &mut metrics,
            )?;
        }
        let total_bytes = counts.bytes.load(Ordering::Relaxed);
        let total_read_calls = counts.blocks.load(Ordering::Relaxed);
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics.physical_bytes.saturating_add(total_bytes);
        metrics.validation_bytes = metrics.validation_bytes.saturating_add(validation_bytes);
        metrics.selected_value_bytes = metrics
            .selected_value_bytes
            .saturating_add(total_bytes.saturating_sub(validation_bytes));
        metrics.read_calls = metrics.read_calls.saturating_add(total_read_calls);
        metrics.validation_read_calls = metrics
            .validation_read_calls
            .saturating_add(validation_read_calls);
        metrics.selected_value_read_calls = metrics
            .selected_value_read_calls
            .saturating_add(total_read_calls.saturating_sub(validation_read_calls));
        metrics.physical_blocks = metrics.physical_blocks.saturating_add(total_read_calls);
        metrics.range_seeks = metrics
            .range_seeks
            .saturating_add(counts.range_seeks.load(Ordering::Relaxed));
    }
    metrics.logical_rows = u64::try_from(found.len()).unwrap_or(u64::MAX);
    metrics.peak_buffered_rows = metrics.decoder_peak_rows;
    Ok((found, metrics))
}

fn select_target_row_groups(
    fragment: &AuthenticatedPropertyFragment,
    kind: PropertyRouteKind,
    unresolved: &std::collections::BTreeSet<[u8; 16]>,
    counts: &Arc<ReadCounts>,
    metrics: &mut PropertyOverlayMetrics,
    page_reservation_bytes: u64,
    targeted_batch_rows: usize,
) -> Result<Vec<usize>, GfError> {
    let builder = open_counted_retained_property_builder(fragment, Arc::clone(counts))?;
    let mut selected_groups = Vec::new();
    let mut prior_uuid = None;
    for index in 0..builder.metadata().num_row_groups() {
        let validation = open_counted_retained_property_builder(fragment, Arc::clone(counts))?
            .with_row_groups(vec![index])
            .with_batch_size(targeted_batch_rows)
            .build()
            .map_err(parquet_error)?;
        let mut selected = false;
        for batch in validation {
            let batch = batch.map_err(|error| GfError::Storage(error.to_string()))?;
            charge_target_batch(metrics, &batch, page_reservation_bytes)?;
            let uuids = batch
                .column_by_name(kind.uuid_field())
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| corrupt("property UUID column has wrong physical type"))?;
            let tombstones = batch
                .column_by_name(PROPERTY_TOMBSTONE_FIELD)
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| corrupt("property tombstone column is not boolean"))
                })
                .transpose()?;
            if tombstones.is_none() && fragment.id.generation != 0 {
                return Err(corrupt("property snapshot fragment lacks tombstone field"));
            }
            if uuids.null_count() != 0 || tombstones.is_some_and(|values| values.null_count() != 0)
            {
                return Err(corrupt("property identity columns contain null slots"));
            }
            for row in 0..batch.num_rows() {
                let uuid: [u8; 16] = uuids
                    .value(row)
                    .try_into()
                    .map_err(|_| corrupt("property UUID value has wrong width"))?;
                if prior_uuid.is_some_and(|prior| prior >= uuid) {
                    return Err(corrupt(
                        "property fragment UUIDs are not strictly sorted and unique",
                    ));
                }
                prior_uuid = Some(uuid);
                selected |= !unresolved.is_empty() && unresolved.contains(&uuid);
                if tombstones.is_some_and(|values| values.value(row))
                    && batch
                        .columns()
                        .iter()
                        .skip(2)
                        .any(|column| !column.is_null(row))
                {
                    return Err(corrupt("property tombstone carries values"));
                }
            }
            metrics.physical_rows = metrics
                .physical_rows
                .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        }
        if selected {
            selected_groups.push(index);
        }
    }
    Ok(selected_groups)
}

struct TargetDecodeOptions<'a> {
    fragment: &'a AuthenticatedPropertyFragment,
    kind: PropertyRouteKind,
    row_groups: Vec<usize>,
    batch_rows: usize,
    page_reservation_bytes: u64,
}

fn decode_target_row_groups(
    options: TargetDecodeOptions<'_>,
    counts: &Arc<ReadCounts>,
    unresolved: &mut std::collections::BTreeSet<[u8; 16]>,
    found: &mut BTreeMap<[u8; 16], PropertySnapshotRow>,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<(), GfError> {
    let reader = open_counted_retained_property_builder(options.fragment, Arc::clone(counts))?
        .with_row_groups(options.row_groups)
        .with_batch_size(options.batch_rows)
        .build()
        .map_err(parquet_error)?;
    for batch in reader {
        let batch = batch.map_err(|error| GfError::Storage(error.to_string()))?;
        charge_target_batch(metrics, &batch, options.page_reservation_bytes)?;
        let decoded = decode_snapshot_batch(&batch, options.kind.uuid_field())?;
        if decoded
            .iter()
            .any(|row| snapshot_charge(row) > PropertyOverlayLimits::default().max_row_bytes)
        {
            return Err(corrupt("property snapshot row exceeds byte limit"));
        }
        metrics.decoder_peak_bytes = metrics
            .decoder_peak_bytes
            .max(decoded.iter().map(snapshot_charge).sum::<u64>());
        for row in decoded {
            metrics.physical_rows = metrics.physical_rows.saturating_add(1);
            if unresolved.remove(&row.uuid) {
                if row.tombstone {
                    metrics.tombstones = metrics.tombstones.saturating_add(1);
                } else {
                    found.insert(row.uuid, row);
                }
            }
        }
    }
    Ok(())
}

fn charge_target_batch(
    metrics: &mut PropertyOverlayMetrics,
    batch: &RecordBatch,
    page_reservation_bytes: u64,
) -> Result<(), GfError> {
    let limits = PropertyOverlayLimits::default();
    let arrow_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let decoded_reservation = limits
        .max_row_bytes
        .saturating_mul(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
    if page_reservation_bytes
        .checked_add(arrow_bytes)
        .and_then(|bytes| bytes.checked_add(decoded_reservation))
        .is_none_or(|bytes| bytes > limits.max_buffered_bytes)
    {
        return Err(corrupt("targeted property decode exceeds live-byte budget"));
    }
    metrics.emitted_batches = metrics.emitted_batches.saturating_add(1);
    metrics.decoder_peak_rows = metrics
        .decoder_peak_rows
        .max(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
    metrics.decoder_peak_bytes = metrics.decoder_peak_bytes.max(arrow_bytes);
    metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(
        page_reservation_bytes
            .saturating_add(arrow_bytes)
            .saturating_add(decoded_reservation),
    );
    Ok(())
}

fn open_counted_retained_property_builder(
    fragment: &AuthenticatedPropertyFragment,
    counts: Arc<ReadCounts>,
) -> Result<ParquetRecordBatchReaderBuilder<CountingChunkReader>, GfError> {
    ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file: Arc::new(fragment.file.try_clone().map_err(io_error)?),
        length: fragment.entry.byte_length,
        counts,
    })
    .map_err(parquet_error)
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

#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw compact-Thrift page type and sizes only here"
)]
#[allow(
    clippy::too_many_lines,
    reason = "raw page parsing and aggregate pre-decode admission form one proof"
)]
fn validate_parquet_resource_admission(
    metadata: &parquet::file::metadata::ParquetMetaData,
    limits: PropertyOverlayLimits,
    file: &File,
    counts: &Arc<ReadCounts>,
) -> Result<u64, GfError> {
    const MAX_PAGE_HEADER_BYTES: usize = 64 * 1024;
    let max_page_bytes = limits.max_buffered_bytes / 4;
    if max_page_bytes == 0 {
        return Err(corrupt("property page byte budget is too small"));
    }
    let mut largest_group_exposure = 0_u64;
    for group in metadata.row_groups() {
        let mut group_exposure = 0_u64;
        for column in group.columns() {
            let mut dictionary_exposure = 0_u64;
            let mut data_exposure = 0_u64;
            let _uncompressed = u64::try_from(column.uncompressed_size())
                .map_err(|_| corrupt("property column chunk has negative uncompressed size"))?;
            let compressed = u64::try_from(column.compressed_size())
                .map_err(|_| corrupt("property column chunk has negative compressed size"))?;
            let data_offset = u64::try_from(column.data_page_offset())
                .map_err(|_| corrupt("property column chunk has negative data-page offset"))?;
            let start = column
                .dictionary_page_offset()
                .map(|offset| {
                    u64::try_from(offset)
                        .map_err(|_| corrupt("property dictionary page has negative offset"))
                })
                .transpose()?
                .map_or(data_offset, |offset| offset.min(data_offset));
            let end = start
                .checked_add(compressed)
                .ok_or_else(|| corrupt("property column chunk range overflows"))?;
            if end > file.metadata().map_err(io_error)?.len() {
                return Err(corrupt("property column chunk escapes authenticated file"));
            }
            let mut position = start;
            while position < end {
                let consumed = Arc::new(AtomicU64::new(0));
                let transport = HeaderRead {
                    file: Arc::new(file.try_clone().map_err(io_error)?),
                    position,
                    remaining: MAX_PAGE_HEADER_BYTES,
                    consumed: Arc::clone(&consumed),
                    counts: Arc::clone(counts),
                };
                let mut protocol = thrift::protocol::TCompactInputProtocol::new(transport);
                #[allow(deprecated, reason = "Parquet 58 exposes raw page headers only here")]
                let header = parquet::format::PageHeader::read_from_in_protocol(&mut protocol)
                    .map_err(|_| corrupt("property page header is malformed or oversized"))?;
                let header_bytes = consumed.load(Ordering::Relaxed);
                #[allow(deprecated, reason = "Parquet 58 page admission requires raw sizes")]
                let compressed_page = u64::try_from(header.compressed_page_size)
                    .map_err(|_| corrupt("property page has negative compressed size"))?;
                #[allow(deprecated, reason = "Parquet 58 page admission requires raw sizes")]
                let uncompressed_page = u64::try_from(header.uncompressed_page_size)
                    .map_err(|_| corrupt("property page has negative uncompressed size"))?;
                if header_bytes == 0
                    || uncompressed_page > max_page_bytes
                    || compressed_page > compressed
                {
                    return Err(corrupt("property page exceeds pre-decode byte admission"));
                }
                if header.type_ == parquet::format::PageType::DICTIONARY_PAGE {
                    dictionary_exposure = dictionary_exposure.max(uncompressed_page);
                } else {
                    data_exposure = data_exposure.max(uncompressed_page);
                }
                position = position
                    .checked_add(header_bytes)
                    .and_then(|offset| offset.checked_add(compressed_page))
                    .ok_or_else(|| corrupt("property page range overflows"))?;
                if position > end {
                    return Err(corrupt("property page escapes its column chunk"));
                }
            }
            if position != end {
                return Err(corrupt(
                    "property page sequence does not cover its column chunk",
                ));
            }
            group_exposure = group_exposure
                .checked_add(data_exposure)
                .and_then(|bytes| {
                    dictionary_exposure
                        .checked_mul(u64::try_from(admitted_batch_rows(limits)).ok()?)
                        .and_then(|decoded_dictionary| bytes.checked_add(decoded_dictionary))
                })
                .and_then(|bytes| {
                    // Validity, offsets, and values buffers are live together.
                    // Sixteen bytes/value/column deliberately over-reserves the
                    // fixed Arrow bookkeeping before the builder allocates it.
                    u64::try_from(admitted_batch_rows(limits))
                        .ok()?
                        .checked_mul(16)
                        .and_then(|overhead| bytes.checked_add(overhead))
                })
                .ok_or_else(|| corrupt("property projected page exposure overflows"))?;
            if group_exposure > max_page_bytes {
                return Err(corrupt(
                    "property projected pages exceed pre-decode live-byte admission",
                ));
            }
        }
        largest_group_exposure = largest_group_exposure.max(group_exposure);
    }
    Ok(largest_group_exposure)
}

fn admitted_batch_rows(limits: PropertyOverlayLimits) -> usize {
    let row_budget = limits.max_buffered_bytes / 4;
    let byte_limited = usize::try_from(row_budget / limits.max_row_bytes)
        .unwrap_or(usize::MAX)
        .max(1);
    limits.max_buffered_rows.clamp(1, 4096).min(byte_limited)
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
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

#[derive(Debug, Default)]
struct RunLevels {
    levels: Vec<Vec<PathBuf>>,
    next_ordinal: u64,
}

impl RunLevels {
    fn add<F>(
        &mut self,
        root: &Path,
        mut level: usize,
        mut path: PathBuf,
        fan_in: usize,
        budget: &LiveByteBudget,
        metrics: &mut PropertyOverlayMetrics,
    ) -> Result<(), GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        loop {
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, Vec::new);
            }
            self.levels[level].push(path);
            metrics.peak_run_references = metrics.peak_run_references.max(
                u64::try_from(self.levels.iter().map(Vec::len).sum::<usize>()).unwrap_or(u64::MAX),
            );
            if self.levels[level].len() < fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            path = root.join(format!("level-{level}-{}.jsonl", self.next_ordinal));
            self.next_ordinal = self
                .next_ordinal
                .checked_add(1)
                .ok_or_else(|| corrupt("property run ordinal overflow"))?;
            merge_runs::<F>(&inputs, &path, None, budget, metrics)?;
            for input in inputs {
                fs::remove_file(input).map_err(io_error)?;
            }
            metrics.merge_passes = metrics.merge_passes.saturating_add(1);
            level = level
                .checked_add(1)
                .ok_or_else(|| corrupt("property run level overflow"))?;
        }
    }

    fn finish(self) -> Vec<PathBuf> {
        self.levels.into_iter().flatten().collect()
    }
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
#[allow(
    clippy::too_many_lines,
    reason = "bounded merge accounting stays co-located"
)]
pub(crate) fn visit_newest_property_snapshots<I, R, F>(
    inputs: I,
    scratch: &Path,
    limits: PropertyOverlayLimits,
    budget: &LiveByteBudget,
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
    let mut runs = RunLevels::default();
    let mut next_run = 0_usize;
    let mut buffered_bytes = 0_u64;
    for (id, physical_bytes, read_calls, rows) in inputs {
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics.physical_bytes.saturating_add(physical_bytes);
        metrics.read_calls = metrics.read_calls.saturating_add(read_calls);
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
                && (buffered_bytes
                    .checked_add(charge)
                    .is_none_or(|bytes| bytes > limits.max_buffered_bytes)
                    || !budget.can_charge(charge))
            {
                let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
                budget.release(buffered_bytes);
                next_run = next_run
                    .checked_add(1)
                    .ok_or_else(|| corrupt("property run ordinal overflow"))?;
                runs.add::<F>(
                    temp.path(),
                    0,
                    run,
                    limits.max_open_runs,
                    budget,
                    &mut metrics,
                )?;
                buffered_bytes = 0;
            }
            buffered_bytes = buffered_bytes
                .checked_add(charge)
                .ok_or_else(|| corrupt("property snapshot byte charge overflows"))?;
            budget.charge(charge)?;
            buffer.push(record);
            metrics.peak_buffered_rows = metrics
                .peak_buffered_rows
                .max(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
            metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(buffered_bytes);
            if buffer.len() == limits.max_buffered_rows {
                let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
                budget.release(buffered_bytes);
                next_run = next_run
                    .checked_add(1)
                    .ok_or_else(|| corrupt("property run ordinal overflow"))?;
                runs.add::<F>(
                    temp.path(),
                    0,
                    run,
                    limits.max_open_runs,
                    budget,
                    &mut metrics,
                )?;
                buffered_bytes = 0;
            }
        }
    }
    if !buffer.is_empty() {
        let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
        budget.release(buffered_bytes);
        runs.add::<F>(
            temp.path(),
            0,
            run,
            limits.max_open_runs,
            budget,
            &mut metrics,
        )?;
    }
    let mut runs = runs.finish();
    while runs.len() > limits.max_open_runs {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(limits.max_open_runs).enumerate() {
            let path = temp
                .path()
                .join(format!("pass-{}-{group}.jsonl", metrics.merge_passes));
            merge_runs::<F>(chunk, &path, None, budget, &mut metrics)?;
            next.push(path);
            for input in chunk {
                fs::remove_file(input).map_err(io_error)?;
            }
        }
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
        runs = next;
    }
    if !runs.is_empty() {
        merge_runs(
            &runs,
            &temp.path().join("final.jsonl"),
            Some(&mut emit),
            budget,
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
    budget: &LiveByteBudget,
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
        current.push(read_spool(reader, budget.max)?);
        if let Some(row) = &current[index] {
            heap.push(Reverse((row.sort_key(), index)));
        }
    }
    let cursor_bytes = current
        .iter()
        .flatten()
        .fold(0_u64, |total, row| total.saturating_add(record_charge(row)));
    budget.charge(cursor_bytes)?;
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
        budget.release(record_charge(&row));
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
        current[index] = read_spool(&mut readers[index], budget.max)?;
        if let Some(next) = &current[index] {
            budget.charge(record_charge(next))?;
        }
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

fn read_spool(
    reader: &mut BufReader<File>,
    max_encoded_bytes: u64,
) -> Result<Option<SpoolRecord>, GfError> {
    let mut line = String::new();
    let read = reader
        .take(max_encoded_bytes.saturating_add(1))
        .read_line(&mut line)
        .map_err(io_error)?;
    if read == 0 {
        return Ok(None);
    }
    if u64::try_from(read).unwrap_or(u64::MAX) > max_encoded_bytes || !line.ends_with('\n') {
        return Err(corrupt("property spill record exceeds byte limit"));
    }
    serde_json::from_str(&line).map(Some).map_err(json_error)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
fn io_error(error: std::io::Error) -> GfError {
    GfError::Storage(format!("property overlay I/O: {error}"))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
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
    validate_fragment_id_sequence(fragments.iter().map(|fragment| fragment.id))?;
    Ok(fragments)
}

fn validate_fragment_id_sequence(
    ids: impl IntoIterator<Item = PropertyFragmentId>,
) -> Result<(), GfError> {
    let mut prior: Option<PropertyFragmentId> = None;
    for id in ids {
        if let Some(previous) = prior {
            if id.generation == previous.generation
                && Some(id.ordinal) != previous.ordinal.checked_add(1)
            {
                return Err(corrupt("property fragment ordinal sequence has a gap"));
            }
            if id.generation != previous.generation && id.ordinal != 0 {
                return Err(corrupt(
                    "property fragment generation does not start at ordinal zero",
                ));
            }
        } else if id.generation != 0 && id.ordinal != 0 {
            return Err(corrupt(
                "property fragment generation does not start at ordinal zero",
            ));
        }
        prior = Some(id);
    }
    Ok(())
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
    use std::collections::{BTreeSet, HashMap};
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
    fn route_inventory_rejects_mixed_authority_and_noncanonical_ordinals() {
        let mixed = TempDir::new().unwrap();
        fs::create_dir_all(mixed.path().join("properties/Person")).unwrap();
        fs::write(mixed.path().join("properties/Person.parquet"), b"legacy").unwrap();
        fs::write(
            mixed.path().join("properties/Person").join(
                PropertyFragmentId {
                    generation: 1,
                    ordinal: 0,
                }
                .file_name(),
            ),
            b"immutable",
        )
        .unwrap();
        let migrated =
            enumerate_property_fragments(mixed.path(), PropertyRouteKind::Node, "Person").unwrap();
        assert_eq!(migrated.len(), 2);
        assert_eq!(
            migrated[0].id,
            PropertyFragmentId {
                generation: 0,
                ordinal: 0,
            }
        );
        assert_eq!(migrated[1].id.generation, 1);

        let gap = TempDir::new().unwrap();
        fs::create_dir_all(gap.path().join("properties/Person")).unwrap();
        fs::write(
            gap.path().join("properties/Person").join(
                PropertyFragmentId {
                    generation: 4,
                    ordinal: 1,
                }
                .file_name(),
            ),
            b"gap",
        )
        .unwrap();
        assert!(
            enumerate_property_fragments(gap.path(), PropertyRouteKind::Node, "Person")
                .unwrap_err()
                .to_string()
                .contains("ordinal zero")
        );
    }

    #[test]
    fn authenticated_fragment_sequence_rejects_mixed_gapped_and_nonzero_starts() {
        let id = |generation, ordinal| PropertyFragmentId {
            generation,
            ordinal,
        };
        assert!(validate_fragment_id_sequence([id(0, 0), id(1, 0)]).is_ok());
        assert!(validate_fragment_id_sequence([id(7, 1)]).is_err());
        assert!(validate_fragment_id_sequence([id(7, 0), id(7, 2)]).is_err());
        assert!(validate_fragment_id_sequence([id(7, 0), id(8, 1)]).is_err());
        assert!(validate_fragment_id_sequence([id(7, 0), id(7, 1), id(8, 0)]).is_ok());
        assert!(
            validate_fragment_id_sequence([id(7, u64::MAX - 1), id(7, u64::MAX), id(7, 0),])
                .is_err()
        );
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
        let budget = LiveByteBudget::new(1024);
        let metrics = visit_newest_property_snapshots(
            inputs,
            dir.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 1,
                max_open_runs: 2,
                max_buffered_bytes: 1024,
                max_row_bytes: 512,
            },
            &budget,
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
        assert_eq!(metrics.read_calls, 9);
        assert_eq!(metrics.logical_rows, 2);
        assert_eq!(metrics.shadowed_rows, 2);
        assert_eq!(metrics.tombstones, 1);
        assert!(metrics.spill_runs >= 5);
        assert!(metrics.peak_run_references <= 3);
        assert!(metrics.merge_passes >= 2);
        // One decoded spill row or one cursor per open run, whichever is larger.
        assert_eq!(metrics.peak_buffered_rows, 2);
        assert!(metrics.peak_buffered_bytes > 33);
        assert!(metrics.peak_buffered_bytes < metrics.spill_bytes);
        assert_eq!(metrics.per_record_seeks, 0);
    }

    #[test]
    fn rolling_fan_in_keeps_run_references_logarithmic() {
        let dir = TempDir::new().unwrap();
        let rows = (0_u16..1024)
            .map(|value| {
                let mut uuid = [0_u8; 16];
                uuid[14..].copy_from_slice(&value.to_be_bytes());
                PropertySnapshotRow {
                    uuid,
                    tombstone: false,
                    values: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();
        let budget = LiveByteBudget::new(1024);
        let metrics = visit_newest_property_snapshots(
            [(
                PropertyFragmentId {
                    generation: 1,
                    ordinal: 0,
                },
                0,
                0,
                rows,
            )],
            dir.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 1,
                max_open_runs: 2,
                max_buffered_bytes: 1024,
                max_row_bytes: 512,
            },
            &budget,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(metrics.physical_rows, 1024);
        assert!(metrics.spill_runs > 1024);
        assert!(metrics.peak_run_references <= 11);
        assert!(budget.peak() <= 1024);
    }

    #[test]
    fn authenticated_reader_reports_actual_io_and_decodes_snapshot() {
        OPEN_FRAGMENT_READERS.store(0, Ordering::SeqCst);
        PEAK_OPEN_FRAGMENT_READERS.store(0, Ordering::SeqCst);
        let dir = TempDir::new().unwrap();
        let scratch = TempDir::new().unwrap();
        let id = PropertyFragmentId {
            generation: 7,
            ordinal: 0,
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
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
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
        assert!(metrics.read_calls > 0);
        assert!(metrics.decoder_peak_bytes > 100_000);
        assert!(metrics.peak_buffered_bytes >= metrics.decoder_peak_bytes);
        assert_eq!(metrics.per_record_seeks, 0);
        assert_eq!(OPEN_FRAGMENT_READERS.load(Ordering::SeqCst), 0);
        assert_eq!(PEAK_OPEN_FRAGMENT_READERS.load(Ordering::SeqCst), 1);
        let error = visit_authenticated_property_snapshots(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 8,
                max_open_runs: 2,
                max_buffered_bytes: 1024,
                max_row_bytes: 512,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("pre-decode byte admission"),
            "{error}"
        );

        let bytes = fs::read(&path).unwrap();
        let entry = crate::GraphFileEntry {
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        };
        let inventory = Arc::new(
            AuthenticatedPropertyInventory::from_entries_at_root(dir.path(), vec![entry.clone()])
                .unwrap(),
        );
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
        // Schema discovery and every targeted mutation baseline reuse this one
        // admitted inventory; neither operation resolves CURRENT nor captures
        // the project tree again.
        assert!(
            inventory
                .route_schema(PropertyRouteKind::Node, "Person")
                .is_some()
        );
        let targets = BTreeSet::from([[4; 16]]);
        let open_metrics = inventory.open_metrics();
        assert!(open_metrics.authentication_bytes > 0);
        let mut repeated_scan_metrics = Vec::new();
        for _ in 0..2 {
            let (rows, metrics) = read_authenticated_property_snapshots_for_inventory(
                &inventory,
                PropertyRouteKind::Node,
                "Person",
                &targets,
            )
            .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(metrics.per_record_seeks, 0);
            assert_eq!(metrics.authentication_bytes, 0);
            assert_eq!(metrics.authentication_blocks, 0);
            assert_eq!(
                metrics.physical_bytes,
                metrics.validation_bytes + metrics.selected_value_bytes
            );
            repeated_scan_metrics.push(metrics);
        }
        assert_eq!(repeated_scan_metrics[0], repeated_scan_metrics[1]);
        let concurrent = (0..8)
            .map(|_| {
                let inventory = Arc::clone(&inventory);
                std::thread::spawn(move || {
                    let scratch = TempDir::new().unwrap();
                    for _ in 0..8 {
                        let mut rows = Vec::new();
                        inventory
                            .visit_route(
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
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in concurrent {
            thread.join().unwrap();
        }

        let unrelated_dir = dir.path().join("properties/Unrelated");
        fs::create_dir_all(&unrelated_dir).unwrap();
        let unrelated_path = unrelated_dir.join(id.file_name());
        let unrelated_bytes = b"authenticated but deliberately not parquet";
        fs::write(&unrelated_path, unrelated_bytes).unwrap();
        let unrelated_entry = crate::GraphFileEntry {
            relative_path: format!("properties/Unrelated/{}", id.file_name()),
            byte_length: u64::try_from(unrelated_bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(unrelated_bytes)),
            role: crate::GraphFileRole::Properties,
        };
        let selected = AuthenticatedPropertyInventory::from_entries_at_root_for_route(
            dir.path(),
            vec![entry.clone(), unrelated_entry],
            PropertyRouteKind::Node,
            "Person",
        )
        .unwrap();
        let selected_metrics = selected
            .visit_route(
                PropertyRouteKind::Node,
                "Person",
                scratch.path(),
                PropertyOverlayLimits::default(),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            selected.open_metrics().authentication_bytes,
            entry.byte_length
        );
        assert_eq!(selected_metrics.authentication_bytes, 0);
        assert_eq!(selected_metrics.authentication_blocks, 0);

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
        let evolved = AuthenticatedPropertyInventory::from_entries_at_root(
            dir.path(),
            vec![entry, conflicting_entry],
        );
        let evolved = evolved.unwrap();
        assert_eq!(
            evolved
                .route_schema(PropertyRouteKind::Node, "Person")
                .unwrap()
                .field_with_name("name")
                .unwrap()
                .data_type(),
            &DataType::Struct(crate::writer::heterogeneous_scalar_fields())
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

    #[test]
    fn retained_reader_rejects_wide_projected_pages_before_arrow_allocation() {
        let dir = TempDir::new().unwrap();
        let scratch = TempDir::new().unwrap();
        let route_dir = dir.path().join("properties/Wide");
        fs::create_dir_all(&route_dir).unwrap();
        let id = PropertyFragmentId {
            generation: 1,
            ordinal: 0,
        };
        let mut fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
        ];
        let mut columns = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![7; 16]].into_iter()).unwrap())
                as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ];
        let value = "x".repeat(256);
        for index in 0..32 {
            fields.push(Field::new(
                format!("value_{index:02}"),
                DataType::Utf8,
                true,
            ));
            columns.push(Arc::new(StringArray::from(vec![Some(value.as_str())])) as ArrayRef);
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Wide".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "1".into()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let path = route_dir.join(id.file_name());
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let error = visit_authenticated_property_snapshots(
            dir.path(),
            PropertyRouteKind::Node,
            "Wide",
            scratch.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 8,
                max_open_runs: 2,
                max_buffered_bytes: 16 * 1024,
                max_row_bytes: 12 * 1024,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("projected pages exceed"),
            "{error}"
        );
    }

    #[test]
    fn targeted_reader_validates_unselected_rows_before_value_pruning() {
        for (uuids, tombstones, names, expected) in [
            (
                vec![vec![2; 16], vec![1; 16]],
                vec![false, false],
                vec![Some("target"), Some("out-of-order")],
                "strictly sorted",
            ),
            (
                vec![vec![1; 16], vec![2; 16]],
                vec![false, true],
                vec![Some("target"), Some("forbidden")],
                "tombstone carries values",
            ),
        ] {
            let dir = TempDir::new().unwrap();
            let route_dir = dir.path().join("properties/Person");
            fs::create_dir_all(&route_dir).unwrap();
            let id = PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            };
            let schema = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                    Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, true),
                ],
                HashMap::from([
                    (
                        PROPERTY_OVERLAY_FORMAT_KEY.into(),
                        PROPERTY_OVERLAY_FORMAT.into(),
                    ),
                    (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                    (PROPERTY_KIND_KEY.into(), "node".into()),
                    (PROPERTY_GENERATION_KEY.into(), "1".into()),
                    (PROPERTY_ORDINAL_KEY.into(), "0".into()),
                ]),
            ));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.into_iter()).unwrap()),
                    Arc::new(BooleanArray::from(tombstones)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                File::create(route_dir.join(id.file_name())).unwrap(),
                schema,
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let error = read_authenticated_property_snapshots_for(
                dir.path(),
                PropertyRouteKind::Node,
                "Person",
                &BTreeSet::from([[1; 16]]),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn targeted_reader_validates_corrupt_older_fragment_after_target_resolves() {
        let dir = TempDir::new().unwrap();
        let route_dir = dir.path().join("properties/Person");
        fs::create_dir_all(&route_dir).unwrap();
        for (generation, uuids) in [(1, vec![[2; 16], [1; 16]]), (2, vec![[9; 16]])] {
            let id = PropertyFragmentId {
                generation,
                ordinal: 0,
            };
            let schema = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                    Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, true),
                ],
                HashMap::from([
                    (
                        PROPERTY_OVERLAY_FORMAT_KEY.into(),
                        PROPERTY_OVERLAY_FORMAT.into(),
                    ),
                    (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                    (PROPERTY_KIND_KEY.into(), "node".into()),
                    (PROPERTY_GENERATION_KEY.into(), generation.to_string()),
                    (PROPERTY_ORDINAL_KEY.into(), "0".into()),
                ]),
            ));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|uuid| uuid.to_vec()))
                            .unwrap(),
                    ),
                    Arc::new(BooleanArray::from(vec![false; uuids.len()])),
                    Arc::new(StringArray::from(vec![Some("value"); uuids.len()])),
                ],
            )
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                File::create(route_dir.join(id.file_name())).unwrap(),
                schema,
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let error = read_authenticated_property_snapshots_for(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            &BTreeSet::from([[9; 16]]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("strictly sorted"), "{error}");
    }

    #[test]
    #[allow(
        deprecated,
        reason = "hostile raw PageHeader regression for Parquet 58"
    )]
    fn retained_reader_rejects_footer_small_page_header_large_before_decode() {
        use std::io::{Cursor, Seek, SeekFrom};

        let dir = TempDir::new().unwrap();
        let route_dir = dir.path().join("properties/Person");
        fs::create_dir_all(&route_dir).unwrap();
        let id = PropertyFragmentId {
            generation: 1,
            ordinal: 0,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Utf8, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "1".into()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![vec![7; 16]].into_iter()).unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(StringArray::from(vec![Some("small")])),
            ],
        )
        .unwrap();
        let path = route_dir.join(id.file_name());
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let column = &builder.metadata().row_group(0).columns()[0];
        let offset = u64::try_from(column.data_page_offset()).unwrap();
        let footer_uncompressed = column.uncompressed_size();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut raw = vec![0_u8; 64 * 1024];
        let read = file.read(&mut raw).unwrap();
        raw.truncate(read);
        let mut cursor = Cursor::new(raw.as_slice());
        let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
        let original = parquet::format::PageHeader::read_from_in_protocol(&mut protocol).unwrap();
        drop(protocol);
        let header_len = usize::try_from(cursor.position()).unwrap();
        let mut hostile = original.clone();
        let mut encoded = Vec::new();
        for candidate in (footer_uncompressed + 1)..=(footer_uncompressed + 4096) {
            hostile.uncompressed_page_size = i32::try_from(candidate).unwrap();
            encoded.clear();
            let mut output = thrift::protocol::TCompactOutputProtocol::new(&mut encoded);
            hostile.write_to_out_protocol(&mut output).unwrap();
            if encoded.len() == header_len {
                break;
            }
        }
        assert_eq!(encoded.len(), header_len);
        let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&encoded).unwrap();
        file.flush().unwrap();

        let error = visit_authenticated_property_snapshots(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            dir.path(),
            PropertyOverlayLimits {
                max_buffered_rows: 8,
                max_open_runs: 2,
                max_buffered_bytes: u64::try_from(footer_uncompressed).unwrap() * 3,
                max_row_bytes: 32,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("page exceeds"), "{error}");
    }

    #[test]
    fn targeted_reader_n_2n_4n_has_bounded_retention_and_exact_work() {
        let mut prior_bytes = 0;
        let mut byte_deltas = Vec::new();
        let mut prior_peak = None;
        for rows in [128_usize, 256, 512] {
            let dir = TempDir::new().unwrap();
            let route_dir = dir.path().join("properties/Person");
            fs::create_dir_all(&route_dir).unwrap();
            let id = PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            };
            let schema = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                    Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, true),
                ],
                HashMap::from([
                    (
                        PROPERTY_OVERLAY_FORMAT_KEY.into(),
                        PROPERTY_OVERLAY_FORMAT.into(),
                    ),
                    (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                    (PROPERTY_KIND_KEY.into(), "node".into()),
                    (PROPERTY_GENERATION_KEY.into(), "1".into()),
                    (PROPERTY_ORDINAL_KEY.into(), "0".into()),
                ]),
            ));
            let uuids = (0..rows)
                .map(|value| {
                    let mut uuid = [0_u8; 16];
                    uuid[14..].copy_from_slice(&u16::try_from(value).unwrap().to_be_bytes());
                    uuid
                })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|uuid| uuid.to_vec()))
                            .unwrap(),
                    ),
                    Arc::new(BooleanArray::from(vec![false; rows])),
                    Arc::new(StringArray::from(vec![Some("value"); rows])),
                ],
            )
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                File::create(route_dir.join(id.file_name())).unwrap(),
                schema,
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let expected_authentication_bytes =
                fs::metadata(route_dir.join(id.file_name())).unwrap().len();

            let (found, metrics) = read_authenticated_property_snapshots_for(
                dir.path(),
                PropertyRouteKind::Node,
                "Person",
                &BTreeSet::from([uuids[0]]),
            )
            .unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(metrics.physical_rows, u64::try_from(rows * 2).unwrap());
            assert_eq!(metrics.fragments_considered, 1);
            // Raw graph-tree adapters first capture/hash the authority and
            // then authenticate the selected retained fragment.
            assert_eq!(
                metrics.authentication_bytes,
                expected_authentication_bytes * 2
            );
            assert_eq!(
                metrics.authentication_blocks,
                expected_authentication_bytes.div_ceil(64 * 1024) * 2
            );
            assert_eq!(metrics.row_groups_considered, 1);
            assert_eq!(metrics.row_groups_selected, 1);
            assert_eq!(metrics.decoder_peak_rows, 2);
            assert_eq!(metrics.peak_buffered_rows, 2);
            assert!(
                metrics.peak_buffered_bytes <= PropertyOverlayLimits::default().max_buffered_bytes
            );
            if let Some(prior) = prior_peak {
                assert!(metrics.peak_buffered_bytes <= prior + 64 * 1024);
            }
            prior_peak = Some(metrics.peak_buffered_bytes);
            assert!(metrics.physical_bytes > prior_bytes);
            assert_eq!(
                metrics.physical_bytes,
                metrics.authentication_bytes
                    + metrics.validation_bytes
                    + metrics.selected_value_bytes
            );
            assert!(metrics.read_calls > 0);
            assert_eq!(
                metrics.physical_blocks,
                metrics.authentication_blocks
                    + metrics.validation_read_calls
                    + metrics.selected_value_read_calls
            );
            assert!(metrics.validation_bytes > 0);
            assert!(metrics.selected_value_bytes > 0);
            if prior_bytes != 0 {
                byte_deltas.push(metrics.physical_bytes - prior_bytes);
                assert!(
                    metrics.physical_bytes <= prior_bytes.saturating_mul(2).saturating_add(16_384),
                    "doubling rows must remain linear within fixed Parquet metadata tolerance"
                );
            }
            assert_eq!(metrics.per_record_seeks, 0);
            prior_bytes = metrics.physical_bytes;
        }
        assert_eq!(byte_deltas.len(), 2);
        assert!(
            byte_deltas[1] <= byte_deltas[0].saturating_mul(2).saturating_add(8_192),
            "first differences must reject superlinear repeated reads"
        );
    }
}
