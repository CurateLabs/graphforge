//! Authenticated immutable property snapshot overlays (#940).
//!
//! A fragment row is a complete property snapshot for one UUID. Fragment
//! authority is the numeric `(generation, ordinal)` encoded in its canonical
//! filename; directory order and mtimes never select a winner.

pub(crate) mod fragment_cap;
pub(crate) use fragment_cap::{
    FragmentSplitter, row_charges, split_into_fragments, with_fragment_ordinal,
};
pub use fragment_cap::{MAX_PROPERTY_FRAGMENT_BYTES, MAX_PROPERTY_FRAGMENT_ROWS};
pub(crate) mod bounded_object;
pub use bounded_object::MAX_PROPERTY_OBJECT_BYTES;
mod inventory;
#[cfg(test)]
use inventory::digest_hex;
pub use inventory::enumerate_property_fragments;
pub(crate) use inventory::{
    authenticated_property_inventory, authenticated_property_inventory_for_rewrite,
    authenticated_property_inventory_for_rewrite_route, authenticated_property_inventory_for_route,
    merge_property_route_schemas, rename_live_schema_summary, update_live_route_schema,
};
mod projected_reads;
pub(crate) use projected_reads::decode_snapshot_batch;
use projected_reads::{authenticated_arrow_error, parquet_error};
pub use projected_reads::{
    read_authenticated_property_snapshots_for, read_authenticated_property_snapshots_for_inventory,
    visit_authenticated_property_snapshots,
};
mod targeted_reads;
use targeted_reads::read_property_targets;
pub use targeted_reads::{
    EdgeOwnerProbeWork, PropertyTargetData, PropertyTargetSnapshots,
    read_authenticated_property_presence_for_inventory,
    read_authenticated_property_snapshot_data_for_inventory,
    read_authenticated_property_target_data_for_inventory,
    read_authenticated_property_targets_for_inventory, resolve_existing_edge_property_owner_data,
    resolve_existing_edge_property_owners,
};
pub(crate) use targeted_reads::{
    read_property_presence_data_for_inventory, read_replay_property_targets,
};
mod parquet_budget;
pub(crate) use parquet_budget::replay_parquet_reader_reservation;
use parquet_budget::{
    CountingChunkReader, ReadCounts, TargetReadAdmission, admit_target_footer, admitted_batch_rows,
    charge_target_batch, open_counted_retained_property_builder, parquet_resource_admission,
    replay_decoder_limit, retained_read_at, validate_fragment_schema,
    validate_parquet_resource_admission,
};
mod snapshot_merge;
use snapshot_merge::{io_error, json_error};
pub(crate) use snapshot_merge::{snapshot_charge, visit_newest_property_snapshots};

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, RecordBatch};
use bytes::Bytes;
use graphforge_core::GfError;
#[cfg(test)]
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use graphforge_ir::IrLiteral;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use parquet::thrift::TSerializable;
#[cfg(test)]
use sha2::Digest;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
pub(crate) const PROPERTY_LIVE_SCHEMA_KEY: &str = "graphforge.property_live_schema";
const PROPERTY_LIVE_SCHEMA_FORMAT: &str = "graphforge-property-live-schema/1";

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

    pub(crate) fn uuid_field(self) -> &'static str {
        match self {
            Self::Node => "node_uuid",
            Self::Edge => "edge_uuid",
        }
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
    root: Option<graphforge_filesystem::StableDirectory>,
    root_path: Option<PathBuf>,
    routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>>,
    edge_routes: BTreeMap<String, Vec<AdmittedEdgeFile>>,
    /// Node topology files the inventory declares (`topology/nodes.parquet`
    /// and `topology/nodes/<range>.parquet`), in canonical order. `None` for
    /// a route-scoped inventory, which carries no topology authority; a reader
    /// then falls back to the directory listing. #1388: the node table reads
    /// only declared files, so an unregistered file in the hydrated directory
    /// is never opened.
    node_files: Option<Vec<AdmittedEdgeFile>>,
    /// Semantic owners that have no fragment yet; routes with fragments are
    /// described by `route_summaries`.
    schemas: BTreeMap<(PropertyRouteKind, String), arrow::datatypes::SchemaRef>,
    /// Footer-derived schema and row bound of each route that has fragments.
    /// Filled at admission when fragments are authenticated eagerly, and on the
    /// first touch of the route when they are admitted lazily.
    route_summaries: BTreeMap<(PropertyRouteKind, String), OnceLock<RouteSummaryOutcome>>,
    /// Node-topology objects named by the manifest, with their declared length
    /// and XXH64. Present only when the inventory covers the whole graph rather
    /// than one requested route.
    node_topology_files: Option<Vec<crate::catalog::AdmittedSourceFile>>,
    authority_bytes: u64,
    authority_block_equivalents: u64,
    authority_read_calls: u64,
    #[cfg(test)]
    handle_counts: Arc<FragmentHandleCounts>,
    #[cfg(test)]
    late_decoder_failure_row_countdown: Arc<AtomicU64>,
    #[cfg(test)]
    mutation_barrier: Mutex<Option<Arc<TestMutationBarrier>>>,
}

#[derive(Debug)]
struct AdmittedEdgeFile {
    path: PathBuf,
    relative_path: String,
}

/// One-time I/O performed while admitting and authenticating an immutable
/// property inventory. Cached scans never repeat or re-report this work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyInventoryOpenMetrics {
    /// Bytes read once while capturing the complete raw graph-files authority.
    pub(crate) authority_authentication_bytes: u64,
    /// 64 KiB block-equivalents covering raw graph-files authority.
    pub(crate) authority_authentication_block_equivalents: u64,
    /// Actual non-empty reads used to capture raw graph-files authority.
    pub(crate) authority_authentication_read_calls: u64,
    /// Bytes read while authenticating retained property fragments.
    pub(crate) property_authentication_bytes: u64,
    /// 64 KiB block-equivalents covering retained property fragments.
    pub(crate) property_authentication_block_equivalents: u64,
    /// Actual non-empty reads used to authenticate retained property fragments.
    pub(crate) property_authentication_read_calls: u64,
    /// Bytes read to capture and authenticate inventory authority.
    pub authentication_bytes: u64,
    /// 64 KiB authentication block-equivalents.
    pub authentication_block_equivalents: u64,
    /// Actual non-empty authentication reads.
    pub authentication_read_calls: u64,
}

/// When a fragment's bytes are checked against the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FragmentAdmission {
    /// At admission, and again by a private snapshot copy on every read. Raw
    /// workspaces, whose files writers replace in place of an immutable object.
    Eager,
    /// Admission checks each part's exact length only. Content (exact length
    /// and XXH64) is checked when the fragment's footer is first needed.
    /// Payload reads always authenticate a private snapshot, including reads
    /// after successful footer admission.
    FirstTouch,
}

/// Facts of one fragment's physical objects that only their bytes carry: the
/// bounded-object envelope and the length of the logical Parquet stream.
#[derive(Debug, Clone, Copy)]
struct FragmentObject {
    envelope: Option<bounded_object::EnvelopeLayout>,
    logical_length: u64,
}

/// Footer facts of one fragment's logical Parquet, which the manifest does not
/// carry.
#[derive(Debug)]
struct FragmentFooter {
    physical_rows: usize,
    schema: arrow::datatypes::SchemaRef,
}

/// Schema of one route, derived from its fragments' footers.
#[derive(Debug)]
struct RouteSummary {
    schema: arrow::datatypes::SchemaRef,
}

type RouteSummaryOutcome = Result<Arc<RouteSummary>, GfError>;

#[derive(Debug)]
struct AuthenticatedPropertyFragment {
    id: PropertyFragmentId,
    layout: PropertyFragmentLayout,
    entry: crate::GraphReadFileEntry,
    physical_relative: PathBuf,
    identity: graphforge_filesystem::FileIdentity,
    /// Every physical object of the fragment, anchor first, as the manifest
    /// names them. Known without reading any of them.
    parts: Vec<PropertyObjectPart>,
    /// Set at admission when fragments are admitted eagerly; otherwise set by
    /// the first touch, which checks every part's content against the manifest.
    object: OnceLock<Result<FragmentObject, GfError>>,
    footer: OnceLock<Result<FragmentFooter, GfError>>,
    authentication_bytes: u64,
    authentication_block_equivalents: u64,
    authentication_read_calls: u64,
}

#[derive(Debug, Clone)]
struct PropertyObjectPart {
    entry: crate::GraphReadFileEntry,
    physical_relative: PathBuf,
    identity: graphforge_filesystem::FileIdentity,
}

#[derive(Debug, Default)]
struct PartAuthentication {
    bytes: AtomicU64,
    blocks: AtomicU64,
    calls: AtomicU64,
    read_bytes: AtomicU64,
    read_calls: AtomicU64,
}

impl PartAuthentication {
    fn add(&self, bytes: u64, blocks: u64, calls: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.blocks.fetch_add(blocks, Ordering::Relaxed);
        self.calls.fetch_add(calls, Ordering::Relaxed);
    }

    fn values(&self) -> (u64, u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.blocks.load(Ordering::Relaxed),
            self.calls.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug)]
enum PropertyFile {
    Plain(File),
    Segmented {
        source: bounded_object::SegmentedSource,
        authentication: Arc<PartAuthentication>,
    },
}

impl PropertyFile {
    fn authentication(&self) -> (u64, u64, u64) {
        match self {
            Self::Plain(_) => (0, 0, 0),
            Self::Segmented { authentication, .. } => authentication.values(),
        }
    }

    fn physical_reads(&self) -> (u64, u64) {
        match self {
            Self::Plain(_) => (0, 0),
            Self::Segmented { authentication, .. } => (
                authentication.read_bytes.load(Ordering::Relaxed),
                authentication.read_calls.load(Ordering::Relaxed),
            ),
        }
    }

    fn reservation_bytes(&self) -> u64 {
        match self {
            Self::Plain(_) => 0,
            Self::Segmented { .. } => 2 * bounded_object::MAX_PROPERTY_OBJECT_BYTES as u64,
        }
    }
}

trait PropertyRead: std::fmt::Debug {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize>;
    fn length(&self) -> std::io::Result<u64>;
    fn physical(&self) -> bool {
        true
    }
}

impl PropertyRead for File {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
        retained_read_at(self, buffer, offset)
    }
    fn length(&self) -> std::io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}

impl PropertyRead for PropertyFile {
    fn physical(&self) -> bool {
        matches!(self, Self::Plain(_))
    }
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::Plain(file) => retained_read_at(file, buffer, offset),
            Self::Segmented { source, .. } => source.read_at(buffer, offset),
        }
    }
    fn length(&self) -> std::io::Result<u64> {
        match self {
            Self::Plain(file) => file.length(),
            Self::Segmented { source, .. } => Ok(source.len()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyFragmentLayout {
    LegacyFlat,
    CanonicalNested,
}

/// One live, revalidated fragment capability. The inventory retains only the
/// directory capability and immutable identity/digest authority; this guard
/// keeps the corresponding OS handle scoped to one decoder.
struct OpenPropertyFragment {
    file: Arc<PropertyFile>,
    /// Length of the logical Parquet stream `file` presents.
    logical_length: u64,
    authentication_bytes: u64,
    authentication_block_equivalents: u64,
    authentication_read_calls: u64,
    handle: FragmentHandleGuard,
}

/// Streaming logical property fragment decoder retaining authenticated capabilities.
pub(crate) struct PropertyFragmentBatches {
    reader: ParquetRecordBatchReader,
    _opened: OpenPropertyFragment,
}

impl Iterator for PropertyFragmentBatches {
    type Item = Result<RecordBatch, GfError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader
            .next()
            .map(|batch| batch.map_err(authenticated_arrow_error))
    }
}

struct FragmentHandleGuard {
    #[cfg(test)]
    counts: Arc<FragmentHandleCounts>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct FragmentHandleCounts {
    live: AtomicU64,
    peak: AtomicU64,
}

#[cfg(test)]
#[derive(Debug)]
struct TestMutationBarrier {
    authenticated: std::sync::Barrier,
    proceed: std::sync::Barrier,
    copied: std::sync::Barrier,
    restored: std::sync::Barrier,
}

impl FragmentHandleGuard {
    fn acquired(#[cfg(test)] counts: &Arc<FragmentHandleCounts>) -> Self {
        #[cfg(test)]
        {
            let current = counts.live.fetch_add(1, Ordering::SeqCst) + 1;
            counts.peak.fetch_max(current, Ordering::SeqCst);
        }
        Self {
            #[cfg(test)]
            counts: Arc::clone(counts),
        }
    }
}

impl Drop for FragmentHandleGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        self.counts.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Exact work performed by one property overlay operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyOverlayMetrics {
    /// Physical snapshot rows decoded.
    pub physical_rows: u64,
    /// Total authentication plus decoder bytes read.
    pub physical_bytes: u64,
    /// Full-file bytes read for checksum admission and control authentication. Cached inventories
    /// stream each bounded on-demand handle into an authenticated immutable snapshot.
    pub authentication_bytes: u64,
    /// Raw graph-files authority bytes included in authentication bytes.
    pub authority_authentication_bytes: u64,
    /// Retained property-fragment bytes included in authentication bytes.
    pub property_authentication_bytes: u64,
    /// Bytes durably written to immutable authenticated decode snapshots.
    pub authenticated_snapshot_bytes: u64,
    /// Largest single immutable snapshot that had to coexist with its source.
    pub authenticated_snapshot_peak_bytes: u64,
    /// Bytes read while validating canonical UUID/tombstone authority.
    pub validation_bytes: u64,
    /// Bytes read while decoding values from selected row groups.
    pub selected_value_bytes: u64,
    /// Total actual non-empty authentication plus decoder reads.
    pub physical_blocks: u64,
    /// Decoder-only non-empty reads, each capped at 64 KiB.
    pub read_calls: u64,
    /// 64 KiB authentication block-equivalents from a raw one-shot adapter.
    pub authentication_block_equivalents: u64,
    /// Actual non-empty authentication reads from a raw one-shot adapter.
    pub authentication_read_calls: u64,
    /// Raw graph-files authority block-equivalents included in authentication blocks.
    pub authority_authentication_block_equivalents: u64,
    /// Actual non-empty raw graph-files authority reads.
    pub authority_authentication_read_calls: u64,
    /// Retained property-fragment block-equivalents included in authentication blocks.
    pub property_authentication_block_equivalents: u64,
    /// Actual non-empty retained property-fragment authentication reads.
    pub property_authentication_read_calls: u64,
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
    /// Encoded bytes written into first-level spill runs before merge
    /// amplification. Includes one newline delimiter per physical row.
    pub spool_input_bytes: u64,
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

fn corrupt(message: &str) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
        message: format!("property overlay: {message}"),
    }
}

pub(crate) trait IntoPropertySnapshotResult {
    fn into_property_snapshot_result(self) -> Result<PropertySnapshotRow, GfError>;
}
