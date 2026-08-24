//! Persistent, bounded-memory UUID membership indexes used by bulk ingest.
//!
//! The canonical graph remains Parquet.  This derived format is deliberately
//! small: one manifest (published last with an atomic replacement) names an
//! immutable base plus size-tiered delta runs. Each run contains a unified
//! UUID-sorted identity file and a node-only surrogate-sorted reverse file.
//! Readers verify version, topology generation, framing, canonical ordering,
//! counts, and SHA-256 before serving bounded binary-search probes.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const FORMAT_VERSION: u32 = 3;
const NODE_LOOKUP_RECORD_BYTES: u64 = 24;
const IDENTITY_RECORD_BYTES: u64 = 32;
const BULK_IO_BYTES: usize = 1 << 20;
const INDEX_DIR: &str = ".graphforge-cache/uuid-membership";
const MANIFEST: &str = "manifest.json";

fn storage_err(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("UUID membership index: {error}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Selects the canonical identity domain to probe.
pub enum UuidIndexKind {
    /// Canonical node UUIDs.
    Node,
    /// Canonical edge UUIDs.
    Edge,
}

#[derive(Clone, Copy, Debug)]
/// Hard work limits for a bounded index build.
pub struct UuidIndexBuildLimits {
    /// Maximum Parquet rows decoded per scan batch.
    pub scan_batch_rows: usize,
    /// Maximum UUID records held by one sort run.
    pub run_records: usize,
    /// Maximum run files opened by one merge group.
    pub merge_fan_in: usize,
}

impl Default for UuidIndexBuildLimits {
    fn default() -> Self {
        Self {
            scan_batch_rows: 8_192,
            run_records: 65_536,
            merge_fan_in: 32,
        }
    }
}

impl UuidIndexBuildLimits {
    fn validate(self) -> Result<Self, GfError> {
        if self.scan_batch_rows == 0 || self.run_records == 0 || self.merge_fan_in < 2 {
            return Err(storage_err(
                "build limits must be non-zero and merge_fan_in >= 2",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Aggregate-only build evidence; it never contains graph identities.
pub struct UuidIndexBuildMetrics {
    /// Unique node identities written.
    pub node_count: u64,
    /// Unique edge identities written.
    pub edge_count: u64,
    /// Maximum UUID records simultaneously held by the sorter.
    pub peak_buffered_records: usize,
    /// Number of temporary sort and merge runs produced.
    pub temporary_runs: u64,
}

/// Aggregate-only evidence for one incremental v3 run publication.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UuidIndexAppendMetrics {
    /// New identity and tombstone records accepted by this publication.
    pub input_records: u64,
    /// Prior canonical topology rows decoded; ordinary append requires zero.
    pub prior_topology_rows_decoded: u64,
    /// Immutable authenticated runs retained after publication.
    pub retained_runs: usize,
    /// Exact physical bytes written for run and manifest outputs.
    pub physical_bytes_written: u64,
    /// Bulk output blocks submitted to the filesystem.
    pub write_blocks: u64,
    /// Bytes submitted through those bulk output blocks.
    pub write_bytes: u64,
    /// Maximum fixed-width records buffered at once.
    pub peak_buffered_records: usize,
    /// Maximum charged fixed-width buffer bytes at once.
    pub peak_buffered_bytes: usize,
    /// Sequential retained-run bytes examined for cross-run uniqueness.
    pub validation_scan_bytes: u64,
    /// One-MiB read blocks covering `validation_scan_bytes`.
    pub validation_scan_blocks: u64,
    /// Bulk append validation never performs per-key random seeks.
    pub validation_random_seeks: u64,
    /// Full-run bytes authenticated once when admitting a new retained snapshot.
    pub snapshot_admission_authentication_bytes: u64,
    /// Full-run blocks authenticated during snapshot admission.
    pub snapshot_admission_authentication_blocks: u64,
    /// Newly installed run bytes authenticated while advancing the retained snapshot.
    pub new_output_authentication_bytes: u64,
    /// Newly installed authenticated run blocks.
    pub new_output_authentication_blocks: u64,
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
    /// Binary-search file seeks performed.
    pub file_seeks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FileRecord {
    name: String,
    count: u64,
    sha256: String,
    blocks: Vec<BlockRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BlockRecord {
    offset: u64,
    len: u32,
    first_key: String,
    last_key: String,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RunRecord {
    base: bool,
    level: u8,
    first_generation: u64,
    last_generation: u64,
    identities: FileRecord,
    node_surrogates: FileRecord,
    node_count: u64,
    edge_count: u64,
    #[serde(default)]
    deleted_node_count: u64,
    #[serde(default)]
    deleted_edge_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Manifest {
    format_version: u32,
    base_generation: u64,
    current_generation: u64,
    #[serde(default)]
    live_node_count: u64,
    #[serde(default)]
    live_edge_count: u64,
    runs: Vec<RunRecord>,
}

#[derive(Debug)]
struct OpenRun {
    identities: File,
    node_surrogates: File,
    descriptor: RunRecord,
}

#[derive(Debug)]
struct AuthenticatedRun {
    identities: File,
    identities_identity: graphforge_filesystem::FileIdentity,
    node_surrogates: File,
    node_surrogates_identity: graphforge_filesystem::FileIdentity,
    descriptor: RunRecord,
}

#[derive(Debug, Default)]
struct ProbeBlockCache {
    file: Option<graphforge_filesystem::FileIdentity>,
    offset: u64,
    bytes: Vec<u8>,
}

fn cached_record<const N: usize>(
    file: &mut File,
    identity: graphforge_filesystem::FileIdentity,
    index: u64,
    cache: &mut ProbeBlockCache,
    block_loads: &mut u64,
) -> Result<[u8; N], GfError> {
    let byte_offset = index
        .checked_mul(N as u64)
        .ok_or_else(|| storage_err("probe offset overflow"))?;
    let block_offset = byte_offset / BULK_IO_BYTES as u64 * BULK_IO_BYTES as u64;
    if cache.file != Some(identity) || cache.offset != block_offset {
        file.seek(SeekFrom::Start(block_offset))
            .map_err(storage_err)?;
        cache.bytes.resize(BULK_IO_BYTES, 0);
        let mut valid = 0;
        while valid < BULK_IO_BYTES {
            let read = file.read(&mut cache.bytes[valid..]).map_err(storage_err)?;
            if read == 0 {
                break;
            }
            valid += read;
        }
        cache.bytes.truncate(valid);
        cache.file = Some(identity);
        cache.offset = block_offset;
        *block_loads = block_loads.saturating_add(1);
    }
    let within = usize::try_from(byte_offset - block_offset).map_err(storage_err)?;
    let end = within
        .checked_add(N)
        .ok_or_else(|| storage_err("probe range overflow"))?;
    cache
        .bytes
        .get(within..end)
        .ok_or_else(|| storage_err("truncated cached UUID record"))?
        .try_into()
        .map_err(|_| storage_err("invalid cached UUID record"))
}

fn cached_identity_state(
    file: &mut File,
    identity: graphforge_filesystem::FileIdentity,
    count: u64,
    target: Uuid,
    expected_kind: UuidIndexKind,
    cache: &mut ProbeBlockCache,
    loads: &mut u64,
) -> Result<Option<(bool, u64)>, GfError> {
    let (mut lo, mut hi) = (0, count);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let record = cached_record::<32>(file, identity, mid, cache, loads)?;
        match record[..16].cmp(target.as_bytes()) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                let kind = if matches!(record[16], 0 | 2) {
                    UuidIndexKind::Node
                } else {
                    UuidIndexKind::Edge
                };
                if kind != expected_kind {
                    return Ok(Some((false, 0)));
                }
                return Ok(Some((
                    matches!(record[16], 0 | 1),
                    u64::from_be_bytes(record[24..32].try_into().expect("fixed")),
                )));
            }
        }
    }
    Ok(None)
}

fn authenticated_block(
    file: &mut File,
    block: &BlockRecord,
    width: usize,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<Vec<u8>, GfError> {
    file.seek(SeekFrom::Start(block.offset))
        .map_err(storage_err)?;
    let mut bytes = vec![0_u8; block.len as usize];
    file.read_exact(&mut bytes).map_err(storage_err)?;
    let key_width = if width == IDENTITY_RECORD_BYTES as usize {
        16
    } else {
        8
    };
    if hex_sha256(&bytes) != block.sha256
        || hex_sha256_key(&bytes[..key_width]) != block.first_key
        || hex_sha256_key(&bytes[bytes.len() - width..bytes.len() - width + key_width])
            != block.last_key
    {
        return Err(storage_err("UUID probe block authentication failed"));
    }
    metrics.validation_scan_bytes = metrics
        .validation_scan_bytes
        .saturating_add(bytes.len() as u64);
    metrics.validation_scan_blocks = metrics.validation_scan_blocks.saturating_add(1);
    Ok(bytes)
}

fn candidate_block(record: &FileRecord, key: &str) -> Option<usize> {
    let index = record
        .blocks
        .partition_point(|block| block.last_key.as_str() < key);
    record
        .blocks
        .get(index)
        .filter(|block| block.first_key.as_str() <= key)
        .map(|_| index)
}

fn block_record_index(bytes: &[u8], width: usize, key: &[u8]) -> Option<usize> {
    let key_width = key.len();
    let (mut low, mut high) = (0, bytes.len() / width);
    while low < high {
        let middle = low + (high - low) / 2;
        let record = &bytes[middle * width..(middle + 1) * width];
        match record[..key_width].cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Some(middle),
        }
    }
    None
}

fn reject_retained_identity_collisions(
    run: &mut AuthenticatedRun,
    incoming: &[(Uuid, u8, u64)],
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut groups = std::collections::BTreeMap::<usize, Vec<&(Uuid, u8, u64)>>::new();
    for item in incoming {
        let key = hex_sha256_key(item.0.as_bytes());
        if let Some(index) = candidate_block(&run.descriptor.identities, &key) {
            groups.entry(index).or_default().push(item);
        }
    }
    for (index, items) in groups {
        let bytes = authenticated_block(
            &mut run.identities,
            &run.descriptor.identities.blocks[index],
            IDENTITY_RECORD_BYTES as usize,
            metrics,
        )?;
        for (uuid, kind, surrogate) in items {
            let found = block_record_index(&bytes, IDENTITY_RECORD_BYTES as usize, uuid.as_bytes())
                .map(|at| &bytes[at * 32..at * 32 + 32]);
            if let Some(record) = found {
                let retained_kind = record[16];
                let retained_surrogate =
                    u64::from_be_bytes(record[24..32].try_into().expect("fixed"));
                let same_domain = matches!((*kind, retained_kind), (0 | 2, 0 | 2) | (1 | 3, 1 | 3));
                let deletion_matches = matches!(kind, 2 | 3)
                    && matches!(retained_kind, 0 | 1)
                    && retained_surrogate == *surrogate;
                if same_domain && !deletion_matches {
                    return Err(storage_err(
                        "UUID already exists in an authenticated retained run",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn reject_retained_surrogate_collisions(
    run: &mut AuthenticatedRun,
    incoming: &[(u64, Uuid)],
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut groups = std::collections::BTreeMap::<usize, Vec<&(u64, Uuid)>>::new();
    for item in incoming {
        let key = hex_sha256_key(&item.0.to_be_bytes());
        if let Some(index) = candidate_block(&run.descriptor.node_surrogates, &key) {
            groups.entry(index).or_default().push(item);
        }
    }
    for (index, items) in groups {
        let bytes = authenticated_block(
            &mut run.node_surrogates,
            &run.descriptor.node_surrogates.blocks[index],
            NODE_LOOKUP_RECORD_BYTES as usize,
            metrics,
        )?;
        for (surrogate, uuid) in items {
            let key = surrogate.to_be_bytes();
            if let Some(at) = block_record_index(&bytes, NODE_LOOKUP_RECORD_BYTES as usize, &key) {
                let record = &bytes[at * 24..at * 24 + 24];
                if record[8..] != *uuid.as_bytes() {
                    return Err(storage_err(
                        "node surrogate already exists in an authenticated retained run",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
/// An authenticated node-and-edge index snapshot pinned by one manifest.
pub struct UuidMembershipIndex {
    runs: Vec<OpenRun>,
    manifest: Manifest,
}

/// Long-lived authenticated UUID-index snapshot retained by construction writers.
#[derive(Debug)]
pub struct AuthenticatedUuidIndexSnapshot {
    root: graphforge_filesystem::StableDirectory,
    root_identity: graphforge_filesystem::FileIdentity,
    manifest_file: File,
    manifest_identity: graphforge_filesystem::FileIdentity,
    manifest_sha256: String,
    manifest: Manifest,
    runs: Vec<AuthenticatedRun>,
    cache: ProbeBlockCache,
    authenticated_bytes: u64,
    authenticated_blocks: u64,
}

impl AuthenticatedUuidIndexSnapshot {
    pub(crate) fn open_at_generation(project_dir: &Path, generation: u64) -> Result<Self, GfError> {
        let root_path = project_dir.join(INDEX_DIR);
        let root = graphforge_filesystem::StableDirectory::open(&root_path).map_err(storage_err)?;
        let root_identity = root.identity();
        let mut manifest_file = root
            .open_child_file(std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        let manifest_identity =
            graphforge_filesystem::file_identity(&manifest_file).map_err(storage_err)?;
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        let manifest_sha256 = hex_sha256(&body);
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION || manifest.current_generation != generation {
            return Err(storage_err("authenticated snapshot generation is stale"));
        }
        validate_run_descriptors(&manifest)?;
        let mut authenticated_bytes = body.len() as u64;
        let mut authenticated_blocks = 1_u64;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        for descriptor in &manifest.runs {
            let identities =
                open_verified_at(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_verified_at(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            authenticated_bytes = authenticated_bytes
                .saturating_add(descriptor.identities.count * IDENTITY_RECORD_BYTES)
                .saturating_add(descriptor.node_surrogates.count * NODE_LOOKUP_RECORD_BYTES);
            authenticated_blocks = authenticated_blocks
                .saturating_add(descriptor.identities.blocks.len() as u64)
                .saturating_add(descriptor.node_surrogates.blocks.len() as u64);
            runs.push(AuthenticatedRun {
                identities_identity: graphforge_filesystem::file_identity(&identities)
                    .map_err(storage_err)?,
                node_surrogates_identity: graphforge_filesystem::file_identity(&node_surrogates)
                    .map_err(storage_err)?,
                identities,
                node_surrogates,
                descriptor: descriptor.clone(),
            });
        }
        Ok(Self {
            root,
            root_identity,
            manifest_file,
            manifest_identity,
            manifest_sha256,
            manifest,
            runs,
            cache: ProbeBlockCache::default(),
            authenticated_bytes,
            authenticated_blocks,
        })
    }

    pub(crate) fn topology_generation(&self) -> u64 {
        self.manifest.current_generation
    }
    fn take_authentication_work(&mut self) -> (u64, u64) {
        (
            std::mem::take(&mut self.authenticated_bytes),
            std::mem::take(&mut self.authenticated_blocks),
        )
    }

    fn revalidate(&self) -> Result<(), GfError> {
        self.root.revalidate_named().map_err(storage_err)?;
        if self.root.identity() != self.root_identity {
            return Err(storage_err("UUID index root identity changed"));
        }
        if graphforge_filesystem::file_identity(&self.manifest_file).map_err(storage_err)?
            != self.manifest_identity
            || graphforge_filesystem::file_link_count(&self.manifest_file).map_err(storage_err)?
                != 1
            || self.manifest_sha256.len() != 64
        {
            return Err(storage_err("retained UUID manifest identity changed"));
        }
        let mut named_manifest = self
            .root
            .open_child_file(std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        if graphforge_filesystem::file_identity(&named_manifest).map_err(storage_err)?
            != self.manifest_identity
            || graphforge_filesystem::file_link_count(&named_manifest).map_err(storage_err)? != 1
        {
            return Err(storage_err("UUID manifest identity changed"));
        }
        let body = read_bounded(&mut named_manifest, MAX_MANIFEST_BYTES)?;
        if hex_sha256(&body) != self.manifest_sha256
            || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
        {
            return Err(storage_err("UUID manifest authentication changed"));
        }
        for run in &self.runs {
            for (record, identity) in [
                (&run.descriptor.identities, run.identities_identity),
                (
                    &run.descriptor.node_surrogates,
                    run.node_surrogates_identity,
                ),
            ] {
                let named = self
                    .root
                    .open_child_file(std::ffi::OsStr::new(&record.name))
                    .map_err(storage_err)?;
                if graphforge_filesystem::file_identity(&named).map_err(storage_err)? != identity
                    || graphforge_filesystem::file_link_count(&named).map_err(storage_err)? != 1
                {
                    return Err(storage_err("UUID retained run identity changed"));
                }
            }
        }
        Ok(())
    }

    fn advance_to(&mut self, manifest: Manifest) -> Result<u64, GfError> {
        self.root.revalidate_named().map_err(storage_err)?;
        let mut manifest_file = self
            .root
            .open_child_file(std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        if hex_sha256(&body) != hex_sha256(&serde_json::to_vec(&manifest).map_err(storage_err)?) {
            return Err(storage_err("committed UUID manifest differs from plan"));
        }
        let mut next_runs = Vec::with_capacity(manifest.runs.len());
        let mut authenticated_bytes = 0_u64;
        for descriptor in &manifest.runs {
            if let Some(retained) = self.runs.iter().find(|run| run.descriptor == *descriptor) {
                next_runs.push(AuthenticatedRun {
                    identities: retained.identities.try_clone().map_err(storage_err)?,
                    identities_identity: retained.identities_identity,
                    node_surrogates: retained.node_surrogates.try_clone().map_err(storage_err)?,
                    node_surrogates_identity: retained.node_surrogates_identity,
                    descriptor: descriptor.clone(),
                });
            } else {
                let identities =
                    open_verified_at(&self.root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
                let node_surrogates = open_verified_at(
                    &self.root,
                    &descriptor.node_surrogates,
                    NODE_LOOKUP_RECORD_BYTES,
                )?;
                authenticated_bytes = authenticated_bytes
                    .saturating_add(descriptor.identities.count * IDENTITY_RECORD_BYTES)
                    .saturating_add(descriptor.node_surrogates.count * NODE_LOOKUP_RECORD_BYTES);
                next_runs.push(AuthenticatedRun {
                    identities_identity: graphforge_filesystem::file_identity(&identities)
                        .map_err(storage_err)?,
                    node_surrogates_identity: graphforge_filesystem::file_identity(
                        &node_surrogates,
                    )
                    .map_err(storage_err)?,
                    identities,
                    node_surrogates,
                    descriptor: descriptor.clone(),
                });
            }
        }
        self.manifest_identity =
            graphforge_filesystem::file_identity(&manifest_file).map_err(storage_err)?;
        self.manifest_sha256 = hex_sha256(&body);
        self.manifest_file = manifest_file;
        self.manifest = manifest;
        self.runs = next_runs;
        self.cache = ProbeBlockCache::default();
        self.authenticated_bytes = 0;
        self.authenticated_blocks = 0;
        Ok(authenticated_bytes)
    }

    pub(crate) fn probe(
        &mut self,
        kind: UuidIndexKind,
        requested: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut resolved = std::collections::BTreeMap::new();
        for uuid in unique {
            let mut found = false;
            for run in self.runs.iter_mut().rev() {
                if let Some((present, _)) = cached_identity_state(
                    &mut run.identities,
                    run.identities_identity,
                    run.descriptor.identities.count,
                    uuid,
                    kind,
                    &mut self.cache,
                    &mut metrics.file_seeks,
                )? {
                    found = present;
                    break;
                }
            }
            metrics.found += u64::from(found);
            resolved.insert(uuid, found);
        }
        Ok((
            requested.iter().map(|uuid| resolved[uuid]).collect(),
            metrics,
        ))
    }

    pub(crate) fn lookup_node_surrogates(
        &mut self,
        requested: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut resolved = std::collections::BTreeMap::new();
        for uuid in unique {
            let mut value = None;
            for run in self.runs.iter_mut().rev() {
                if let Some((present, surrogate)) = cached_identity_state(
                    &mut run.identities,
                    run.identities_identity,
                    run.descriptor.identities.count,
                    uuid,
                    UuidIndexKind::Node,
                    &mut self.cache,
                    &mut metrics.file_seeks,
                )? {
                    if present {
                        value = Some(surrogate);
                    }
                    break;
                }
            }
            metrics.found += u64::from(value.is_some());
            resolved.insert(uuid, value);
        }
        Ok((
            requested.iter().map(|uuid| resolved[uuid]).collect(),
            metrics,
        ))
    }
}

impl UuidMembershipIndex {
    /// Open and fully authenticate the current immutable index snapshot.
    pub fn open(project_dir: &Path) -> Result<Self, GfError> {
        let generation = crate::read_topology_generation(project_dir)?;
        Self::open_at_generation(project_dir, generation)
    }

    pub(crate) fn open_at_generation(project_dir: &Path, generation: u64) -> Result<Self, GfError> {
        let root = project_dir.join(INDEX_DIR);
        let body = fs::read(root.join(MANIFEST)).map_err(storage_err)?;
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(storage_err(format!(
                "unsupported format version {}",
                manifest.format_version
            )));
        }
        if manifest.current_generation != generation {
            return Err(storage_err(format!(
                "stale index generation {} (graph generation {generation})",
                manifest.current_generation
            )));
        }
        validate_run_descriptors(&manifest)?;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        for descriptor in &manifest.runs {
            let identities = open_verified(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_verified(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            validate_run_contents(
                identities.try_clone().map_err(storage_err)?,
                node_surrogates.try_clone().map_err(storage_err)?,
                descriptor,
            )?;
            runs.push(OpenRun {
                identities,
                node_surrogates,
                descriptor: descriptor.clone(),
            });
        }
        Ok(Self { runs, manifest })
    }

    /// Topology generation authenticated by this open handle.
    #[must_use]
    pub const fn topology_generation(&self) -> u64 {
        self.manifest.current_generation
    }

    #[must_use]
    /// Return the authenticated unique-record count for one identity domain.
    pub fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.manifest.live_node_count,
            UuidIndexKind::Edge => self.manifest.live_edge_count,
        }
    }

    /// Probe a batch in caller order. Memory is O(unique requested UUIDs).
    pub fn probe(
        &mut self,
        kind: UuidIndexKind,
        requested: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut membership = std::collections::BTreeMap::new();
        for uuid in unique {
            let mut found = false;
            for run in self.runs.iter_mut().rev() {
                if let Some((present, _)) = binary_search_identity_state(
                    &mut run.identities,
                    run.descriptor.identities.count,
                    uuid,
                    kind,
                    &mut metrics.file_seeks,
                )? {
                    found = present;
                    break;
                }
            }
            metrics.found += u64::from(found);
            membership.insert(uuid, found);
        }
        Ok((requested.iter().map(|u| membership[u]).collect(), metrics))
    }

    /// Resolve node UUIDs to their canonical surrogates without scanning
    /// topology. Results retain caller order; an absent UUID returns `None`.
    pub fn lookup_node_surrogates(
        &mut self,
        requested: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut resolved = std::collections::BTreeMap::new();
        for uuid in unique {
            let mut surrogate = None;
            for run in self.runs.iter_mut().rev() {
                let state = binary_search_identity_state(
                    &mut run.identities,
                    run.descriptor.identities.count,
                    uuid,
                    UuidIndexKind::Node,
                    &mut metrics.file_seeks,
                )?;
                if let Some((present, value)) = state {
                    if !present {
                        surrogate = None;
                        break;
                    }
                    surrogate = Some(value);
                    if binary_search_surrogate_uuid(
                        &mut run.node_surrogates,
                        run.descriptor.node_surrogates.count,
                        value,
                        &mut metrics.file_seeks,
                    )? != Some(*uuid.as_bytes())
                    {
                        return Err(storage_err("identity/surrogate run pair is inconsistent"));
                    }
                    break;
                }
            }
            metrics.found += u64::from(surrogate.is_some());
            resolved.insert(uuid, surrogate);
        }
        Ok((
            requested.iter().map(|uuid| resolved[uuid]).collect(),
            metrics,
        ))
    }
}

/// Whether a membership manifest exists, without duplicating its private layout.
#[must_use]
pub fn uuid_membership_index_present(project_dir: &Path) -> bool {
    project_dir.join(INDEX_DIR).join(MANIFEST).is_file()
}

/// Whether the manifest version and topology generation match the workspace.
/// This cheap publication-path check deliberately does not authenticate data;
/// readers still use [`UuidMembershipIndex::open`] before trusting membership.
pub fn uuid_membership_index_is_fresh(project_dir: &Path) -> Result<bool, GfError> {
    let body = match fs::read(project_dir.join(INDEX_DIR).join(MANIFEST)) {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(storage_err(error)),
    };
    let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
    Ok(manifest.format_version == FORMAT_VERSION
        && manifest.current_generation == crate::read_topology_generation(project_dir)?)
}

const TOPOLOGY_RECEIPT: &str = "topology-receipt.json";
const MAX_MANIFEST_BYTES: u64 = 1 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TopologyIndexReceipt {
    nonce: String,
    expected_generation: u64,
    topology_delta_sha256: String,
    manifest_sha256: String,
}

pub(crate) struct PreparedUuidIndexDelta {
    expected_generation: u64,
    auxiliary: crate::AuxiliaryReceipt,
    metrics: UuidIndexAppendMetrics,
    manifest: Manifest,
}

pub(crate) struct UuidTopologyDelta {
    pub nodes: Vec<(Uuid, u64)>,
    pub edges: Vec<Uuid>,
    pub deleted_nodes: Vec<Uuid>,
    pub deleted_edges: Vec<Uuid>,
}

/// Commit topology and its UUID participant under the one durable rewrite lock.
pub(crate) fn commit_uuid_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
    delta: UuidTopologyDelta,
    snapshot: &mut Option<AuthenticatedUuidIndexSnapshot>,
) -> Result<Option<u64>, GfError> {
    if delta.nodes.is_empty()
        && delta.edges.is_empty()
        && delta.deleted_nodes.is_empty()
        && delta.deleted_edges.is_empty()
    {
        return crate::generation::commit_topology_aware(staged, project_dir);
    }
    let prepared = std::rc::Rc::new(std::cell::RefCell::new(None));
    let prepared_from_callback = std::rc::Rc::clone(&prepared);
    let generations = std::rc::Rc::new(std::cell::Cell::new(None));
    let generations_from_callback = std::rc::Rc::clone(&generations);
    let root = project_dir.to_path_buf();
    let expected_root = root.clone();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(|context, batch| {
            if context.project_root != expected_root {
                return Err(storage_err("rewrite participant project root changed"));
            }
            context.project.revalidate_named().map_err(storage_err)?;
            generations_from_callback.set(Some((context.prior, context.next)));
            if context.prior.topology != 0 && !uuid_membership_index_present(context.project_root) {
                rebuild_uuid_membership_indexes(
                    context.project_root,
                    UuidIndexBuildLimits::default(),
                )?;
            }
            if context.prior.topology != 0
                && snapshot
                    .as_ref()
                    .is_none_or(|value| value.topology_generation() != context.prior.topology)
            {
                *snapshot = Some(AuthenticatedUuidIndexSnapshot::open_at_generation(
                    context.project_root,
                    context.prior.topology,
                )?);
            }
            let (deleted_nodes, deleted_edges) = if let Some(index) = snapshot.as_mut() {
                let (surrogates, _) = index.lookup_node_surrogates(&delta.deleted_nodes)?;
                let nodes = delta
                    .deleted_nodes
                    .iter()
                    .copied()
                    .zip(surrogates)
                    .filter_map(|(uuid, surrogate)| surrogate.map(|id| (uuid, id)))
                    .collect::<Vec<_>>();
                let (present, _) = index.probe(UuidIndexKind::Edge, &delta.deleted_edges)?;
                let edges = delta
                    .deleted_edges
                    .iter()
                    .copied()
                    .zip(present)
                    .filter_map(|(uuid, present)| present.then_some(uuid))
                    .collect::<Vec<_>>();
                (nodes, edges)
            } else {
                (Vec::new(), Vec::new())
            };
            let token = prepare_uuid_membership_delta(
                context.project_root,
                context.prior.topology,
                context.next.topology,
                snapshot.as_mut(),
                batch,
                &delta.nodes,
                &delta.edges,
                &deleted_nodes,
                &deleted_edges,
            )?;
            let receipt = token
                .as_ref()
                .map(PreparedUuidIndexDelta::auxiliary_receipt);
            *prepared_from_callback.borrow_mut() = token;
            Ok(receipt)
        });
    let commit =
        crate::generation::commit_topology_aware_with_participant(staged, &root, participant);
    let token = prepared.borrow_mut().take();
    let committed = match commit {
        Ok(value) => value,
        Err(error) => {
            let (Some(token), Some((prior, next))) = (token.as_ref(), generations.get()) else {
                return Err(error);
            };
            match reconcile_uuid_auxiliary(&root, prior, next, token)? {
                crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed => Some(next.topology),
                crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted => {
                    return Err(error);
                }
            }
        }
    };
    if let (Some(generation), Some(token)) = (committed, token.as_ref()) {
        token.verify_generation(generation)?;
        let _work = token.metrics();
        let refresh = if let Some(value) = snapshot.as_mut() {
            token.advance_snapshot(value).map(|_| ())
        } else {
            AuthenticatedUuidIndexSnapshot::open_at_generation(&root, generation)
                .map(|value| *snapshot = Some(value))
        };
        if refresh.is_err() {
            *snapshot = None;
        }
    }
    Ok(committed)
}

/// Stage one bounded v3 UUID-index delta and its authenticated receipt into the
/// caller's generation-last topology rewrite transaction.
pub(crate) fn prepare_uuid_membership_delta(
    project_dir: &Path,
    current: u64,
    generation: u64,
    snapshot: Option<&mut AuthenticatedUuidIndexSnapshot>,
    batch: &mut crate::staging::RewriteBatch,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<Option<PreparedUuidIndexDelta>, GfError> {
    if generation != current.saturating_add(1) {
        return Err(storage_err(
            "prepared UUID delta generation is not the next generation",
        ));
    }
    let source_root = project_dir.join(INDEX_DIR);
    if current != 0 && !source_root.join(MANIFEST).is_file() {
        return Err(storage_err(
            "UUID membership index migration is required before topology mutation",
        ));
    }
    fs::create_dir_all(&source_root).map_err(storage_err)?;
    let parent = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-plan-")
        .tempdir_in(parent)
        .map_err(storage_err)?;
    let (manifest, outputs, _superseded, metrics) = plan_uuid_membership_delta(
        &source_root,
        current,
        generation,
        snapshot,
        scratch.path(),
        nodes,
        edges,
        deleted_nodes,
        deleted_edges,
    )?;
    for (record, path) in outputs {
        batch.stage_file(&source_root.join(record.name), &path)?;
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    batch.stage_bytes(&source_root.join(MANIFEST), &manifest_bytes)?;
    let nonce = Uuid::new_v4().simple().to_string();
    let receipt = TopologyIndexReceipt {
        nonce,
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256(nodes, edges, deleted_nodes, deleted_edges),
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let receipt_path = source_root.join(TOPOLOGY_RECEIPT);
    batch.stage_bytes(&receipt_path, &receipt_bytes)?;
    let digest = Sha256::digest(&receipt_bytes);
    Ok(Some(PreparedUuidIndexDelta {
        expected_generation: generation,
        metrics,
        manifest,
        auxiliary: crate::AuxiliaryReceipt {
            kind: "uuid-membership/v3".to_owned(),
            schema_version: FORMAT_VERSION,
            path: format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
            digest: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
            bytes: receipt_bytes.len() as u64,
        },
    }))
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn reconcile_uuid_auxiliary(
    project_dir: &Path,
    prior: crate::durable_rewrite::GenerationPair,
    next: crate::durable_rewrite::GenerationPair,
    prepared: &PreparedUuidIndexDelta,
) -> Result<crate::durable_rewrite::AuxiliaryReconcileOutcome, GfError> {
    let auxiliary = prepared.auxiliary_receipt();
    let outcome =
        crate::durable_rewrite::reconcile_auxiliary(project_dir, prior, next, &auxiliary)?;
    if outcome == crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted {
        return Ok(outcome);
    }
    let project = graphforge_filesystem::StableDirectory::open(project_dir).map_err(storage_err)?;
    let cache = project
        .open_child_directory(std::ffi::OsStr::new(".graphforge-cache"))
        .map_err(storage_err)?;
    let index = cache
        .open_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .map_err(storage_err)?;
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(TOPOLOGY_RECEIPT))
        .map_err(storage_err)?;
    let receipt_body = read_bounded(&mut receipt_file, MAX_MANIFEST_BYTES)?;
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&receipt_body).map_err(storage_err)?;
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(MANIFEST))
        .map_err(storage_err)?;
    let manifest_body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
    project.revalidate_named().map_err(storage_err)?;
    cache.revalidate_named().map_err(storage_err)?;
    index.revalidate_named().map_err(storage_err)?;
    if receipt.expected_generation != next.topology
        || receipt.manifest_sha256 != hex_sha256(&manifest_body)
        || receipt.manifest_sha256
            != hex_sha256(&serde_json::to_vec(&prepared.manifest).map_err(storage_err)?)
    {
        return Err(storage_err(
            "committed UUID receipt does not authenticate the expected manifest",
        ));
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn plan_uuid_membership_delta(
    root: &Path,
    current: u64,
    generation: u64,
    mut snapshot: Option<&mut AuthenticatedUuidIndexSnapshot>,
    scratch: &Path,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<
    (
        Manifest,
        Vec<(FileRecord, PathBuf)>,
        Vec<(String, graphforge_filesystem::FileIdentity)>,
        UuidIndexAppendMetrics,
    ),
    GfError,
> {
    let mut manifest = if current == 0 {
        Manifest {
            format_version: FORMAT_VERSION,
            base_generation: 0,
            current_generation: 0,
            live_node_count: 0,
            live_edge_count: 0,
            runs: Vec::new(),
        }
    } else {
        let retained = snapshot
            .as_deref_mut()
            .ok_or_else(|| storage_err("authenticated UUID snapshot is required"))?;
        retained.revalidate()?;
        if retained.manifest.current_generation != current {
            return Err(storage_err("retained manifest generation is stale"));
        }
        retained.manifest.clone()
    };

    let prior_names = manifest_file_names(&manifest);
    let mut identities = nodes
        .iter()
        .map(|(uuid, id)| (*uuid, 0_u8, *id))
        .chain(edges.iter().map(|uuid| (*uuid, 1_u8, 0)))
        .chain(deleted_nodes.iter().map(|(uuid, id)| (*uuid, 2_u8, *id)))
        .chain(deleted_edges.iter().map(|uuid| (*uuid, 3_u8, 0)))
        .collect::<Vec<_>>();
    identities.sort_unstable_by_key(|entry| *entry.0.as_bytes());
    if identities.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || nodes.iter().any(|(_, id)| *id == 0)
    {
        return Err(storage_err(
            "new identity run contains duplicate/invalid identity",
        ));
    }
    let mut surrogates = nodes
        .iter()
        .map(|(uuid, id)| (*id, *uuid))
        .chain(deleted_nodes.iter().map(|(uuid, id)| (*id, *uuid)))
        .collect::<Vec<_>>();
    surrogates.sort_unstable();
    if surrogates.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("new node run contains duplicate surrogate"));
    }

    let (retained_authentication_bytes, retained_authentication_blocks) =
        snapshot.as_deref_mut().map_or(
            (0, 0),
            AuthenticatedUuidIndexSnapshot::take_authentication_work,
        );
    let mut validation_metrics = UuidIndexAppendMetrics::default();
    if let Some(retained) = snapshot.as_deref_mut() {
        for run in &mut retained.runs {
            reject_retained_identity_collisions(run, &identities, &mut validation_metrics)?;
            reject_retained_surrogate_collisions(run, &surrogates, &mut validation_metrics)?;
        }
    }

    let identity_path = scratch.join("identities-l0.run");
    let surrogate_path = scratch.join("surrogates-l0.run");
    write_identity_records(&identity_path, &identities)?;
    write_surrogate_records(&surrogate_path, &surrogates)?;
    let identity_record = describe_run(
        &identity_path,
        "identities-v3",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let surrogate_record = describe_run(
        &surrogate_path,
        "node-surrogates-v3",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    let mut sources = HashMap::from([
        (identity_record.name.clone(), identity_path),
        (surrogate_record.name.clone(), surrogate_path),
    ]);
    if current == 0 {
        let empty_identity_path = scratch.join("identities-base.run");
        let empty_surrogate_path = scratch.join("surrogates-base.run");
        File::create(&empty_identity_path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        File::create(&empty_surrogate_path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        let base_identities = describe_run(
            &empty_identity_path,
            "identities-v3-base",
            0,
            IDENTITY_RECORD_BYTES,
        )?;
        let base_surrogates = describe_run(
            &empty_surrogate_path,
            "node-surrogates-v3-base",
            0,
            NODE_LOOKUP_RECORD_BYTES,
        )?;
        sources.insert(base_identities.name.clone(), empty_identity_path);
        sources.insert(base_surrogates.name.clone(), empty_surrogate_path);
        manifest.runs.push(RunRecord {
            base: true,
            level: 0,
            first_generation: 0,
            last_generation: 0,
            identities: base_identities,
            node_surrogates: base_surrogates,
            node_count: 0,
            edge_count: 0,
            deleted_node_count: 0,
            deleted_edge_count: 0,
        });
    }
    manifest.runs.push(RunRecord {
        base: false,
        level: 0,
        first_generation: generation,
        last_generation: generation,
        identities: identity_record,
        node_surrogates: surrogate_record,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        deleted_node_count: deleted_nodes.len() as u64,
        deleted_edge_count: deleted_edges.len() as u64,
    });
    let mut metrics = UuidIndexAppendMetrics {
        input_records: identities.len() as u64,
        physical_bytes_written: identities.len() as u64 * IDENTITY_RECORD_BYTES
            + surrogates.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_bytes: identities.len() as u64 * IDENTITY_RECORD_BYTES
            + surrogates.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_blocks: (identities.len() as u64 * IDENTITY_RECORD_BYTES)
            .div_ceil(BULK_IO_BYTES as u64)
            + (surrogates.len() as u64 * NODE_LOOKUP_RECORD_BYTES).div_ceil(BULK_IO_BYTES as u64),
        peak_buffered_records: identities.len() + surrogates.len(),
        peak_buffered_bytes: identities.len() * 32 + surrogates.len() * 24,
        validation_random_seeks: 0,
        validation_scan_bytes: validation_metrics.validation_scan_bytes,
        validation_scan_blocks: validation_metrics.validation_scan_blocks,
        snapshot_admission_authentication_bytes: retained_authentication_bytes,
        snapshot_admission_authentication_blocks: retained_authentication_blocks,
        ..Default::default()
    };
    compact_planned_levels(
        root,
        scratch,
        &mut manifest,
        &mut sources,
        snapshot.as_deref(),
        &mut metrics,
    )?;
    manifest.current_generation = generation;
    manifest.live_node_count = manifest
        .live_node_count
        .checked_add(nodes.len() as u64)
        .and_then(|v| v.checked_sub(deleted_nodes.len() as u64))
        .ok_or_else(|| storage_err("node live-count delta is invalid"))?;
    manifest.live_edge_count = manifest
        .live_edge_count
        .checked_add(edges.len() as u64)
        .and_then(|v| v.checked_sub(deleted_edges.len() as u64))
        .ok_or_else(|| storage_err("edge live-count delta is invalid"))?;
    manifest
        .runs
        .sort_unstable_by_key(|run| run.first_generation);
    validate_run_descriptors(&manifest)?;
    let retained = manifest_file_names(&manifest);
    let mut outputs = sources
        .into_iter()
        .filter(|(name, _)| retained.contains(name))
        .map(|(name, path)| {
            let record = manifest
                .runs
                .iter()
                .flat_map(|run| [&run.identities, &run.node_surrogates])
                .find(|record| record.name == name)
                .expect("planned output is retained")
                .clone();
            (record, path)
        })
        .collect::<Vec<_>>();
    outputs.sort_unstable_by(|left, right| left.0.name.cmp(&right.0.name));
    metrics.new_output_authentication_bytes = outputs
        .iter()
        .map(|(record, _)| {
            record
                .blocks
                .iter()
                .map(|block| u64::from(block.len))
                .sum::<u64>()
        })
        .sum();
    metrics.new_output_authentication_blocks = outputs
        .iter()
        .map(|(record, _)| record.blocks.len() as u64)
        .sum();
    let mut superseded = Vec::new();
    if let Some(snapshot) = snapshot.as_deref() {
        for name in prior_names.difference(&retained) {
            let file = snapshot
                .root
                .open_child_file(std::ffi::OsStr::new(name))
                .map_err(storage_err)?;
            superseded.push((
                name.clone(),
                graphforge_filesystem::file_identity(&file).map_err(storage_err)?,
            ));
        }
    }
    metrics.retained_runs = manifest.runs.len();
    Ok((manifest, outputs, superseded, metrics))
}

fn open_verified_at(
    directory: &graphforge_filesystem::StableDirectory,
    record: &FileRecord,
    record_bytes: u64,
) -> Result<File, GfError> {
    if Path::new(&record.name).components().count() != 1 {
        return Err(storage_err("manifest contains a non-local index filename"));
    }
    let mut file = directory
        .open_child_file(std::ffi::OsStr::new(&record.name))
        .map_err(storage_err)?;
    let expected = record
        .count
        .checked_mul(record_bytes)
        .ok_or_else(|| storage_err("record length overflow"))?;
    if file.metadata().map_err(storage_err)?.len() != expected {
        return Err(storage_err("retained run authentication failed"));
    }
    authenticate_file_blocks(&mut file, record, record_bytes, None)?;
    file.rewind().map_err(storage_err)?;
    Ok(file)
}

fn authenticate_file_blocks(
    file: &mut File,
    record: &FileRecord,
    record_bytes: u64,
    mut work: Option<&mut UuidIndexAppendMetrics>,
) -> Result<(), GfError> {
    validate_block_records(record, record_bytes)?;
    let mut whole = Sha256::new();
    for block in &record.blocks {
        file.seek(SeekFrom::Start(block.offset))
            .map_err(storage_err)?;
        let mut bytes = vec![0_u8; block.len as usize];
        file.read_exact(&mut bytes).map_err(storage_err)?;
        let width = record_bytes as usize;
        let key_width = if record_bytes == IDENTITY_RECORD_BYTES {
            16
        } else {
            8
        };
        if hex_sha256(&bytes) != block.sha256
            || hex_sha256_key(&bytes[..key_width]) != block.first_key
            || hex_sha256_key(&bytes[bytes.len() - width..bytes.len() - width + key_width])
                != block.last_key
        {
            return Err(storage_err("UUID run block authentication failed"));
        }
        whole.update(&bytes);
        if let Some(metrics) = work.as_deref_mut() {
            metrics.validation_scan_bytes = metrics
                .validation_scan_bytes
                .saturating_add(bytes.len() as u64);
            metrics.validation_scan_blocks = metrics.validation_scan_blocks.saturating_add(1);
        }
    }
    let digest: String = whole
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if digest != record.sha256 {
        return Err(storage_err("UUID run authentication failed"));
    }
    Ok(())
}

fn validate_block_records(record: &FileRecord, record_bytes: u64) -> Result<(), GfError> {
    let expected = record
        .count
        .checked_mul(record_bytes)
        .ok_or_else(|| storage_err("record length overflow"))?;
    if expected == 0 {
        if !record.blocks.is_empty() {
            return Err(storage_err("empty UUID run has authenticated blocks"));
        }
        return Ok(());
    }
    let key_hex_len = if record_bytes == IDENTITY_RECORD_BYTES {
        32
    } else {
        16
    };
    let mut offset = 0_u64;
    for block in &record.blocks {
        if block.offset != offset
            || block.len == 0
            || u64::from(block.len) % record_bytes != 0
            || block.len as usize > BULK_IO_BYTES
            || block.first_key.len() != key_hex_len
            || block.last_key.len() != key_hex_len
            || block.first_key > block.last_key
            || block.sha256.len() != 64
        {
            return Err(storage_err("UUID run block table is not canonical"));
        }
        offset = offset.saturating_add(u64::from(block.len));
    }
    if offset != expected
        || record
            .blocks
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
    {
        return Err(storage_err("UUID run block fences are not canonical"));
    }
    Ok(())
}

fn describe_run(
    path: &Path,
    kind: &str,
    generation: u64,
    width: u64,
) -> Result<FileRecord, GfError> {
    let length = path.metadata().map_err(storage_err)?.len();
    if length % width != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let (sha256, blocks) = describe_blocks(&mut File::open(path).map_err(storage_err)?, width)?;
    Ok(FileRecord {
        name: format!("{kind}-{generation}-{}.uuidx", &sha256[..16]),
        count: length / width,
        sha256,
        blocks,
    })
}

fn describe_blocks(file: &mut File, width: u64) -> Result<(String, Vec<BlockRecord>), GfError> {
    let width = usize::try_from(width).map_err(storage_err)?;
    let key_width = match width {
        32 => 16,
        24 => 8,
        _ => return Err(storage_err("unsupported UUID run record width")),
    };
    let block_len = BULK_IO_BYTES / width * width;
    let mut offset = 0_u64;
    let mut whole = Sha256::new();
    let mut blocks = Vec::new();
    let mut bytes = vec![0_u8; block_len];
    loop {
        let mut valid = 0;
        while valid < bytes.len() {
            let read = file.read(&mut bytes[valid..]).map_err(storage_err)?;
            if read == 0 {
                break;
            }
            valid += read;
        }
        if valid == 0 {
            break;
        }
        if valid % width != 0 {
            return Err(storage_err("internal run has a partial index record"));
        }
        let slice = &bytes[..valid];
        whole.update(slice);
        blocks.push(BlockRecord {
            offset,
            len: u32::try_from(valid).map_err(storage_err)?,
            first_key: hex_sha256_key(&slice[..key_width]),
            last_key: hex_sha256_key(&slice[valid - width..valid - width + key_width]),
            sha256: hex_sha256(slice),
        });
        offset = offset.saturating_add(valid as u64);
    }
    Ok((
        whole
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        blocks,
    ))
}

fn hex_sha256_key(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn compact_planned_levels(
    root: &Path,
    scratch: &Path,
    manifest: &mut Manifest,
    sources: &mut HashMap<String, PathBuf>,
    snapshot: Option<&AuthenticatedUuidIndexSnapshot>,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    for level in 0..63_u8 {
        let mut indexes = manifest
            .runs
            .iter()
            .enumerate()
            .filter(|(_, run)| !run.base && run.level == level)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if indexes.len() < 2 {
            continue;
        }
        if indexes.len() != 2 {
            return Err(storage_err(
                "manifest has more than one retained run at a level",
            ));
        }
        indexes.sort_unstable_by_key(|index| manifest.runs[*index].first_generation);
        let right = manifest.runs.remove(indexes[1]);
        let left = manifest.runs.remove(indexes[0]);
        if left.last_generation.saturating_add(1) != right.first_generation {
            return Err(storage_err("equal-level runs are not adjacent"));
        }
        let identity_path = scratch.join(format!("identities-level-{}.run", level + 1));
        let surrogate_path = scratch.join(format!("surrogates-level-{}.run", level + 1));
        let identity_inputs = [
            (
                planned_file(root, sources, snapshot, &left.identities.name, true)?,
                left.identities.clone(),
            ),
            (
                planned_file(root, sources, snapshot, &right.identities.name, true)?,
                right.identities.clone(),
            ),
        ];
        let surrogate_inputs = [
            (
                planned_file(root, sources, snapshot, &left.node_surrogates.name, false)?,
                left.node_surrogates.clone(),
            ),
            (
                planned_file(root, sources, snapshot, &right.node_surrogates.name, false)?,
                right.node_surrogates.clone(),
            ),
        ];
        merge_identity_handles(identity_inputs, &identity_path, metrics)?;
        merge_surrogate_handles(surrogate_inputs, &surrogate_path, metrics)?;
        let identities = describe_run(
            &identity_path,
            &format!("identities-v3-l{}", level + 1),
            right.last_generation,
            IDENTITY_RECORD_BYTES,
        )?;
        let node_surrogates = describe_run(
            &surrogate_path,
            &format!("node-surrogates-v3-l{}", level + 1),
            right.last_generation,
            NODE_LOOKUP_RECORD_BYTES,
        )?;
        let bytes = identities.count * IDENTITY_RECORD_BYTES
            + node_surrogates.count * NODE_LOOKUP_RECORD_BYTES;
        metrics.physical_bytes_written = metrics.physical_bytes_written.saturating_add(bytes);
        metrics.write_bytes = metrics.write_bytes.saturating_add(bytes);
        metrics.write_blocks = metrics
            .write_blocks
            .saturating_add(bytes.div_ceil(BULK_IO_BYTES as u64));
        let counts = count_identity_states(&identity_path)?;
        sources.insert(identities.name.clone(), identity_path);
        sources.insert(node_surrogates.name.clone(), surrogate_path);
        manifest.runs.push(RunRecord {
            base: false,
            level: level + 1,
            first_generation: left.first_generation,
            last_generation: right.last_generation,
            identities,
            node_surrogates,
            node_count: counts.0,
            edge_count: counts.1,
            deleted_node_count: counts.2,
            deleted_edge_count: counts.3,
        });
    }
    Ok(())
}

fn planned_file(
    root: &Path,
    sources: &HashMap<String, PathBuf>,
    snapshot: Option<&AuthenticatedUuidIndexSnapshot>,
    name: &str,
    identities: bool,
) -> Result<File, GfError> {
    if let Some(path) = sources.get(name) {
        return File::open(path).map_err(storage_err);
    }
    if let Some(snapshot) = snapshot {
        for run in &snapshot.runs {
            if identities && run.descriptor.identities.name == name {
                return run.identities.try_clone().map_err(storage_err);
            }
            if !identities && run.descriptor.node_surrogates.name == name {
                return run.node_surrogates.try_clone().map_err(storage_err);
            }
        }
        return Err(storage_err("planned compaction input is not retained"));
    }
    File::open(root.join(name)).map_err(storage_err)
}

struct VerifiedBlockReader {
    file: File,
    blocks: Vec<BlockRecord>,
    next: usize,
    bytes: Vec<u8>,
    cursor: usize,
    authenticated_bytes: u64,
    authenticated_blocks: u64,
    width: usize,
    key_width: usize,
}

impl VerifiedBlockReader {
    fn new(file: File, record: &FileRecord, width: u64) -> Result<Self, GfError> {
        validate_block_records(record, width)?;
        Ok(Self {
            file,
            blocks: record.blocks.clone(),
            next: 0,
            bytes: Vec::new(),
            cursor: 0,
            authenticated_bytes: 0,
            authenticated_blocks: 0,
            width: width as usize,
            key_width: if width == IDENTITY_RECORD_BYTES {
                16
            } else {
                8
            },
        })
    }
}

impl Read for VerifiedBlockReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.cursor == self.bytes.len() {
            let Some(block) = self.blocks.get(self.next) else {
                return Ok(0);
            };
            self.file.seek(SeekFrom::Start(block.offset))?;
            self.bytes.resize(block.len as usize, 0);
            self.file.read_exact(&mut self.bytes)?;
            if hex_sha256(&self.bytes) != block.sha256
                || hex_sha256_key(&self.bytes[..self.key_width]) != block.first_key
                || hex_sha256_key(
                    &self.bytes[self.bytes.len() - self.width
                        ..self.bytes.len() - self.width + self.key_width],
                ) != block.last_key
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "UUID compaction block authentication failed",
                ));
            }
            self.next += 1;
            self.cursor = 0;
            self.authenticated_bytes = self
                .authenticated_bytes
                .saturating_add(self.bytes.len() as u64);
            self.authenticated_blocks = self.authenticated_blocks.saturating_add(1);
        }
        let available = &self.bytes[self.cursor..];
        let copied = available.len().min(output.len());
        output[..copied].copy_from_slice(&available[..copied]);
        self.cursor += copied;
        Ok(copied)
    }
}

fn merge_identity_handles(
    inputs: [(File, FileRecord); 2],
    output: &Path,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut readers = inputs
        .into_iter()
        .map(|(file, record)| VerifiedBlockReader::new(file, &record, IDENTITY_RECORD_BYTES))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 32], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_exact_record::<32>(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let key: [u8; 16] = record[..16].try_into().expect("fixed");
        let mut newest = index;
        if let Some(next) = read_exact_record::<32>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..16] == key)
        {
            let Reverse((candidate, source)) = heap.pop().expect("peeked");
            if source > newest {
                record = candidate;
                newest = source;
            }
            if let Some(next) = read_exact_record::<32>(&mut readers[source])? {
                heap.push(Reverse((next, source)));
            }
        }
        if block.len() + 32 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&record);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    for reader in readers {
        metrics.validation_scan_bytes = metrics
            .validation_scan_bytes
            .saturating_add(reader.authenticated_bytes);
        metrics.validation_scan_blocks = metrics
            .validation_scan_blocks
            .saturating_add(reader.authenticated_blocks);
    }
    Ok(())
}

fn merge_surrogate_handles(
    inputs: [(File, FileRecord); 2],
    output: &Path,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut readers = inputs
        .into_iter()
        .map(|(file, record)| VerifiedBlockReader::new(file, &record, NODE_LOOKUP_RECORD_BYTES))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 24], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_exact_record::<24>(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let key: [u8; 8] = record[..8].try_into().expect("fixed");
        let mut newest = index;
        if let Some(next) = read_exact_record::<24>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..8] == key)
        {
            let Reverse((candidate, source)) = heap.pop().expect("peeked");
            if source > newest {
                record = candidate;
                newest = source;
            }
            if let Some(next) = read_exact_record::<24>(&mut readers[source])? {
                heap.push(Reverse((next, source)));
            }
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&record);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    for reader in readers {
        metrics.validation_scan_bytes = metrics
            .validation_scan_bytes
            .saturating_add(reader.authenticated_bytes);
        metrics.validation_scan_blocks = metrics
            .validation_scan_blocks
            .saturating_add(reader.authenticated_blocks);
    }
    Ok(())
}

fn topology_delta_sha256(
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> String {
    let mut nodes = nodes.to_vec();
    nodes.sort_unstable_by_key(|(uuid, _)| *uuid.as_bytes());
    let mut edges = edges.to_vec();
    edges.sort_unstable_by_key(|uuid| *uuid.as_bytes());
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge/uuid-index-topology-delta/v1");
    for (uuid, surrogate) in nodes {
        hasher.update([0]);
        hasher.update(uuid.as_bytes());
        hasher.update(surrogate.to_be_bytes());
    }
    for uuid in edges {
        hasher.update([1]);
        hasher.update(uuid.as_bytes());
    }
    let mut deleted_nodes = deleted_nodes.to_vec();
    deleted_nodes.sort_unstable_by_key(|(uuid, _)| *uuid.as_bytes());
    for (uuid, surrogate) in deleted_nodes {
        hasher.update([2]);
        hasher.update(uuid.as_bytes());
        hasher.update(surrogate.to_be_bytes());
    }
    let mut deleted_edges = deleted_edges.to_vec();
    deleted_edges.sort_unstable_by_key(|uuid| *uuid.as_bytes());
    for uuid in deleted_edges {
        hasher.update([3]);
        hasher.update(uuid.as_bytes());
    }
    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn read_bounded(file: &mut File, maximum: u64) -> Result<Vec<u8>, GfError> {
    let length = file.metadata().map_err(storage_err)?.len();
    if length > maximum {
        return Err(storage_err("recovery control record exceeds size limit"));
    }
    let mut body = Vec::with_capacity(length as usize);
    file.read_to_end(&mut body).map_err(storage_err)?;
    Ok(body)
}

impl PreparedUuidIndexDelta {
    pub(crate) fn auxiliary_receipt(&self) -> crate::AuxiliaryReceipt {
        self.auxiliary.clone()
    }

    pub(crate) fn verify_generation(&self, committed_generation: u64) -> Result<(), GfError> {
        if committed_generation != self.expected_generation {
            return Err(storage_err(
                "topology commit returned an unexpected generation",
            ));
        }
        Ok(())
    }

    pub(crate) fn metrics(&self) -> &UuidIndexAppendMetrics {
        &self.metrics
    }

    pub(crate) fn advance_snapshot(
        &self,
        snapshot: &mut AuthenticatedUuidIndexSnapshot,
    ) -> Result<u64, GfError> {
        snapshot.advance_to(self.manifest.clone())
    }
}

/// Publish one committed topology batch as an immutable authenticated v3 run.
#[cfg(test)]
pub(crate) fn append_uuid_membership_delta(
    project_dir: &Path,
    generation: u64,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
) -> Result<UuidIndexAppendMetrics, GfError> {
    append_uuid_membership_delta_with_tombstones(project_dir, generation, nodes, edges, &[], &[])
}

#[cfg(test)]
fn append_uuid_membership_delta_with_tombstones(
    project_dir: &Path,
    generation: u64,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<UuidIndexAppendMetrics, GfError> {
    if crate::read_topology_generation(project_dir)? != generation {
        return Err(storage_err(
            "topology generation changed before index append",
        ));
    }
    let root = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&root).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let mut manifest: Manifest = match fs::read(root.join(MANIFEST)) {
        Ok(body) => serde_json::from_slice(&body).map_err(storage_err)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && generation == 1 => {
            let scratch = tempfile::Builder::new()
                .prefix("uuid-v3-empty-")
                .tempdir_in(staging)
                .map_err(storage_err)?;
            let empty = scratch.path().join("empty.run");
            File::create(&empty)
                .and_then(|file| file.sync_all())
                .map_err(storage_err)?;
            let identities = publish_data(
                &empty,
                &root,
                staging,
                "identities-v3-base",
                0,
                IDENTITY_RECORD_BYTES,
            )?;
            let node_surrogates = publish_data(
                &empty,
                &root,
                staging,
                "node-surrogates-v3-base",
                0,
                NODE_LOOKUP_RECORD_BYTES,
            )?;
            Manifest {
                format_version: FORMAT_VERSION,
                base_generation: 0,
                current_generation: 0,
                live_node_count: 0,
                live_edge_count: 0,
                runs: vec![RunRecord {
                    base: true,
                    level: 0,
                    first_generation: 0,
                    last_generation: 0,
                    identities,
                    node_surrogates,
                    node_count: 0,
                    edge_count: 0,
                    deleted_node_count: 0,
                    deleted_edge_count: 0,
                }],
            }
        }
        Err(error) => return Err(storage_err(error)),
    };
    if manifest.format_version != FORMAT_VERSION || manifest.current_generation + 1 != generation {
        return Err(storage_err(
            "index append is not a v3 generation continuation",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let prior_files = manifest_file_names(&manifest);
    let mut open_runs = Vec::new();
    for descriptor in &manifest.runs {
        let identities = open_verified(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
        let node_surrogates =
            open_verified(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
        validate_run_contents(
            identities.try_clone().map_err(storage_err)?,
            node_surrogates.try_clone().map_err(storage_err)?,
            descriptor,
        )?;
        open_runs.push(OpenRun {
            identities,
            node_surrogates,
            descriptor: descriptor.clone(),
        });
    }
    let mut identities = nodes
        .iter()
        .map(|(uuid, surrogate)| (*uuid, 0_u8, *surrogate))
        .chain(edges.iter().map(|uuid| (*uuid, 1_u8, 0)))
        .chain(
            deleted_nodes
                .iter()
                .map(|(uuid, surrogate)| (*uuid, 2_u8, *surrogate)),
        )
        .chain(deleted_edges.iter().map(|uuid| (*uuid, 3_u8, 0)))
        .collect::<Vec<_>>();
    identities.sort_unstable_by_key(|entry| *entry.0.as_bytes());
    if identities.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || nodes.iter().any(|(_, surrogate)| *surrogate == 0)
    {
        return Err(storage_err(
            "new identity run contains duplicate/invalid identity",
        ));
    }
    let mut surrogate_keys = nodes
        .iter()
        .map(|(uuid, surrogate)| (*surrogate, *uuid))
        .chain(
            deleted_nodes
                .iter()
                .map(|(uuid, surrogate)| (*surrogate, *uuid)),
        )
        .collect::<Vec<_>>();
    surrogate_keys.sort_unstable();
    if surrogate_keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("new node run contains duplicate surrogate"));
    }
    let mut validation_bytes = 0_u64;
    let mut validation_blocks = 0_u64;
    for run in &open_runs {
        let identity_bytes = reject_identity_collisions(
            BufReader::with_capacity(
                BULK_IO_BYTES,
                File::open(root.join(&run.descriptor.identities.name)).map_err(storage_err)?,
            ),
            &identities,
        )?;
        validation_bytes = validation_bytes.saturating_add(identity_bytes);
        validation_blocks =
            validation_blocks.saturating_add(identity_bytes.div_ceil(BULK_IO_BYTES as u64));
        let surrogate_bytes = reject_surrogate_collisions(
            BufReader::with_capacity(
                BULK_IO_BYTES,
                File::open(root.join(&run.descriptor.node_surrogates.name)).map_err(storage_err)?,
            ),
            &surrogate_keys,
        )?;
        validation_bytes = validation_bytes.saturating_add(surrogate_bytes);
        validation_blocks =
            validation_blocks.saturating_add(surrogate_bytes.div_ceil(BULK_IO_BYTES as u64));
    }
    let scratch = tempfile::Builder::new()
        .prefix("uuid-v3-append-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let identity_path = scratch.path().join("identities.run");
    let surrogate_path = scratch.path().join("surrogates.run");
    write_identity_records(&identity_path, &identities)?;
    write_surrogate_records(&surrogate_path, &surrogate_keys)?;
    let identity_record = publish_data(
        &identity_path,
        &root,
        staging,
        "identities-v3",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let surrogate_record = publish_data(
        &surrogate_path,
        &root,
        staging,
        "node-surrogates-v3",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    manifest.runs.push(RunRecord {
        base: false,
        level: 0,
        first_generation: generation,
        last_generation: generation,
        identities: identity_record,
        node_surrogates: surrogate_record,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        deleted_node_count: deleted_nodes.len() as u64,
        deleted_edge_count: deleted_edges.len() as u64,
    });
    let mut metrics = UuidIndexAppendMetrics {
        input_records: identities.len() as u64,
        physical_bytes_written: identities.len() as u64 * IDENTITY_RECORD_BYTES
            + surrogate_keys.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_blocks: (identities.len() as u64 * IDENTITY_RECORD_BYTES)
            .div_ceil(BULK_IO_BYTES as u64)
            + (surrogate_keys.len() as u64 * NODE_LOOKUP_RECORD_BYTES)
                .div_ceil(BULK_IO_BYTES as u64),
        write_bytes: identities.len() as u64 * IDENTITY_RECORD_BYTES
            + surrogate_keys.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        peak_buffered_records: identities.len() + surrogate_keys.len(),
        peak_buffered_bytes: identities.len() * 32 + surrogate_keys.len() * 24,
        validation_scan_bytes: validation_bytes,
        validation_scan_blocks: validation_blocks,
        ..Default::default()
    };
    compact_manifest_levels(&root, staging, scratch.path(), &mut manifest, &mut metrics)?;
    manifest.current_generation = generation;
    manifest.live_node_count = manifest
        .live_node_count
        .checked_add(nodes.len() as u64)
        .and_then(|count| count.checked_sub(deleted_nodes.len() as u64))
        .ok_or_else(|| storage_err("node live-count delta is invalid"))?;
    manifest.live_edge_count = manifest
        .live_edge_count
        .checked_add(edges.len() as u64)
        .and_then(|count| count.checked_sub(deleted_edges.len() as u64))
        .ok_or_else(|| storage_err("edge live-count delta is invalid"))?;
    manifest
        .runs
        .sort_unstable_by_key(|run| run.first_generation);
    publish_manifest(&root, staging, &manifest)?;
    cleanup_superseded_files(&root, prior_files, &manifest)?;
    metrics.retained_runs = manifest.runs.len();
    Ok(metrics)
}

fn manifest_file_names(manifest: &Manifest) -> BTreeSet<String> {
    manifest
        .runs
        .iter()
        .flat_map(|run| {
            [
                run.identities.name.clone(),
                run.node_surrogates.name.clone(),
            ]
        })
        .collect()
}

#[cfg(test)]
fn cleanup_superseded_files(
    root: &Path,
    prior: BTreeSet<String>,
    manifest: &Manifest,
) -> Result<(), GfError> {
    let retained = manifest_file_names(manifest);
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    for name in prior.difference(&retained) {
        let file = directory
            .open_child_file(std::ffi::OsStr::new(name))
            .map_err(storage_err)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        directory
            .unlink_child_if_identity(std::ffi::OsStr::new(name), identity)
            .map_err(storage_err)?;
    }
    directory.sync().map_err(storage_err)
}

#[cfg(test)]
fn reject_identity_collisions(
    mut retained: BufReader<File>,
    incoming: &[(Uuid, u8, u64)],
) -> Result<u64, GfError> {
    let mut incoming_index = 0;
    let mut bytes = 0_u64;
    while incoming_index < incoming.len() {
        let Some(record) = read_exact_record::<32>(&mut retained)? else {
            break;
        };
        bytes += IDENTITY_RECORD_BYTES;
        let retained_uuid = &record[..16];
        while incoming_index < incoming.len()
            && incoming[incoming_index].0.as_bytes().as_slice() < retained_uuid
        {
            incoming_index += 1;
        }
        if incoming_index < incoming.len()
            && incoming[incoming_index].0.as_bytes().as_slice() == retained_uuid
        {
            let incoming_record = incoming[incoming_index];
            let retained_kind = record[16];
            let retained_surrogate = u64::from_be_bytes(record[24..32].try_into().expect("fixed"));
            if matches!(incoming_record.1, 2 | 3)
                && incoming_record.1 - 2 == retained_kind
                && incoming_record.2 == retained_surrogate
            {
                continue;
            }
            return Err(storage_err(
                "UUID already exists in an authenticated retained run",
            ));
        }
    }
    Ok(bytes)
}

#[cfg(test)]
fn reject_surrogate_collisions(
    mut retained: BufReader<File>,
    incoming: &[(u64, Uuid)],
) -> Result<u64, GfError> {
    let mut incoming_index = 0;
    let mut bytes = 0_u64;
    while incoming_index < incoming.len() {
        let Some(record) = read_exact_record::<24>(&mut retained)? else {
            break;
        };
        bytes += NODE_LOOKUP_RECORD_BYTES;
        let retained_surrogate = u64::from_be_bytes(record[..8].try_into().expect("fixed"));
        while incoming_index < incoming.len() && incoming[incoming_index].0 < retained_surrogate {
            incoming_index += 1;
        }
        if incoming_index < incoming.len() && incoming[incoming_index].0 == retained_surrogate {
            if incoming[incoming_index].1.as_bytes() == &record[8..24] {
                continue;
            }
            return Err(storage_err(
                "node surrogate already exists in an authenticated retained run",
            ));
        }
    }
    Ok(bytes)
}

fn binary_search_surrogate_uuid(
    file: &mut File,
    count: u64,
    target: u64,
    seeks: &mut u64,
) -> Result<Option<[u8; 16]>, GfError> {
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * NODE_LOOKUP_RECORD_BYTES))
            .map_err(storage_err)?;
        *seeks += 1;
        let mut record = [0_u8; 24];
        file.read_exact(&mut record).map_err(storage_err)?;
        match u64::from_be_bytes(record[..8].try_into().expect("fixed")).cmp(&target) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Some(record[8..].try_into().expect("fixed"))),
        }
    }
    Ok(None)
}

fn write_identity_records(path: &Path, records: &[(Uuid, u8, u64)]) -> Result<(), GfError> {
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = File::create(path).map_err(storage_err)?;
    for (uuid, kind, surrogate) in records {
        if bytes.len() + 32 > BULK_IO_BYTES {
            file.write_all(&bytes).map_err(storage_err)?;
            bytes.clear();
        }
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.push(*kind);
        bytes.extend_from_slice(&[0; 7]);
        bytes.extend_from_slice(&surrogate.to_be_bytes());
    }
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
    }
    file.sync_all().map_err(storage_err)
}

fn write_surrogate_records(path: &Path, records: &[(u64, Uuid)]) -> Result<(), GfError> {
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = File::create(path).map_err(storage_err)?;
    for (surrogate, uuid) in records {
        if bytes.len() + 24 > BULK_IO_BYTES {
            file.write_all(&bytes).map_err(storage_err)?;
            bytes.clear();
        }
        bytes.extend_from_slice(&surrogate.to_be_bytes());
        bytes.extend_from_slice(uuid.as_bytes());
    }
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
    }
    file.sync_all().map_err(storage_err)
}

fn publish_manifest(root: &Path, staging: &Path, manifest: &Manifest) -> Result<(), GfError> {
    let _ = staging;
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    let temp_name = std::ffi::OsString::from(format!(".manifest-{}.tmp", Uuid::new_v4()));
    let mut temp = directory
        .create_child_file(&temp_name)
        .map_err(storage_err)?;
    let identity = graphforge_filesystem::file_identity(&temp).map_err(storage_err)?;
    let result = (|| -> Result<(), GfError> {
        serde_json::to_writer(&mut temp, manifest).map_err(storage_err)?;
        temp.flush().map_err(storage_err)?;
        temp.sync_all().map_err(storage_err)?;
        directory
            .replace_child(&temp_name, identity, std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        directory.sync().map_err(storage_err)
    })();
    if result.is_err() {
        let _ = directory.unlink_child_if_identity(&temp_name, identity);
    }
    result
}

#[cfg(test)]
fn compact_manifest_levels(
    root: &Path,
    staging: &Path,
    scratch: &Path,
    manifest: &mut Manifest,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    for level in 0..63_u8 {
        loop {
            let mut indexes = manifest
                .runs
                .iter()
                .enumerate()
                .filter(|(_, run)| !run.base && run.level == level)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if indexes.len() < 2 {
                break;
            }
            indexes.sort_unstable_by_key(|index| manifest.runs[*index].first_generation);
            let right = manifest.runs.remove(indexes[1]);
            let left = manifest.runs.remove(indexes[0]);
            if left.last_generation.saturating_add(1) != right.first_generation {
                return Err(storage_err("equal-level runs are not adjacent"));
            }
            let identity_path = scratch.join(format!("identities-level-{}.run", level + 1));
            let surrogate_path = scratch.join(format!("surrogates-level-{}.run", level + 1));
            merge_identity_v3(
                &[
                    root.join(&left.identities.name),
                    root.join(&right.identities.name),
                ],
                &identity_path,
            )?;
            merge_surrogate_runs(
                &[
                    root.join(&left.node_surrogates.name),
                    root.join(&right.node_surrogates.name),
                ],
                &surrogate_path,
            )?;
            let identities = publish_data(
                &identity_path,
                root,
                staging,
                &format!("identities-v3-l{}", level + 1),
                right.last_generation,
                IDENTITY_RECORD_BYTES,
            )?;
            let node_surrogates = publish_data(
                &surrogate_path,
                root,
                staging,
                &format!("node-surrogates-v3-l{}", level + 1),
                right.last_generation,
                NODE_LOOKUP_RECORD_BYTES,
            )?;
            let bytes = identities.count * IDENTITY_RECORD_BYTES
                + node_surrogates.count * NODE_LOOKUP_RECORD_BYTES;
            metrics.physical_bytes_written = metrics.physical_bytes_written.saturating_add(bytes);
            metrics.write_bytes = metrics.write_bytes.saturating_add(bytes);
            metrics.write_blocks = metrics
                .write_blocks
                .saturating_add(bytes.div_ceil(BULK_IO_BYTES as u64));
            let (node_count, edge_count, deleted_node_count, deleted_edge_count) =
                count_identity_states(&root.join(&identities.name))?;
            manifest.runs.push(RunRecord {
                base: false,
                level: level + 1,
                first_generation: left.first_generation,
                last_generation: right.last_generation,
                identities,
                node_surrogates,
                node_count,
                edge_count,
                deleted_node_count,
                deleted_edge_count,
            });
        }
    }
    Ok(())
}

fn count_identity_states(path: &Path) -> Result<(u64, u64, u64, u64), GfError> {
    let mut reader =
        BufReader::with_capacity(BULK_IO_BYTES, File::open(path).map_err(storage_err)?);
    let mut counts = [0_u64; 4];
    while let Some(record) = read_exact_record::<32>(&mut reader)? {
        let kind = usize::from(record[16]);
        if kind >= counts.len() {
            return Err(storage_err("invalid compacted identity kind"));
        }
        counts[kind] += 1;
    }
    Ok((counts[0], counts[1], counts[2], counts[3]))
}

#[cfg(test)]
fn merge_identity_v3(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 32], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_exact_record::<32>(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let uuid: [u8; 16] = record[..16].try_into().expect("fixed");
        let mut newest_index = index;
        if let Some(next) = read_exact_record::<32>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..16] == uuid)
        {
            let Reverse((candidate, candidate_index)) = heap.pop().expect("peeked");
            if candidate_index > newest_index {
                record = candidate;
                newest_index = candidate_index;
            }
            if let Some(next) = read_exact_record::<32>(&mut readers[candidate_index])? {
                heap.push(Reverse((next, candidate_index)));
            }
        }
        if block.len() + 32 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&record);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

fn validate_run_descriptors(manifest: &Manifest) -> Result<(), GfError> {
    for record in manifest.runs.iter().flat_map(|run| {
        [
            (&run.identities, IDENTITY_RECORD_BYTES),
            (&run.node_surrogates, NODE_LOOKUP_RECORD_BYTES),
        ]
    }) {
        validate_block_records(record.0, record.1)?;
    }
    let mut levels = BTreeSet::new();
    let mut intervals = manifest
        .runs
        .iter()
        .map(|run| (run.first_generation, run.last_generation))
        .collect::<Vec<_>>();
    intervals.sort_unstable();
    let bases = manifest
        .runs
        .iter()
        .filter(|run| run.base)
        .collect::<Vec<_>>();
    if bases.len() != 1
        || bases[0].first_generation != 0
        || bases[0].last_generation != manifest.base_generation
        || manifest
            .runs
            .iter()
            .filter(|run| !run.base)
            .any(|run| run.first_generation > run.last_generation || !levels.insert(run.level))
    {
        return Err(storage_err(
            "manifest runs violate canonical level/interval policy",
        ));
    }
    if intervals.last().map(|interval| interval.1).unwrap_or(0) != manifest.current_generation
        || intervals
            .windows(2)
            .any(|pair| pair[0].1.saturating_add(1) != pair[1].0)
    {
        return Err(storage_err(
            "manifest generation intervals are not contiguous",
        ));
    }
    Ok(())
}

fn binary_search_identity_state(
    file: &mut File,
    count: u64,
    target: Uuid,
    expected_kind: UuidIndexKind,
    seeks: &mut u64,
) -> Result<Option<(bool, u64)>, GfError> {
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * IDENTITY_RECORD_BYTES))
            .map_err(storage_err)?;
        *seeks += 1;
        let mut record = [0_u8; 32];
        file.read_exact(&mut record).map_err(storage_err)?;
        match record[..16].cmp(target.as_bytes()) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                let kind = if matches!(record[16], 0 | 2) {
                    UuidIndexKind::Node
                } else {
                    UuidIndexKind::Edge
                };
                if kind != expected_kind {
                    return Ok(Some((false, 0)));
                }
                return Ok(Some((
                    matches!(record[16], 0 | 1),
                    u64::from_be_bytes(record[24..32].try_into().expect("fixed record")),
                )));
            }
        }
    }
    Ok(None)
}

fn validate_run_contents(
    identities: File,
    surrogates: File,
    descriptor: &RunRecord,
) -> Result<(), GfError> {
    let mut identities = BufReader::with_capacity(BULK_IO_BYTES, identities);
    let mut surrogates = BufReader::with_capacity(BULK_IO_BYTES, surrogates);
    let mut previous_uuid = None;
    let mut node_count = 0_u64;
    let mut edge_count = 0_u64;
    let mut deleted_node_count = 0_u64;
    let mut deleted_edge_count = 0_u64;
    for _ in 0..descriptor.identities.count {
        let mut record = [0_u8; 32];
        identities.read_exact(&mut record).map_err(storage_err)?;
        let uuid: [u8; 16] = record[..16].try_into().expect("fixed record");
        if previous_uuid.is_some_and(|previous| previous >= uuid) || record[17..24] != [0_u8; 7] {
            return Err(storage_err(
                "identity run is not canonical and strictly sorted",
            ));
        }
        previous_uuid = Some(uuid);
        let surrogate = u64::from_be_bytes(record[24..32].try_into().expect("fixed record"));
        match record[16] {
            0 if surrogate != 0 => node_count += 1,
            1 if surrogate == 0 => edge_count += 1,
            2 if surrogate != 0 => deleted_node_count += 1,
            3 if surrogate == 0 => deleted_edge_count += 1,
            _ => return Err(storage_err("identity run contains an invalid kind")),
        }
    }
    if node_count != descriptor.node_count
        || edge_count != descriptor.edge_count
        || deleted_node_count != descriptor.deleted_node_count
        || deleted_edge_count != descriptor.deleted_edge_count
        || descriptor.identities.count
            != node_count + edge_count + deleted_node_count + deleted_edge_count
        || descriptor.node_surrogates.count != node_count + deleted_node_count
    {
        return Err(storage_err("run descriptor counts do not reconcile"));
    }
    let mut previous_surrogate = None;
    for _ in 0..node_count + deleted_node_count {
        let mut record = [0_u8; 24];
        surrogates.read_exact(&mut record).map_err(storage_err)?;
        let surrogate = u64::from_be_bytes(record[..8].try_into().expect("fixed record"));
        if previous_surrogate.is_some_and(|previous| previous >= surrogate) {
            return Err(storage_err("surrogate run is not strictly sorted"));
        }
        previous_surrogate = Some(surrogate);
    }
    Ok(())
}

fn open_verified(root: &Path, record: &FileRecord, record_bytes: u64) -> Result<File, GfError> {
    if Path::new(&record.name).components().count() != 1 {
        return Err(storage_err("manifest contains a non-local index filename"));
    }
    let path = root.join(&record.name);
    let mut file = File::open(&path).map_err(storage_err)?;
    let expected_len = record
        .count
        .checked_mul(record_bytes)
        .ok_or_else(|| storage_err("record length overflow"))?;
    if file.metadata().map_err(storage_err)?.len() != expected_len {
        return Err(storage_err(format!(
            "length mismatch for {}",
            path.display()
        )));
    }
    authenticate_file_blocks(&mut file, record, record_bytes, None)?;
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    Ok(file)
}

/// Explicit bounded rebuild/migration path. Immutable data files are completed
/// and synced first; `manifest.json` is atomically replaced last.
pub fn rebuild_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    let limits = limits.validate()?;
    let root = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&root).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-build-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let mut metrics = UuidIndexBuildMetrics::default();
    let generation = crate::read_topology_generation(project_dir)?;
    let node_paths = crate::mutator::node_parquet_files(project_dir).map_err(storage_err)?;
    let node_runs = scan_to_runs(
        &node_paths,
        "node_uuid",
        scratch.path(),
        "node",
        limits,
        &mut metrics,
    )?;
    let node_surrogate_runs = scan_entity_surrogate_runs(
        &node_paths,
        "node_uuid",
        "node_id",
        "node",
        scratch.path(),
        limits,
        &mut metrics,
    )?;
    let node_surrogate_validation_runs =
        scan_node_surrogate_validation_runs(&node_paths, scratch.path(), limits, &mut metrics)?;
    let mut edge_paths = crate::mutator::edge_parquet_files(project_dir, None)
        .map_err(storage_err)?
        .into_iter()
        .map(|(_, path)| path)
        .collect::<Vec<_>>();
    edge_paths.sort();
    let edge_runs = scan_to_runs(
        &edge_paths,
        "edge_uuid",
        scratch.path(),
        "edge",
        limits,
        &mut metrics,
    )?;
    let node_tmp = merge_all(
        node_runs,
        scratch.path(),
        "nodes",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let node_surrogates_tmp = merge_node_surrogate_runs(
        node_surrogate_runs,
        scratch.path(),
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let validated_surrogates = merge_node_surrogate_validation_runs(
        node_surrogate_validation_runs,
        scratch.path(),
        limits.merge_fan_in,
        &mut metrics,
    )?;
    fs::remove_file(validated_surrogates).map_err(storage_err)?;
    let edge_tmp = merge_all(
        edge_runs,
        scratch.path(),
        "edges",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    if crate::read_topology_generation(project_dir)? != generation {
        return Err(storage_err(
            "topology generation changed during the index build",
        ));
    }
    reject_cross_kind_identities(&node_tmp, &edge_tmp)?;
    let identity_tmp = scratch.path().join("identities-v3.run");
    build_identity_run(&node_surrogates_tmp, &edge_tmp, &identity_tmp)?;
    let surrogate_tmp =
        build_surrogate_run(&node_surrogates_tmp, scratch.path(), limits, &mut metrics)?;
    let identities = publish_data(
        &identity_tmp,
        &root,
        staging,
        "identities-v3",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let node_surrogates = publish_data(
        &surrogate_tmp,
        &root,
        staging,
        "node-surrogates-v3",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    if crate::read_topology_generation(project_dir)? != generation {
        return Err(storage_err(
            "topology generation changed before index manifest publication",
        ));
    }
    metrics.node_count = node_surrogates.count;
    metrics.edge_count = identities.count.saturating_sub(metrics.node_count);
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        base_generation: generation,
        current_generation: generation,
        live_node_count: metrics.node_count,
        live_edge_count: metrics.edge_count,
        runs: vec![RunRecord {
            base: true,
            level: 0,
            first_generation: 0,
            last_generation: generation,
            identities,
            node_surrogates,
            node_count: metrics.node_count,
            edge_count: metrics.edge_count,
            deleted_node_count: 0,
            deleted_edge_count: 0,
        }],
    };
    publish_manifest(&root, staging, &manifest)?;
    Ok(metrics)
}

fn scan_to_runs(
    paths: &[PathBuf],
    column: &str,
    scratch: &Path,
    prefix: &str,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<[u8; 16]>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        if !path.exists() {
            continue;
        }
        let file = File::open(path).map_err(storage_err)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(storage_err)?
            .with_batch_size(limits.scan_batch_rows)
            .build()
            .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let array = batch
                .column_by_name(column)
                .ok_or_else(|| storage_err(format!("{} lacks {column}", path.display())))?
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| storage_err(format!("{column} is not FixedSizeBinary")))?;
            for row in 0..array.len() {
                if array.is_null(row) || array.value(row).len() != 16 {
                    return Err(storage_err(format!("invalid {column} at row {row}")));
                }
                buffer.push(array.value(row).try_into().expect("length checked"));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn build_identity_run(nodes: &Path, edges: &Path, output: &Path) -> Result<(), GfError> {
    let mut node_reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut edge_reader = BufReader::new(File::open(edges).map_err(storage_err)?);
    let mut node = read_node_surrogate_record(&mut node_reader)?;
    let mut edge = read_record(&mut edge_reader)?;
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while node.is_some() || edge.is_some() {
        let take_node = match (&node, &edge) {
            (Some((node_uuid, _)), Some(edge_uuid)) => {
                if node_uuid == edge_uuid {
                    return Err(storage_err("UUID occurs in both identity domains"));
                }
                node_uuid < edge_uuid
            }
            (Some(_), None) => true,
            _ => false,
        };
        let (uuid, surrogate, kind) = if take_node {
            let (uuid, surrogate) = node.take().expect("node present");
            node = read_node_surrogate_record(&mut node_reader)?;
            (uuid, surrogate, 0_u8)
        } else {
            let uuid = edge.take().expect("edge present");
            edge = read_record(&mut edge_reader)?;
            (uuid, 0, 1_u8)
        };
        let mut record = [0_u8; 32];
        record[..16].copy_from_slice(&uuid);
        record[16] = kind;
        record[24..].copy_from_slice(&surrogate.to_be_bytes());
        if block.len() + 32 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&record);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

fn build_surrogate_run(
    nodes: &Path,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut buffer = Vec::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    while let Some((uuid, surrogate)) = read_node_surrogate_record(&mut reader)? {
        buffer.push((surrogate, uuid));
        metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
        if buffer.len() == limits.run_records {
            flush_surrogate_run(&mut buffer, scratch, &mut runs, metrics)?;
        }
    }
    if !buffer.is_empty() {
        flush_surrogate_run(&mut buffer, scratch, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("surrogates-empty.run");
        File::create(&path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        runs.push(path);
    }
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, inputs) in runs.chunks(limits.merge_fan_in).enumerate() {
            let output = scratch.join(format!("surrogates-merge-{round}-{group}.run"));
            merge_surrogate_runs(inputs, &output)?;
            next.push(output);
        }
        for run in runs {
            let _ = fs::remove_file(run);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("surrogate run exists"))
}

fn flush_surrogate_run(
    buffer: &mut Vec<(u64, [u8; 16])>,
    scratch: &Path,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("duplicate node surrogate"));
    }
    let path = scratch.join(format!("surrogates-{:08}.run", runs.len()));
    let mut bytes = Vec::with_capacity(buffer.len() * 24);
    for (surrogate, uuid) in buffer.iter() {
        bytes.extend_from_slice(&surrogate.to_be_bytes());
        bytes.extend_from_slice(uuid);
    }
    let mut file = File::create(&path).map_err(storage_err)?;
    file.write_all(&bytes).map_err(storage_err)?;
    file.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_surrogate_runs(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<((u64, [u8; 16]), usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_surrogate_record(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse(((surrogate, uuid), index))) = heap.pop() {
        if previous.is_some_and(|(prior, _)| prior == surrogate) {
            if previous.is_some_and(|(_, prior_uuid)| prior_uuid == uuid) {
                if let Some(record) = read_surrogate_record(&mut readers[index])? {
                    heap.push(Reverse((record, index)));
                }
                continue;
            }
            return Err(storage_err("duplicate node surrogate across runs"));
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&surrogate.to_be_bytes());
        block.extend_from_slice(&uuid);
        previous = Some((surrogate, uuid));
        if let Some(record) = read_surrogate_record(&mut readers[index])? {
            heap.push(Reverse((record, index)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

fn read_surrogate_record(reader: &mut BufReader<File>) -> Result<Option<(u64, [u8; 16])>, GfError> {
    let Some(record) = read_exact_record::<24>(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        u64::from_be_bytes(record[..8].try_into().expect("fixed")),
        record[8..].try_into().expect("fixed"),
    )))
}

fn read_exact_record<const N: usize>(reader: &mut impl Read) -> Result<Option<[u8; N]>, GfError> {
    let mut record = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        match reader.read(&mut record[filled..]).map_err(storage_err)? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(storage_err("truncated fixed-width index record")),
            read => filled += read,
        }
    }
    Ok(Some(record))
}

fn flush_run(
    buffer: &mut Vec<[u8; 16]>,
    scratch: &Path,
    prefix: &str,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(format!(
            "duplicate {prefix} UUID in canonical topology"
        )));
    }
    let path = scratch.join(format!("{prefix}-{:08}.run", runs.len()));
    let mut out = File::create(&path).map_err(storage_err)?;
    let mut block = Vec::with_capacity(buffer.len().min(BULK_IO_BYTES / 16) * 16);
    for value in buffer.iter() {
        block.extend_from_slice(value);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_all(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    prefix: &str,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!("{prefix}-merge-{round}-{group}.run"));
            merge_runs(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("at least one run"))
}

fn merge_runs(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|p| File::open(p).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 16], usize)>>::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_record(reader)? {
            heap.push(Reverse((value, idx)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse((value, idx))) = heap.pop() {
        if previous == Some(value) {
            return Err(storage_err("duplicate UUID across external index runs"));
        }
        if block.len() + 16 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&value);
        previous = Some(value);
        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(Reverse((next, idx)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    Ok(())
}

fn scan_entity_surrogate_runs(
    paths: &[PathBuf],
    uuid_column: &str,
    surrogate_column: &str,
    prefix: &str,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<([u8; 16], u64)>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let uuids = batch
                .column_by_name(uuid_column)
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| storage_err(format!("{} lacks {uuid_column}", path.display())))?;
            let surrogates = batch
                .column_by_name(surrogate_column)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    storage_err(format!("{} lacks {surrogate_column}", path.display()))
                })?;
            if uuids.len() != surrogates.len() {
                return Err(storage_err(
                    "identity UUID and surrogate columns differ in length",
                ));
            }
            for row in 0..uuids.len() {
                if uuids.is_null(row) || uuids.value(row).len() != 16 || surrogates.is_null(row) {
                    return Err(storage_err(format!("invalid entity identity at row {row}")));
                }
                buffer.push((
                    uuids.value(row).try_into().expect("length checked"),
                    surrogates.value(row),
                ));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-surrogates-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn scan_node_surrogate_validation_runs(
    paths: &[PathBuf],
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<u64>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let surrogates = batch
                .column_by_name("node_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| storage_err(format!("{} lacks node_id", path.display())))?;
            for row in 0..surrogates.len() {
                if surrogates.is_null(row) || surrogates.value(row) == 0 {
                    return Err(storage_err(format!("invalid node surrogate at row {row}")));
                }
                buffer.push(surrogates.value(row));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_node_surrogate_validation_run(&mut buffer, scratch, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_node_surrogate_validation_run(&mut buffer, scratch, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("node-surrogate-validation-empty.run");
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn flush_node_surrogate_validation_run(
    buffer: &mut Vec<u64>,
    scratch: &Path,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(
            "duplicate node surrogate in canonical topology",
        ));
    }
    let path = scratch.join(format!("node-surrogate-validation-{:08}.run", runs.len()));
    let mut bytes = Vec::with_capacity(buffer.len() * 8);
    for surrogate in buffer.iter() {
        bytes.extend_from_slice(&surrogate.to_le_bytes());
    }
    let mut file = File::create(&path).map_err(storage_err)?;
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
    }
    file.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_node_surrogate_validation_runs(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!(
                "node-surrogate-validation-merge-{round}-{group}.run"
            ));
            merge_node_surrogate_validation_group(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("surrogate validation run exists"))
}

fn merge_node_surrogate_validation_group(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(u64, usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_validation_surrogate(reader)? {
            heap.push(Reverse((value, index)));
        }
    }
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut out = File::create(output).map_err(storage_err)?;
    let mut previous = None;
    while let Some(Reverse((value, index))) = heap.pop() {
        if previous == Some(value) {
            return Err(storage_err(
                "duplicate node surrogate across external index runs",
            ));
        }
        if bytes.len() + 8 > BULK_IO_BYTES {
            out.write_all(&bytes).map_err(storage_err)?;
            bytes.clear();
        }
        bytes.extend_from_slice(&value.to_le_bytes());
        previous = Some(value);
        if let Some(next) = read_validation_surrogate(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    if !bytes.is_empty() {
        out.write_all(&bytes).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)
}

fn read_validation_surrogate(reader: &mut impl Read) -> Result<Option<u64>, GfError> {
    Ok(read_exact_record::<8>(reader)?.map(u64::from_le_bytes))
}

fn flush_entity_surrogate_run(
    buffer: &mut Vec<([u8; 16], u64)>,
    scratch: &Path,
    prefix: &str,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable_by_key(|record| record.0);
    if buffer.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("duplicate UUID in canonical topology"));
    }
    let path = scratch.join(format!("{prefix}-surrogates-{:08}.run", runs.len()));
    let mut out = File::create(&path).map_err(storage_err)?;
    let mut block = Vec::with_capacity(buffer.len().min(BULK_IO_BYTES / 24) * 24);
    for (uuid, surrogate) in buffer.iter() {
        block.extend_from_slice(uuid);
        block.extend_from_slice(&surrogate.to_le_bytes());
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_node_surrogate_runs(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!("node-surrogates-merge-{round}-{group}.run"));
            merge_node_surrogate_group(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("at least one node-surrogate run"))
}

fn merge_node_surrogate_group(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(([u8; 16], u64), usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_node_surrogate_record(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse(((uuid, surrogate), index))) = heap.pop() {
        if previous == Some(uuid) {
            return Err(storage_err(
                "duplicate node UUID across external index runs",
            ));
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&uuid);
        block.extend_from_slice(&surrogate.to_le_bytes());
        previous = Some(uuid);
        if let Some(record) = read_node_surrogate_record(&mut readers[index])? {
            heap.push(Reverse((record, index)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    Ok(())
}

fn read_node_surrogate_record(
    reader: &mut BufReader<File>,
) -> Result<Option<([u8; 16], u64)>, GfError> {
    let Some(record) = read_exact_record::<24>(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        record[..16].try_into().expect("fixed"),
        u64::from_le_bytes(record[16..].try_into().expect("fixed")),
    )))
}

fn reject_cross_kind_identities(nodes: &Path, edges: &Path) -> Result<(), GfError> {
    let mut node_reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut edge_reader = BufReader::new(File::open(edges).map_err(storage_err)?);
    let mut node = read_record(&mut node_reader)?;
    let mut edge = read_record(&mut edge_reader)?;
    while let (Some(node_uuid), Some(edge_uuid)) = (node, edge) {
        match node_uuid.cmp(&edge_uuid) {
            std::cmp::Ordering::Less => node = read_record(&mut node_reader)?,
            std::cmp::Ordering::Greater => edge = read_record(&mut edge_reader)?,
            std::cmp::Ordering::Equal => {
                return Err(storage_err(
                    "UUID occurs in both node and edge identity domains",
                ));
            }
        }
    }
    Ok(())
}

fn read_record(reader: &mut BufReader<File>) -> Result<Option<[u8; 16]>, GfError> {
    read_exact_record::<16>(reader)
}

fn publish_data(
    source: &Path,
    root: &Path,
    _staging: &Path,
    kind: &str,
    generation: u64,
    record_bytes: u64,
) -> Result<FileRecord, GfError> {
    let length = source.metadata().map_err(storage_err)?.len();
    if length % record_bytes != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let mut input = File::open(source).map_err(storage_err)?;
    let (sha256, blocks) = describe_blocks(&mut input, record_bytes)?;
    let name = format!("{kind}-{generation}-{}.uuidx", &sha256[..16]);
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    let target = std::ffi::OsStr::new(&name);
    if let Ok(mut existing) = directory.open_child_file(target) {
        if existing.metadata().map_err(storage_err)?.len() != length
            || sha256_reader(&mut existing)? != sha256
        {
            return Err(storage_err(
                "existing immutable run does not match its content name",
            ));
        }
    } else {
        let temp_name = std::ffi::OsString::from(format!(".run-{}.tmp", Uuid::new_v4()));
        let mut temp = directory
            .create_child_file(&temp_name)
            .map_err(storage_err)?;
        let temp_identity = graphforge_filesystem::file_identity(&temp).map_err(storage_err)?;
        let mut install = || -> Result<(), GfError> {
            let mut input = File::open(source).map_err(storage_err)?;
            std::io::copy(&mut input, &mut temp).map_err(storage_err)?;
            temp.sync_all().map_err(storage_err)?;
            match directory.link_child_into(&temp_name, &temp, temp_identity, &directory, target) {
                Ok(_) => Ok(()),
                Err(_) => {
                    let mut existing = directory.open_child_file(target).map_err(storage_err)?;
                    if existing.metadata().map_err(storage_err)?.len() != length
                        || sha256_reader(&mut existing)? != sha256
                    {
                        return Err(storage_err("concurrent immutable run mismatch"));
                    }
                    Ok(())
                }
            }
        };
        let result = install();
        let _ = directory.unlink_child_if_identity(&temp_name, temp_identity);
        result?;
        directory.sync().map_err(storage_err)?;
    }
    Ok(FileRecord {
        name,
        count: length / record_bytes,
        sha256,
        blocks,
    })
}

fn sha256_reader(reader: &mut impl Read) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(storage_err)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use arrow::array::{FixedSizeBinaryArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    #[test]
    fn fixed_width_codecs_distinguish_clean_eof_from_partial_tail() {
        let mut clean = std::io::Cursor::new(Vec::<u8>::new());
        assert_eq!(read_exact_record::<32>(&mut clean).unwrap(), None);
        for length in 1..32 {
            let mut partial = std::io::Cursor::new(vec![0_u8; length]);
            assert!(read_exact_record::<32>(&mut partial).is_err());
        }
        for length in 1..24 {
            let mut partial = std::io::Cursor::new(vec![0_u8; length]);
            assert!(read_exact_record::<24>(&mut partial).is_err());
        }
    }

    #[test]
    fn unified_identity_merge_rejects_cross_kind_uuid() {
        let scratch = tempfile::tempdir().unwrap();
        let uuid = [7_u8; 16];
        let node = scratch.path().join("node.run");
        let edge = scratch.path().join("edge.run");
        for path in [&node, &edge] {
            let mut file = File::create(path).unwrap();
            file.write_all(&uuid).unwrap();
            file.write_all(&1_u64.to_le_bytes()).unwrap();
        }
        assert!(build_identity_run(&node, &edge, &scratch.path().join("out.run")).is_err());
    }

    fn write_uuid_parquet(path: &Path, column: &str, values: &[Uuid]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let array = FixedSizeBinaryArray::try_from_iter(
            values.iter().map(|value| value.as_bytes().as_slice()),
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_node_parquet(path: &Path, values: &[Uuid]) {
        let ids = (1..=values.len() as u64).collect::<Vec<_>>();
        write_node_parquet_with_ids(path, values, &ids);
    }

    fn write_node_parquet_with_ids(path: &Path, values: &[Uuid], ids: &[u64]) {
        assert_eq!(values.len(), ids.len());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("node_id", DataType::UInt64, false),
        ]));
        let uuids = FixedSizeBinaryArray::try_from_iter(
            values.iter().map(|value| value.as_bytes().as_slice()),
        )
        .unwrap();
        let ids = UInt64Array::from_iter_values(ids.iter().copied());
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(uuids), Arc::new(ids)]).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn fixture() -> (tempfile::TempDir, Vec<Uuid>, Vec<Uuid>) {
        let dir = tempfile::tempdir().unwrap();
        let nodes = vec![Uuid::from_u128(3), Uuid::from_u128(1), Uuid::from_u128(2)];
        let edges = vec![Uuid::from_u128(12), Uuid::from_u128(11)];
        write_node_parquet(&dir.path().join("topology/nodes.parquet"), &nodes);
        write_uuid_parquet(
            &dir.path().join("topology/edges/R.parquet"),
            "edge_uuid",
            &edges,
        );
        (dir, nodes, edges)
    }

    #[test]
    fn bounded_build_reopens_and_probes_in_caller_order() {
        let (dir, nodes, _) = fixture();
        let metrics = rebuild_uuid_membership_indexes(
            dir.path(),
            UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap();
        assert_eq!((metrics.node_count, metrics.edge_count), (3, 2));
        assert_eq!(metrics.peak_buffered_records, 1);
        let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
        let missing = Uuid::from_u128(99);
        let (found, probe) = index
            .probe(UuidIndexKind::Node, &[nodes[1], missing, nodes[1]])
            .unwrap();
        assert_eq!(found, vec![true, false, true]);
        let (surrogates, lookup) = index
            .lookup_node_surrogates(&[nodes[1], missing, nodes[0]])
            .unwrap();
        assert_eq!(surrogates, vec![Some(2), None, Some(1)]);
        assert_eq!(lookup.found, 2);
        assert_eq!(
            (probe.requested, probe.unique_requested, probe.found),
            (3, 2, 1)
        );
    }

    #[test]
    fn probe_work_is_candidate_logarithmic_not_index_linear() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = (1..=8_192).map(Uuid::from_u128).collect::<Vec<_>>();
        write_node_parquet(&dir.path().join("topology/nodes.parquet"), &nodes);
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();

        let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
        let (found, metrics) = index
            .probe(UuidIndexKind::Node, &[nodes[4_095], Uuid::from_u128(9_000)])
            .unwrap();
        assert_eq!(found, [true, false]);
        assert_eq!(metrics.unique_requested, 2);
        assert!(
            metrics.file_seeks <= 28,
            "two probes in 8192 records require at most 2 * (log2(8192) + 1) seeks"
        );
    }

    #[test]
    fn corrupt_data_fails_closed_without_replacing_manifest() {
        let (dir, _, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let root = dir.path().join(INDEX_DIR);
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(root.join(&manifest.runs[0].identities.name))
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        assert!(
            UuidMembershipIndex::open(dir.path())
                .unwrap_err()
                .to_string()
                .contains("authentication failed")
        );
    }

    #[test]
    fn retained_snapshot_rehashes_manifest_and_authenticates_only_candidate_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = (1_u64..=40_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();
        crate::generation::bump_topology_generation(dir.path()).unwrap();
        append_uuid_membership_delta(dir.path(), 1, &nodes, &[]).unwrap();
        let root = dir.path().join(INDEX_DIR);
        let mut snapshot =
            AuthenticatedUuidIndexSnapshot::open_at_generation(dir.path(), 1).unwrap();

        let manifest_path = root.join(MANIFEST);
        let original_manifest = fs::read(&manifest_path).unwrap();
        fs::write(
            &manifest_path,
            [original_manifest.as_slice(), b"\n"].concat(),
        )
        .unwrap();
        assert!(
            snapshot
                .revalidate()
                .unwrap_err()
                .to_string()
                .contains("manifest authentication")
        );
        fs::write(&manifest_path, original_manifest).unwrap();

        let run_path = root.join(
            &snapshot
                .manifest
                .runs
                .iter()
                .find(|run| run.identities.count > 0)
                .unwrap()
                .identities
                .name,
        );
        let mut run = fs::OpenOptions::new().write(true).open(run_path).unwrap();
        run.seek(SeekFrom::Start(0)).unwrap();
        run.write_all(&[0xff]).unwrap();
        run.sync_all().unwrap();
        let scratch = tempfile::tempdir_in(dir.path()).unwrap();
        let error = plan_uuid_membership_delta(
            &root,
            1,
            2,
            Some(&mut snapshot),
            scratch.path(),
            &[(nodes[0].0, nodes[0].1)],
            &[],
            &[],
            &[],
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("block authentication"),
            "{error}"
        );
    }

    #[test]
    fn missing_and_stale_manifests_fail_closed() {
        let (dir, _, _) = fixture();
        assert!(!uuid_membership_index_is_fresh(dir.path()).unwrap());
        assert!(UuidMembershipIndex::open(dir.path()).is_err());
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        assert!(uuid_membership_index_is_fresh(dir.path()).unwrap());
        crate::generation::bump_topology_generation(dir.path()).unwrap();
        assert!(!uuid_membership_index_is_fresh(dir.path()).unwrap());
        assert!(
            UuidMembershipIndex::open(dir.path())
                .unwrap_err()
                .to_string()
                .contains("stale index generation")
        );
    }

    #[test]
    fn unpublished_build_artifacts_do_not_change_concurrent_readers() {
        let (dir, nodes, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let reader_root = dir.path().to_path_buf();
        let reader_barrier = barrier.clone();
        let expected = nodes[0];
        let reader = std::thread::spawn(move || {
            let mut index = UuidMembershipIndex::open(&reader_root).unwrap();
            reader_barrier.wait();
            index.probe(UuidIndexKind::Node, &[expected]).unwrap().0
        });
        fs::write(
            dir.path().join(INDEX_DIR).join("nodes-unpublished.uuidx"),
            [7_u8; 16],
        )
        .unwrap();
        barrier.wait();
        assert_eq!(reader.join().unwrap(), vec![true]);
        let mut reopened = UuidMembershipIndex::open(dir.path()).unwrap();
        assert_eq!(
            reopened.probe(UuidIndexKind::Node, &[expected]).unwrap().0,
            vec![true]
        );
    }

    #[test]
    fn concurrent_rebuilds_publish_one_authenticated_snapshot() {
        let (dir, _, _) = fixture();
        let root = Arc::new(dir.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let root = root.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    rebuild_uuid_membership_indexes(
                        &root,
                        UuidIndexBuildLimits {
                            scan_batch_rows: 1,
                            run_records: 1,
                            merge_fan_in: 2,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }
        let index = UuidMembershipIndex::open(&root).unwrap();
        assert_eq!(index.count(UuidIndexKind::Node), 3);
        assert_eq!(index.count(UuidIndexKind::Edge), 2);
        let names = fs::read_dir(root.join(INDEX_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(names.iter().all(|name| !name.ends_with(".tmp")));
    }

    #[test]
    fn duplicate_and_cross_kind_identities_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let repeated = Uuid::from_u128(7);
        write_node_parquet(
            &dir.path().join("topology/nodes.parquet"),
            &[repeated, repeated],
        );
        let duplicate = rebuild_uuid_membership_indexes(
            dir.path(),
            UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate"));

        let dir = tempfile::tempdir().unwrap();
        write_node_parquet(&dir.path().join("topology/nodes.parquet"), &[repeated]);
        write_uuid_parquet(
            &dir.path().join("topology/edges/R.parquet"),
            "edge_uuid",
            &[repeated],
        );
        let cross_kind =
            rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default())
                .unwrap_err();
        assert!(cross_kind.to_string().contains("both node and edge"));
    }

    fn singleton_append_series(batches: u64) -> (tempfile::TempDir, u64) {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = 0;
        for generation in 1..=batches {
            crate::generation::bump_topology_generation(dir.path()).unwrap();
            let metrics = append_uuid_membership_delta(
                dir.path(),
                generation,
                &[(Uuid::from_u128(u128::from(generation)), generation)],
                &[],
            )
            .unwrap();
            assert_eq!(metrics.prior_topology_rows_decoded, 0);
            assert!(metrics.write_blocks >= 2);
            assert_eq!(metrics.write_bytes, metrics.physical_bytes_written);
            bytes += metrics.physical_bytes_written;
        }
        (dir, bytes)
    }

    #[test]
    fn v3_leveled_append_has_bounded_runs_and_nonquadratic_doubling() {
        let (_, small) = singleton_append_series(64);
        let (large, large_bytes) = singleton_append_series(128);
        assert!(large_bytes <= small * 5 / 2);
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(large.path().join(INDEX_DIR).join(MANIFEST)).unwrap())
                .unwrap();
        assert!(manifest.runs.len() <= 9);
        assert_eq!(manifest.runs.iter().filter(|run| run.base).count(), 1);
        let mut index = UuidMembershipIndex::open(large.path()).unwrap();
        assert_eq!(index.count(UuidIndexKind::Node), 128);
        assert_eq!(
            index
                .lookup_node_surrogates(&[Uuid::from_u128(1), Uuid::from_u128(128)])
                .unwrap()
                .0,
            [Some(1), Some(128)]
        );
    }

    #[test]
    fn append_rejects_cross_run_uuid_and_surrogate_collisions_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        crate::generation::bump_topology_generation(dir.path()).unwrap();
        append_uuid_membership_delta(
            dir.path(),
            1,
            &[(Uuid::from_u128(1), 1)],
            &[Uuid::from_u128(2)],
        )
        .unwrap();
        let manifest_before = fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap();

        crate::generation::bump_topology_generation(dir.path()).unwrap();
        assert!(
            append_uuid_membership_delta(dir.path(), 2, &[], &[Uuid::from_u128(1)],)
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );
        assert_eq!(
            fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap(),
            manifest_before
        );
        assert!(
            append_uuid_membership_delta(dir.path(), 2, &[(Uuid::from_u128(3), 1)], &[],)
                .unwrap_err()
                .to_string()
                .contains("surrogate already exists")
        );
    }

    #[test]
    fn bulk_append_validation_uses_sequential_megabyte_blocks_and_zero_random_seeks() {
        let dir = tempfile::tempdir().unwrap();
        let retained = (1_u64..=40_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();
        crate::generation::bump_topology_generation(dir.path()).unwrap();
        append_uuid_membership_delta(dir.path(), 1, &retained, &[]).unwrap();

        crate::generation::bump_topology_generation(dir.path()).unwrap();
        let metrics =
            append_uuid_membership_delta(dir.path(), 2, &[(Uuid::from_u128(50_000), 50_000)], &[])
                .unwrap();
        assert_eq!(metrics.validation_random_seeks, 0);
        assert_eq!(metrics.validation_scan_bytes, 40_000 * (32 + 24));
        assert_eq!(metrics.validation_scan_blocks, 3);
    }

    #[test]
    fn retained_planner_stages_only_new_and_binary_carry_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(INDEX_DIR);
        let first_nodes = (1_u64..=40_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();

        let mut first = crate::RewriteBatch::new();
        prepare_uuid_membership_delta(
            dir.path(),
            0,
            1,
            None,
            &mut first,
            &first_nodes,
            &[],
            &[],
            &[],
        )
        .unwrap();
        // Two empty base files, two L0 files, manifest, and receipt. No copy of
        // any retained corpus exists on the initial plan.
        assert_eq!(first.staged_paths().count(), 6);
        first.commit().unwrap();
        fs::create_dir_all(dir.path().join("topology")).unwrap();
        fs::write(
            crate::generation::generation_path(dir.path()),
            crate::generation::encode_generation_state(1, 1).unwrap(),
        )
        .unwrap();
        let before = manifest_file_names(
            &serde_json::from_slice::<Manifest>(&fs::read(root.join(MANIFEST)).unwrap()).unwrap(),
        );

        let scratch = tempfile::tempdir_in(dir.path()).unwrap();
        let mut snapshot =
            AuthenticatedUuidIndexSnapshot::open_at_generation(dir.path(), 1).unwrap();
        let second_nodes = (40_001_u64..=80_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();
        let (planned, outputs, superseded, metrics) = plan_uuid_membership_delta(
            &root,
            1,
            2,
            Some(&mut snapshot),
            scratch.path(),
            &second_nodes,
            &[],
            &[],
            &[],
        )
        .unwrap();
        // Generation two carries L0+L0 into exactly one L1 pair. Retained base
        // files are descriptor-reused, not copied into planner outputs.
        assert_eq!(outputs.len(), 2);
        assert_eq!(superseded.len(), 2);
        assert_eq!(planned.runs.iter().filter(|run| !run.base).count(), 1);
        assert_eq!(planned.runs.iter().find(|run| !run.base).unwrap().level, 1);
        assert!(
            before
                .iter()
                .all(|name| !outputs.iter().any(|(out, _)| &out.name == name))
        );
        assert!(metrics.validation_scan_bytes > 0);
        assert_eq!(metrics.validation_random_seeks, 0);
        assert_eq!(metrics.prior_topology_rows_decoded, 0);
        assert!(metrics.snapshot_admission_authentication_bytes > 0);
        assert!(
            metrics.validation_scan_bytes <= metrics.snapshot_admission_authentication_bytes * 2
        );
        assert!(metrics.new_output_authentication_bytes <= metrics.physical_bytes_written);

        let mut install = crate::RewriteBatch::new();
        for (record, path) in &outputs {
            install.stage_file(&root.join(&record.name), path).unwrap();
        }
        install
            .stage_bytes(&root.join(MANIFEST), &serde_json::to_vec(&planned).unwrap())
            .unwrap();
        install.commit().unwrap();
        snapshot.advance_to(planned).unwrap();

        let scratch = tempfile::tempdir_in(dir.path()).unwrap();
        let (_, _, _, subsequent) = plan_uuid_membership_delta(
            &root,
            2,
            3,
            Some(&mut snapshot),
            scratch.path(),
            &[(Uuid::from_u128(80_001), 80_001)],
            &[],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(subsequent.snapshot_admission_authentication_bytes, 0);
        assert_eq!(subsequent.snapshot_admission_authentication_blocks, 0);
        assert_eq!(subsequent.validation_scan_bytes, 0);
        assert_eq!(subsequent.validation_scan_blocks, 0);
    }

    #[test]
    fn lookup_lazily_rejects_authenticated_pair_inconsistency() {
        let (dir, nodes, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let root = dir.path().join(INDEX_DIR);
        let mut manifest: Manifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        let run = &mut manifest.runs[0];
        let path = root.join(&run.node_surrogates.name);
        let mut bytes = fs::read(&path).unwrap();
        bytes[8..24].copy_from_slice(Uuid::from_u128(999).as_bytes());
        fs::write(&path, &bytes).unwrap();
        let mut file = File::open(&path).unwrap();
        let (sha256, blocks) = describe_blocks(&mut file, NODE_LOOKUP_RECORD_BYTES).unwrap();
        run.node_surrogates.sha256 = sha256;
        run.node_surrogates.blocks = blocks;
        fs::write(root.join(MANIFEST), serde_json::to_vec(&manifest).unwrap()).unwrap();

        // Ordinary open remains a bounded linear stream and does not perform
        // one random identity probe per surrogate record.
        let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
        assert!(
            index
                .lookup_node_surrogates(&[nodes[0]])
                .unwrap_err()
                .to_string()
                .contains("pair is inconsistent")
        );
    }

    #[test]
    fn duplicate_and_zero_node_surrogates_fail_closed_across_bounded_runs() {
        let limits = UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        };
        let dir = tempfile::tempdir().unwrap();
        let nodes = [Uuid::from_u128(1), Uuid::from_u128(2)];
        write_node_parquet_with_ids(&dir.path().join("topology/nodes.parquet"), &nodes, &[7, 7]);
        assert!(
            rebuild_uuid_membership_indexes(dir.path(), limits)
                .unwrap_err()
                .to_string()
                .contains("duplicate node surrogate")
        );

        let dir = tempfile::tempdir().unwrap();
        write_node_parquet_with_ids(
            &dir.path().join("topology/nodes.parquet"),
            &[Uuid::from_u128(3)],
            &[0],
        );
        assert!(
            rebuild_uuid_membership_indexes(dir.path(), limits)
                .unwrap_err()
                .to_string()
                .contains("invalid node surrogate")
        );
    }
}
