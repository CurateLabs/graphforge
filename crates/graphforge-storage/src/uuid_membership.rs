//! Persistent, bounded-memory UUID membership indexes used by bulk ingest.
//!
//! The canonical graph remains Parquet.  This derived format is deliberately
//! small: one manifest (published last with an atomic replacement) names an
//! immutable base plus size-tiered delta runs. Each run contains a unified
//! UUID-sorted identity file and a node-only surrogate-sorted reverse file.
//! Readers verify version, topology generation, framing, canonical ordering,
//! counts, and SHA-256 before serving bounded binary-search probes.

use self::probing::ProbeFileKind;
use self::probing::authenticated_probe_block;
use self::topology_delta::hex_sha256;
use graphforge_core::GfError;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

mod construction;
mod maintenance;
mod ordinal_artifacts;
mod ordinal_compaction;
mod probing;
mod rebuild;
mod topology_delta;

pub(crate) use construction::encode_construction_index;
pub(crate) use construction::is_exact_private_v4_name;
pub use maintenance::maintain_uuid_membership_orphans;
#[cfg(test)]
pub(crate) use maintenance::maintain_uuid_membership_orphans_with_ordinal_authority;
pub(crate) use ordinal_artifacts::V4ConstructionArtifactBundle;
pub(crate) use ordinal_artifacts::V4OrdinalConstructionWriter;
pub(crate) use ordinal_artifacts::V4OrdinalPublicationMetrics;
pub(crate) use ordinal_artifacts::publish_v4_construction_artifacts;
#[cfg(test)]
pub(crate) use ordinal_artifacts::stage_v4_ordinal_artifacts;
#[cfg(test)]
pub(crate) use probing::ConstructionUuidIdentity;
#[cfg(test)]
pub(crate) use probing::UuidConstructionSnapshot;
#[cfg(test)]
pub(crate) use probing::open_uuid_construction_snapshot;
#[cfg(test)]
pub(crate) use probing::pin_uuid_construction_snapshot;
pub(crate) use rebuild::ensure_uuid_membership_migrated;
pub use rebuild::rebuild_uuid_membership_indexes;
pub use rebuild::rebuild_v4_ordinal_identity;
pub use rebuild::rebuild_v4_ordinal_identity_with_evidence;
#[cfg(test)]
pub(crate) use topology_delta::append_uuid_membership_delta;
pub(crate) use topology_delta::commit_uuid_neutral_topology_rewrite;
pub(crate) use topology_delta::commit_uuid_topology_rewrite;
pub(crate) use topology_delta::prepare_uuid_membership_delta;
pub(crate) use topology_delta::prepare_v4_ordinal_delta;

mod identity_codec;

const FORMAT_VERSION: u32 = 5;
const NODE_LOOKUP_RECORD_BYTES: u64 = 24;
const IDENTITY_RECORD_BYTES: u64 = 25;
const NODE_LOOKUP_RECORD_WIDTH: usize = 24;
const IDENTITY_RECORD_WIDTH: usize = identity_codec::WIDTH;
const BULK_IO_BYTES: usize = 1 << 20;
// Persistent authenticated authority for UUID-to-surrogate resolution. Keeping
// it in the immutable topology generation is what lets writer reopen avoid
// decoding historical topology shards; `.graphforge-cache` is only for data
// that can be discarded and reconstructed without violating that contract.
const INDEX_DIR: &str = "topology/uuid-membership";
const MANIFEST: &str = "manifest.json";
const V4_ORDINAL_MANIFEST: &str = "ordinal-v4-manifest.json";
const V4_ORDINAL_RECEIPT: &str = "ordinal-v4-receipt.json";
const CONSTRUCTION_INTENT: &str = ".construction-intent.json";

fn storage_err(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("UUID membership index: {error}"))
}

fn open_uuid_file(path: &Path) -> Result<File, GfError> {
    let file = File::open(path).map_err(storage_err)?;
    crate::io_stats::record_uuid_file_open();
    Ok(file)
}

fn create_uuid_file(path: &Path) -> Result<File, GfError> {
    let file = File::create(path).map_err(storage_err)?;
    crate::io_stats::record_uuid_file_open();
    Ok(file)
}

fn sync_uuid_file(file: &File) -> Result<(), GfError> {
    let _wait = crate::concurrency_attribution::RegionScope::named("fsync");
    file.sync_all().map_err(storage_err)?;
    crate::io_stats::record_uuid_file_sync();
    Ok(())
}

fn open_uuid_child_file(
    directory: &graphforge_filesystem::StableDirectory,
    name: &std::ffi::OsStr,
) -> Result<File, GfError> {
    let file = directory.open_child_file(name).map_err(storage_err)?;
    crate::io_stats::record_uuid_file_open();
    Ok(file)
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

/// Explicit source disposition for a v4 ordinal rebuild.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V4OrdinalRebuildDisposition {
    /// Rebuilt from canonical topology; v3 reverse runs were not interpreted.
    CanonicalTopology,
}

/// Aggregate-only evidence for one explicit v4 rebuild/migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V4OrdinalRebuildEvidence {
    /// Rebuild source and migration disposition.
    pub disposition: V4OrdinalRebuildDisposition,
    /// Exact topology generation published.
    pub topology_generation: u64,
    /// Canonical node identities accepted.
    pub input_identities: u64,
    /// Maximal contiguous ordinal ranges emitted.
    pub ordinal_ranges: u64,
    /// Immutable v4 artifact payload bytes written before publication staging.
    pub artifact_bytes: u64,
    /// Fixed-size artifact blocks submitted.
    pub write_blocks: u64,
    /// Largest bounded construction buffer.
    pub peak_buffer_bytes: u64,
    /// Maximum total coexisting scratch bytes across sort runs, merge outputs,
    /// surrogate runs, and final immutable artifacts.
    pub peak_temporary_bytes: u64,
    /// Durable artifact flushes completed.
    pub fsync_operations: u64,
    /// Existing bounded external-sort evidence.
    pub build: UuidIndexBuildMetrics,
}

const V4_ORDINAL_BLOCK_BYTES: usize = 64 * 1024;
// Durable rewrite admits 16,384 graph-file entries. Reserve generation.json
// plus forward, tombstone, receipt, and manifest participants.
const V4_MAX_RANGES: usize = 16_379;

/// Aggregate-only evidence from the bounded v4 construction encoder.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct V4OrdinalBuildMetrics {
    pub(crate) input_records: u64,
    pub(crate) artifact_bytes: u64,
    pub(crate) write_blocks: u64,
    pub(crate) ranges: usize,
    pub(crate) peak_buffer_bytes: usize,
    pub(crate) peak_temporary_bytes: u64,
    pub(crate) fsync_operations: u64,
    pub(crate) cancellation_polls: u64,
    pub(crate) cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

/// Aggregate-only work evidence for one incremental v4 publication.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct V4OrdinalAppendMetrics {
    /// New UUID-to-surrogate mappings accepted by this publication.
    pub(crate) input_identities: u64,
    /// Sorted unique deletion overrides accepted by this publication.
    pub(crate) input_tombstones: u64,
    /// Canonical topology rows decoded from the prior generation. Always zero.
    pub(crate) prior_topology_rows_decoded: u64,
    /// Immutable artifacts created, including deterministic compaction outputs.
    pub(crate) created_artifacts: u64,
    /// Prior immutable artifacts retained without rewriting.
    pub(crate) retained_artifacts: u64,
    /// Binary-carry compactions completed.
    pub(crate) compactions: u64,
    /// Exact authenticated input bytes read by compaction.
    pub(crate) sequential_read_bytes: u64,
    /// Bounded sequential input calls made by compaction.
    pub(crate) sequential_read_calls: u64,
    /// Fixed-size input blocks covered by compaction reads.
    pub(crate) sequential_read_blocks: u64,
    /// Exact artifact and control bytes written.
    pub(crate) physical_bytes_written: u64,
    /// Artifact and control bytes submitted through output writers.
    pub(crate) write_bytes: u64,
    /// Bounded output blocks submitted.
    pub(crate) write_blocks: u64,
    /// Largest anonymous writer buffer.
    pub(crate) peak_buffer_bytes: usize,
    /// Maximum coexisting scratch output bytes.
    pub(crate) peak_temporary_bytes: u64,
    /// Durable file flushes completed while constructing outputs.
    pub(crate) fsync_operations: u64,
    /// Incremental compaction cache-release evidence.
    pub(crate) cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
    /// Largest aggregate of configured windows for one compaction operation.
    pub(crate) peak_configured_cache_window_bytes: u64,
    /// Per-record filesystem seeks are forbidden.
    pub(crate) per_record_seeks: u64,
    /// Unreferenced v4 candidates examined after publication.
    pub(crate) orphan_gc_candidates: u64,
    /// Unreferenced v4 artifacts removed by retained identity.
    pub(crate) orphan_gc_removed: u64,
    /// Candidates conservatively deferred.
    pub(crate) orphan_gc_deferred: u64,
    /// Physical orphan bytes reclaimed.
    pub(crate) orphan_gc_bytes: u64,
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
    /// Unreferenced canonical run files examined under the rewrite lock.
    pub orphan_gc_candidates: u64,
    /// Unreferenced one-link run files removed by retained identity.
    pub orphan_gc_removed: u64,
    /// Candidates left for a later bounded maintenance pass.
    pub orphan_gc_deferred: u64,
    /// Candidates deferred solely because the per-transaction bound was reached.
    pub orphan_gc_deferred_limit: u64,
    /// Candidates retained because another hard link exists.
    pub orphan_gc_deferred_linked: u64,
    /// Physical bytes reclaimed from removed orphan runs.
    pub orphan_gc_bytes: u64,
}

/// Typed bounded orphan-collection evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UuidIndexOrphanGcWork {
    /// Unreferenced canonical run files encountered.
    pub candidates: u64,
    /// Files removed by exact retained identity.
    pub removed: u64,
    /// Files deferred because the transaction work bound was reached.
    pub deferred: u64,
    /// Files deferred because the transaction work bound was reached.
    pub deferred_limit: u64,
    /// Files deferred because their retained inode has another hard link.
    pub deferred_linked: u64,
    /// Bytes reclaimed.
    pub bytes: u64,
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
    /// Block-positioning seeks performed. One seek corresponds to one bounded
    /// authenticated block read, never to one requested record.
    pub file_seeks: u64,
    /// Identity-run blocks read after block-fence selection.
    pub identity_blocks_read: u64,
    /// Identity-run bytes read after block-fence selection.
    pub identity_bytes_read: u64,
    /// Reverse-surrogate blocks read for batched pair validation.
    pub surrogate_blocks_read: u64,
    /// Reverse-surrogate bytes read for batched pair validation.
    pub surrogate_bytes_read: u64,
    /// Immutable runs considered while applying newest-run shadowing.
    pub runs_considered: u64,
    /// Per-record filesystem seeks. Batched lookup must keep this exactly zero.
    pub per_record_seeks: u64,
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
    if !block_matches(&bytes, block, width) {
        return Err(storage_err("UUID probe block authentication failed"));
    }
    metrics.validation_scan_bytes = metrics
        .validation_scan_bytes
        .saturating_add(bytes.len() as u64);
    metrics.validation_scan_blocks = metrics.validation_scan_blocks.saturating_add(1);
    Ok(bytes)
}
#[derive(Clone, Copy, Debug)]
struct IdentityState {
    present: bool,
    surrogate: u64,
}

/// Resolve all candidate keys in one run by selecting authenticated blocks
/// from their fences and merge-scanning each selected block once.
fn batch_identity_states(
    file: &mut File,
    descriptor: &FileRecord,
    expected_kind: UuidIndexKind,
    requested: &BTreeSet<Uuid>,
    metrics: &mut UuidProbeMetrics,
) -> Result<std::collections::BTreeMap<Uuid, IdentityState>, GfError> {
    let mut groups = std::collections::BTreeMap::<usize, Vec<Uuid>>::new();
    for uuid in requested {
        let key = hex_sha256_key(uuid.as_bytes());
        if let Some(index) = candidate_block(descriptor, &key) {
            groups.entry(index).or_default().push(*uuid);
        }
    }
    let mut found = std::collections::BTreeMap::new();
    for (index, candidates) in groups {
        let bytes = authenticated_probe_block(
            file,
            &descriptor.blocks[index],
            IDENTITY_RECORD_WIDTH,
            ProbeFileKind::Identity,
            metrics,
        )?;
        let mut remaining = bytes.as_slice();
        let mut next = identity_codec::take(&mut remaining)?;
        for uuid in candidates {
            while let Some(record) = next {
                match record[..16].cmp(uuid.as_bytes()) {
                    std::cmp::Ordering::Less => next = identity_codec::take(&mut remaining)?,
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {
                        let record_kind = if matches!(record[16], 0 | 2) {
                            UuidIndexKind::Node
                        } else {
                            UuidIndexKind::Edge
                        };
                        found.insert(
                            uuid,
                            IdentityState {
                                present: record_kind == expected_kind
                                    && matches!(record[16], 0 | 1),
                                surrogate: u64::from_be_bytes(
                                    record[17..25].try_into().expect("fixed record"),
                                ),
                            },
                        );
                        next = identity_codec::take(&mut remaining)?;
                        break;
                    }
                }
            }
        }
    }
    Ok(found)
}

/// Validate all resolved node identity/surrogate pairs in one run with the
/// same fence-selected merge scan. A missing or mismatched reverse pair is
/// authenticated corruption.
fn validate_surrogate_pairs(
    file: &mut File,
    descriptor: &FileRecord,
    pairs: &[(u64, Uuid)],
    metrics: &mut UuidProbeMetrics,
) -> Result<(), GfError> {
    let mut groups = std::collections::BTreeMap::<usize, Vec<(u64, Uuid)>>::new();
    for &(surrogate, uuid) in pairs {
        let key = hex_sha256_key(&surrogate.to_be_bytes());
        let index = candidate_block(descriptor, &key)
            .ok_or_else(|| storage_err("identity/surrogate run pair is inconsistent"))?;
        groups.entry(index).or_default().push((surrogate, uuid));
    }
    for (index, mut candidates) in groups {
        candidates.sort_unstable();
        let bytes = authenticated_probe_block(
            file,
            &descriptor.blocks[index],
            NODE_LOOKUP_RECORD_WIDTH,
            ProbeFileKind::Surrogate,
            metrics,
        )?;
        let mut record_index = 0_usize;
        for (surrogate, uuid) in candidates {
            let key = surrogate.to_be_bytes();
            let mut matched = false;
            while record_index < bytes.len() / NODE_LOOKUP_RECORD_WIDTH {
                let start = record_index * NODE_LOOKUP_RECORD_WIDTH;
                let record = &bytes[start..start + NODE_LOOKUP_RECORD_WIDTH];
                match record[..8].cmp(&key) {
                    std::cmp::Ordering::Less => record_index += 1,
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {
                        matched = record[8..24] == *uuid.as_bytes();
                        record_index += 1;
                        break;
                    }
                }
            }
            if !matched {
                return Err(storage_err("identity/surrogate run pair is inconsistent"));
            }
        }
    }
    Ok(())
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
    for (index, mut items) in groups {
        let bytes = authenticated_block(
            &mut run.identities,
            &run.descriptor.identities.blocks[index],
            IDENTITY_RECORD_WIDTH,
            metrics,
        )?;
        items.sort_unstable_by_key(|item| item.0);
        let mut remaining = bytes.as_slice();
        let mut next = identity_codec::take(&mut remaining)?;
        for (uuid, kind, surrogate) in items {
            while next.is_some_and(|record| record[..16] < uuid.as_bytes()[..]) {
                next = identity_codec::take(&mut remaining)?;
            }
            if let Some(record) = next.filter(|record| record[..16] == uuid.as_bytes()[..]) {
                let retained_kind = record[16];
                let retained_surrogate =
                    u64::from_be_bytes(record[17..25].try_into().expect("fixed"));
                let deletion_matches = ((*kind == 2 && retained_kind == 0)
                    || (*kind == 3 && retained_kind == 1))
                    && retained_surrogate == *surrogate;
                if !deletion_matches {
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
            NODE_LOOKUP_RECORD_WIDTH,
            metrics,
        )?;
        for (surrogate, uuid) in items {
            let key = surrogate.to_be_bytes();
            if let Some(at) = block_record_index(&bytes, NODE_LOOKUP_RECORD_WIDTH, &key) {
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
    graph_root: graphforge_filesystem::StableDirectory,
    graph_root_path: PathBuf,
    graph_root_identity: graphforge_filesystem::FileIdentity,
    root: graphforge_filesystem::StableDirectory,
    root_identity: graphforge_filesystem::FileIdentity,
    manifest_file: Option<File>,
    manifest_bytes: u64,
    manifest_identity: graphforge_filesystem::FileIdentity,
    manifest_sha256: String,
    manifest: Manifest,
    runs: Vec<AuthenticatedRun>,
    authenticated_bytes: u64,
    authenticated_blocks: u64,
    cas_source_paths: Option<BTreeMap<String, (String, String, u64)>>,
    _cas_leases: Vec<crate::graph_object_store::AuthenticatedGraphObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConstructionIndexOutput {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConstructionIndexReference {
    pub source_root: String,
    pub source_root_volume: u64,
    pub source_root_file_id: String,
    pub source_path: String,
    pub source_volume: u64,
    pub source_file_id: String,
    pub target_path: String,
    pub bytes: u64,
    pub sha256: String,
    pub parent_manifest_sha256: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConstructionIndexEncoding {
    pub artifacts: Vec<ConstructionIndexOutput>,
    pub retained_references: Vec<ConstructionIndexReference>,
    pub input_records: u64,
    pub read_bytes: u64,
    pub read_operations: u64,
    pub final_write_bytes: u64,
    pub write_bytes: u64,
    pub write_operations: u64,
    pub fsync_operations: u64,
    pub created_runs: u64,
    pub retained_runs: u64,
    pub retained_payload_bytes: u64,
    pub peak_buffer_bytes: u64,
    pub peak_temporary_bytes: u64,
    pub cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

#[cfg(test)]
type ConstructionOrdinalHook = Box<dyn FnMut(&str, u64)>;
#[cfg(test)]
thread_local! {
    static CONSTRUCTION_ORDINAL_HOOK: std::cell::RefCell<Option<ConstructionOrdinalHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
pub(crate) fn set_construction_ordinal_hook(hook: Option<ConstructionOrdinalHook>) {
    CONSTRUCTION_ORDINAL_HOOK.with(|slot| *slot.borrow_mut() = hook);
}
fn construction_ordinal_event(_phase: &str, _generation: u64) {
    #[cfg(test)]
    CONSTRUCTION_ORDINAL_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(_phase, _generation);
        }
    });
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UuidConstructionSnapshotWork {
    pub authentication_bytes: u64,
    pub authentication_blocks: u64,
    pub live_nodes: u64,
    pub live_edges: u64,
    pub max_node_surrogate: u64,
}

#[allow(clippy::struct_field_names)]
pub(crate) struct ConstructionReferenceAuthentication<'a> {
    pub(crate) source_root: &'a str,
    pub(crate) source_root_volume: u64,
    pub(crate) source_root_file_id: &'a str,
    pub(crate) source_path: &'a str,
    pub(crate) source_volume: u64,
    pub(crate) source_file_id: &'a str,
    pub(crate) target_path: &'a str,
    pub(crate) bytes: u64,
    pub(crate) sha256: &'a str,
    pub(crate) parent_manifest_sha256: &'a str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConstructionReferenceAuthenticationWork {
    pub(crate) global_revalidation_bytes: u64,
    pub(crate) referenced_payload_bytes: u64,
}

// Immutable authenticated runs may be shared by a hydrated CAS workspace.
// Mutable manifests/receipts and private construction artifacts retain their
// strict single-link rules. Digest/block and named-inode checks remain separate.
fn retained_run_has_safe_links(file: &File) -> Result<bool, GfError> {
    let links = graphforge_filesystem::file_link_count(file).map_err(storage_err)?;
    Ok(links == 1
        || links > 1
            && file
                .metadata()
                .map_err(storage_err)?
                .permissions()
                .readonly())
}

/// Whether a membership manifest exists, without duplicating its private layout.
#[must_use]
pub fn uuid_membership_index_present(project_dir: &Path) -> bool {
    project_dir.join(INDEX_DIR).join(MANIFEST).is_file()
}

const DEFAULT_ORPHAN_GC_LIMIT: usize = 64;

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_SNAPSHOT_REFRESH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_next_snapshot_refresh_for_test() {
    FAIL_NEXT_SNAPSHOT_REFRESH.set(true);
}

#[cfg(test)]
fn injected_snapshot_refresh_failure() -> Option<GfError> {
    if FAIL_NEXT_SNAPSHOT_REFRESH.replace(false) {
        return Some(storage_err("injected UUID snapshot refresh failure"));
    }
    None
}

#[cfg(not(test))]
fn injected_snapshot_refresh_failure() -> Option<GfError> {
    None
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

pub(crate) struct PreparedV4OrdinalDelta {
    expected_generation: u64,
    auxiliary: crate::AuxiliaryReceipt,
    metrics: V4OrdinalAppendMetrics,
    manifest: crate::V4OrdinalIdentityManifest,
}

impl PreparedV4OrdinalDelta {
    pub(crate) fn auxiliary_receipt(&self) -> crate::AuxiliaryReceipt {
        self.auxiliary.clone()
    }

    pub(crate) fn verify_generation(&self, committed_generation: u64) -> Result<(), GfError> {
        if committed_generation != self.expected_generation {
            return Err(storage_err(
                "topology commit returned an unexpected v4 ordinal generation",
            ));
        }
        Ok(())
    }

    pub(crate) fn metrics(&self) -> &V4OrdinalAppendMetrics {
        &self.metrics
    }
}

pub(crate) struct UuidTopologyDelta {
    pub nodes: Vec<(Uuid, u64)>,
    pub edges: Vec<Uuid>,
    pub deleted_nodes: Vec<Uuid>,
    pub deleted_edges: Vec<Uuid>,
}

pub(crate) enum CommittedUuidTopologyRewrite {
    NoTopologyChange,
    Committed {
        generation: u64,
        metrics: UuidIndexAppendMetrics,
        v4_metrics: Option<V4OrdinalAppendMetrics>,
    },
    CommittedNeedsRefresh {
        generation: u64,
        metrics: UuidIndexAppendMetrics,
        v4_metrics: Option<V4OrdinalAppendMetrics>,
        error: GfError,
    },
}

#[cfg(test)]
thread_local! {
    static FAIL_AFTER_MANIFEST_SUSPEND: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn record_length(record: &FileRecord, width: u64) -> Result<u64, GfError> {
    if width == IDENTITY_RECORD_BYTES {
        record.blocks.iter().try_fold(0_u64, |total, block| {
            total
                .checked_add(u64::from(block.len))
                .ok_or_else(|| storage_err("record length overflow"))
        })
    } else {
        record
            .count
            .checked_mul(width)
            .ok_or_else(|| storage_err("record length overflow"))
    }
}

fn block_layout(bytes: &[u8], width: usize) -> Result<(u64, &[u8], &[u8]), GfError> {
    if width == IDENTITY_RECORD_WIDTH {
        return identity_codec::layout(bytes);
    }
    if width != NODE_LOOKUP_RECORD_WIDTH || bytes.is_empty() || !bytes.len().is_multiple_of(width) {
        return Err(storage_err("partial UUID run record"));
    }
    Ok((
        (bytes.len() / width) as u64,
        &bytes[..8],
        &bytes[bytes.len() - width..bytes.len() - width + 8],
    ))
}

fn block_matches(bytes: &[u8], block: &BlockRecord, width: usize) -> bool {
    block_layout(bytes, width).is_ok_and(|(_, first, last)| {
        hex_sha256(bytes) == block.sha256
            && hex_sha256_key(first) == block.first_key
            && hex_sha256_key(last) == block.last_key
    })
}

fn open_verified_at(
    directory: &graphforge_filesystem::StableDirectory,
    record: &FileRecord,
    record_bytes: u64,
) -> Result<File, GfError> {
    if Path::new(&record.name).components().count() != 1 {
        return Err(storage_err("manifest contains a non-local index filename"));
    }
    let mut file = open_uuid_child_file(directory, std::ffi::OsStr::new(&record.name))?;
    let expected = record_length(record, record_bytes)?;
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
    let mut count = 0_u64;
    for block in &record.blocks {
        file.seek(SeekFrom::Start(block.offset))
            .map_err(storage_err)?;
        let mut bytes = vec![0_u8; block.len as usize];
        file.read_exact(&mut bytes).map_err(storage_err)?;
        let width = usize::try_from(record_bytes)
            .map_err(|_| storage_err("record width does not fit address space"))?;
        if !block_matches(&bytes, block, width) {
            return Err(storage_err("UUID run block authentication failed"));
        }
        count = count
            .checked_add(block_layout(&bytes, width)?.0)
            .ok_or_else(|| storage_err("record count overflow"))?;
        whole.update(&bytes);
        if let Some(metrics) = work.as_deref_mut() {
            metrics.validation_scan_bytes = metrics
                .validation_scan_bytes
                .saturating_add(bytes.len() as u64);
            metrics.validation_scan_blocks = metrics.validation_scan_blocks.saturating_add(1);
        }
    }
    let digest = hex_bytes(&whole.finalize());
    if digest != record.sha256 || count != record.count {
        return Err(storage_err("UUID run authentication failed"));
    }
    Ok(())
}

fn validate_block_records(record: &FileRecord, record_bytes: u64) -> Result<(), GfError> {
    let expected = record_length(record, record_bytes)?;
    if expected == 0 {
        if record.count != 0 || !record.blocks.is_empty() {
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
            || (record_bytes != IDENTITY_RECORD_BYTES && u64::from(block.len) % record_bytes != 0)
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
    if (record_bytes == IDENTITY_RECORD_BYTES
        && (expected < record.count.saturating_mul(17)
            || expected > record.count.saturating_mul(25)))
        || offset != expected
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
    if width != IDENTITY_RECORD_BYTES && length % width != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let (sha256, blocks, count) = describe_blocks(&mut open_uuid_file(path)?, width)?;
    Ok(FileRecord {
        name: format!("{kind}-{generation}-{}.uuidx", &sha256[..16]),
        count,
        sha256,
        blocks,
    })
}

fn describe_blocks(
    file: &mut File,
    width: u64,
) -> Result<(String, Vec<BlockRecord>, u64), GfError> {
    describe_stream(
        file,
        usize::try_from(width).map_err(storage_err)?,
        &mut (0, 0),
    )
}

/// Read bounded physical windows while carrying at most one partial record.
/// Published authentication blocks always end at a complete record boundary.
fn describe_stream(
    file: &mut impl Read,
    width: usize,
    reads: &mut (u64, u64),
) -> Result<(String, Vec<BlockRecord>, u64), GfError> {
    if !matches!(width, IDENTITY_RECORD_WIDTH | NODE_LOOKUP_RECORD_WIDTH) {
        return Err(storage_err("unsupported UUID run record width"));
    }
    let mut buffer = vec![0_u8; BULK_IO_BYTES];
    let mut carried = 0;
    let mut offset = 0_u64;
    let mut count = 0_u64;
    let mut whole = Sha256::new();
    let mut blocks = Vec::new();
    loop {
        let mut filled = carried;
        let mut eof = false;
        while filled < buffer.len() {
            let read = file.read(&mut buffer[filled..]).map_err(storage_err)?;
            if read == 0 {
                eof = true;
                break;
            }
            filled += read;
            reads.0 = reads.0.saturating_add(read as u64);
            reads.1 = reads.1.saturating_add(1);
        }
        if filled == 0 {
            break;
        }
        let valid = if width == IDENTITY_RECORD_WIDTH {
            let mut valid = 0;
            while filled - valid >= identity_codec::EDGE_WIDTH {
                let length = match buffer[valid + 16] {
                    1 => identity_codec::EDGE_WIDTH,
                    0 | 2 | 3 => IDENTITY_RECORD_WIDTH,
                    _ => return Err(storage_err("identity record kind is invalid")),
                };
                if filled - valid < length {
                    break;
                }
                valid += length;
            }
            valid
        } else {
            filled / width * width
        };
        if valid == 0 || (eof && valid != filled) {
            return Err(storage_err("internal run has a partial index record"));
        }
        let bytes = &buffer[..valid];
        let (records, first, last) = block_layout(bytes, width)?;
        whole.update(bytes);
        blocks.push(BlockRecord {
            offset,
            len: u32::try_from(valid).map_err(storage_err)?,
            first_key: hex_sha256_key(first),
            last_key: hex_sha256_key(last),
            sha256: hex_sha256(bytes),
        });
        offset = offset
            .checked_add(valid as u64)
            .ok_or_else(|| storage_err("block offset overflow"))?;
        count = count
            .checked_add(records)
            .ok_or_else(|| storage_err("record count overflow"))?;
        carried = filled - valid;
        buffer.copy_within(valid..filled, 0);
        if eof {
            break;
        }
    }
    Ok((hex_bytes(&whole.finalize()), blocks, count))
}

fn hex_sha256_key(bytes: &[u8]) -> String {
    hex_bytes(bytes)
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
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
    if intervals.last().map_or(0, |interval| interval.1) != manifest.current_generation
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
        let record = identity_codec::read(&mut identities)?
            .ok_or_else(|| storage_err("identity run is truncated"))?;
        let uuid: [u8; 16] = record[..16].try_into().expect("fixed record");
        if previous_uuid.is_some_and(|previous| previous >= uuid) {
            return Err(storage_err(
                "identity run is not canonical and strictly sorted",
            ));
        }
        previous_uuid = Some(uuid);
        let surrogate = u64::from_be_bytes(record[17..25].try_into().expect("fixed record"));
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
    let expected_len = record_length(record, record_bytes)?;
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
#[cfg(test)]
thread_local! {
    static V4_COMPACTION_POST_WRITE_FAILURE: std::cell::RefCell<Option<&'static str>> =
        const { std::cell::RefCell::new(None) };
    static V4_OUTPUT_CLEANUP_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static V4_INPUT_RELEASE_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static V4_PUBLICATION_FAILURE: std::cell::RefCell<Option<&'static str>> =
        const { std::cell::RefCell::new(None) };
    static V4_AUTHORITY_FAILURE: std::cell::RefCell<Option<&'static str>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn inject_v4_compaction_post_write_failure(point: &'static str) {
    V4_COMPACTION_POST_WRITE_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
}

#[cfg(test)]
pub(crate) fn inject_v4_output_cleanup_failure() {
    V4_OUTPUT_CLEANUP_FAILURE.with(|failure| failure.set(true));
}

#[cfg(test)]
fn inject_v4_input_release_failure() {
    V4_INPUT_RELEASE_FAILURE.with(|failure| failure.set(true));
}

#[cfg(test)]
fn inject_v4_publication_failure(point: &'static str) {
    V4_PUBLICATION_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
}

#[cfg(test)]
pub(crate) fn inject_v4_authority_failure(point: &'static str) {
    V4_AUTHORITY_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
}

#[allow(clippy::unnecessary_wraps)]
fn v4_authority_failure(point: &str) -> Result<(), GfError> {
    #[cfg(test)]
    V4_AUTHORITY_FAILURE.with(|failure| {
        if failure
            .borrow()
            .is_some_and(|configured| configured == point)
        {
            failure.borrow_mut().take();
            return Err(storage_err(format!(
                "injected v4 authority failure at {point}"
            )));
        }
        Ok(())
    })?;
    #[cfg(not(test))]
    let _ = point;
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn v4_publication_failure(point: &str) -> Result<(), GfError> {
    #[cfg(test)]
    V4_PUBLICATION_FAILURE.with(|failure| {
        if failure
            .borrow()
            .is_some_and(|configured| configured == point)
        {
            failure.borrow_mut().take();
            return Err(storage_err(format!(
                "injected v4 publication failure at {point}"
            )));
        }
        Ok(())
    })?;
    #[cfg(not(test))]
    let _ = point;
    Ok(())
}

fn v4_publication_io_failure(point: &str) -> std::io::Result<()> {
    v4_publication_failure(point)
        .map_err(|_| std::io::Error::other(format!("injected v4 publication failure at {point}")))
}

fn inject_v4_input_release_result(
    released: Result<graphforge_filesystem::FileCacheReleaseEvidence, GfError>,
) -> Result<graphforge_filesystem::FileCacheReleaseEvidence, GfError> {
    #[cfg(test)]
    if released.is_ok() && V4_INPUT_RELEASE_FAILURE.with(|failure| failure.replace(false)) {
        return Err(storage_err("injected v4 input release failure"));
    }
    released
}

#[allow(clippy::unnecessary_wraps)]
fn take_v4_output_cleanup_failure() -> Result<(), GfError> {
    #[cfg(test)]
    if V4_OUTPUT_CLEANUP_FAILURE.with(|failure| failure.replace(false)) {
        return Err(storage_err("injected v4 output cleanup failure"));
    }
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn v4_compaction_post_write_failure(point: &str) -> Result<(), GfError> {
    construction_ordinal_event(point, 0);
    #[cfg(test)]
    V4_COMPACTION_POST_WRITE_FAILURE.with(|failure| {
        if failure
            .borrow()
            .is_some_and(|configured| configured == point)
        {
            failure.borrow_mut().take();
            return Err(storage_err(format!(
                "v4 compaction cancelled after {point} output"
            )));
        }
        Ok(())
    })?;
    #[cfg(not(test))]
    let _ = point;
    Ok(())
}

pub(crate) fn canonical_v3_manifest_marker(bytes: &[u8], expected_generation: u64) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    let Ok(manifest) = serde_json::from_value::<Manifest>(value.clone()) else {
        return false;
    };
    // Serialize the typed v3 schema and require the supplied tree to be a
    // recursive structural subset. This rejects unknown fields in Manifest,
    // RunRecord, FileRecord, and BlockRecord without duplicating descriptor
    // semantics here. Missing serde-default fields remain valid v3.
    let Ok(canonical_shape) = serde_json::to_value(&manifest) else {
        return false;
    };
    if !json_shape_is_subset(&value, &canonical_shape) {
        return false;
    }
    manifest.format_version == FORMAT_VERSION
        && manifest.current_generation == expected_generation
        && validate_run_descriptors(&manifest).is_ok()
}

fn json_shape_is_subset(candidate: &serde_json::Value, canonical: &serde_json::Value) -> bool {
    match (candidate, canonical) {
        (serde_json::Value::Object(candidate), serde_json::Value::Object(canonical)) => {
            candidate.iter().all(|(key, value)| {
                canonical
                    .get(key)
                    .is_some_and(|known| json_shape_is_subset(value, known))
            })
        }
        (serde_json::Value::Array(candidate), serde_json::Value::Array(canonical)) => {
            candidate.len() == canonical.len()
                && candidate
                    .iter()
                    .zip(canonical)
                    .all(|(value, known)| json_shape_is_subset(value, known))
        }
        _ => true,
    }
}

fn describe_staged_data(
    source: &Path,
    kind: &str,
    generation: u64,
    record_bytes: u64,
) -> Result<FileRecord, GfError> {
    let length = source.metadata().map_err(storage_err)?.len();
    if record_bytes != IDENTITY_RECORD_BYTES && length % record_bytes != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let mut input = File::open(source).map_err(storage_err)?;
    let (sha256, blocks, count) = describe_blocks(&mut input, record_bytes)?;
    Ok(FileRecord {
        name: format!("{kind}-{generation}-{}.uuidx", &sha256[..16]),
        count,
        sha256,
        blocks,
    })
}
#[cfg(test)]
#[cfg(test)]
pub(crate) mod tests;
