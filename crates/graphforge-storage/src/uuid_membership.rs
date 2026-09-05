//! Persistent, bounded-memory UUID membership indexes used by bulk ingest.
//!
//! The canonical graph remains Parquet.  This derived format is deliberately
//! small: one manifest (published last with an atomic replacement) names an
//! immutable base plus size-tiered delta runs. Each run contains a unified
//! UUID-sorted identity file and a node-only surrogate-sorted reverse file.
//! Readers verify version, topology generation, framing, canonical ordering,
//! counts, and SHA-256 before serving bounded binary-search probes.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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
const NODE_LOOKUP_RECORD_WIDTH: usize = 24;
const IDENTITY_RECORD_WIDTH: usize = 32;
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

#[derive(Debug)]
pub(crate) struct V4ConstructionArtifactBundle {
    pub(crate) manifest: crate::V4OrdinalIdentityManifest,
    pub(crate) metrics: V4OrdinalBuildMetrics,
    publications: Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
}

struct V4AuthorityTransactionProof;

fn commit_v4_publications(
    publications: Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
    _proof: V4AuthorityTransactionProof,
) -> Result<(), GfError> {
    for (_, publication) in publications {
        publication.commit().map_err(storage_err)?;
    }
    Ok(())
}

fn retain_v4_publication(
    publications: &mut Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
    name: String,
    publication: Option<graphforge_filesystem::UnpublishedArtifactGuard>,
) {
    if let Some(publication) = publication {
        publications.push((name, publication));
    }
}

fn cleanup_v4_publication(
    publication: &mut graphforge_filesystem::UnpublishedArtifactGuard,
) -> Result<(), GfError> {
    publication
        .cleanup_checked(|| {
            take_v4_output_cleanup_failure()
                .map_err(|_| std::io::Error::other("injected v4 output cleanup failure"))
        })
        .map_err(storage_err)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct V4RebuildScratchAccounting {
    peak_bytes: u64,
    sorted_projection_bytes: u64,
    live_artifact_bytes: u64,
    retained_staged_bytes: u64,
    staged_control_bytes: u64,
    scratch_live: bool,
}

impl V4RebuildScratchAccounting {
    fn register_sorted_projection(&mut self, path: &Path) -> Result<(), GfError> {
        self.sorted_projection_bytes = path.metadata().map_err(storage_err)?.len();
        // A complete merge output coexists with the complete input round until
        // that round is retired. The sorter owns both sets, so account for the
        // overlap directly instead of inspecting the active scratch tree.
        self.observe(self.sorted_projection_bytes.checked_mul(2))
    }

    fn register_surrogate_projection(&mut self, path: &Path) -> Result<(), GfError> {
        let surrogate_bytes = path.metadata().map_err(storage_err)?.len();
        if surrogate_bytes != self.sorted_projection_bytes {
            return Err(storage_err(
                "v4 rebuild projections have inconsistent scratch sizes",
            ));
        }
        // UUID-sorted input remains live while a complete surrogate merge
        // output coexists with its complete input round.
        self.observe(
            self.sorted_projection_bytes.checked_add(
                surrogate_bytes
                    .checked_mul(2)
                    .ok_or_else(|| storage_err("v4 rebuild scratch byte count overflow"))?,
            ),
        )
    }

    fn register_artifacts(&mut self, artifact_peak: u64) -> Result<(), GfError> {
        // Both final sort projections remain open while the immutable forward
        // and ordinal artifacts are streamed.
        self.live_artifact_bytes = artifact_peak;
        self.scratch_live = true;
        self.observe_current()
    }

    fn register_staged_artifact(&mut self, bytes: u64) -> Result<(), GfError> {
        self.retained_staged_bytes = self
            .retained_staged_bytes
            .checked_add(bytes)
            .ok_or_else(|| storage_err("v4 rebuild staged artifact byte count overflow"))?;
        self.observe_current()
    }

    fn register_staged_control(&mut self, bytes: u64) -> Result<(), GfError> {
        self.staged_control_bytes = self
            .staged_control_bytes
            .checked_add(bytes)
            .ok_or_else(|| storage_err("v4 rebuild staged control byte count overflow"))?;
        self.observe_current()
    }

    fn release_scratch(&mut self) -> Result<(), GfError> {
        self.scratch_live = false;
        self.observe_current()
    }

    fn observe_current(&mut self) -> Result<(), GfError> {
        let scratch = if self.scratch_live {
            self.sorted_projection_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(self.live_artifact_bytes))
        } else {
            Some(0)
        };
        self.observe(
            scratch
                .and_then(|bytes| bytes.checked_add(self.retained_staged_bytes))
                .and_then(|bytes| bytes.checked_add(self.staged_control_bytes)),
        )
    }

    fn observe(&mut self, bytes: Option<u64>) -> Result<(), GfError> {
        let bytes = bytes.ok_or_else(|| storage_err("v4 rebuild scratch byte count overflow"))?;
        self.peak_bytes = self.peak_bytes.max(bytes);
        Ok(())
    }
}

struct StagedV4OrdinalRebuild {
    generation: u64,
    build: UuidIndexBuildMetrics,
    artifacts: V4OrdinalBuildMetrics,
    scratch: V4RebuildScratchAccounting,
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

fn clone_pinned_v4_file(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    name: &str,
) -> Result<File, GfError> {
    let mut file = pinned
        .artifacts
        .iter()
        .find(|artifact| artifact.descriptor.name == name)
        .ok_or_else(|| storage_err("authenticated v4 update input is absent"))?
        .file
        .try_clone()
        .map_err(storage_err)?;
    // Update inputs are writer-exclusive retained handles. `try_clone` may
    // share an OS file offset with the retained handle, so every sequential
    // consumer must establish its own logical start before reading.
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    Ok(file)
}

/// Encode the already-canonical construction node stream without rescanning
/// topology or consulting v3 reverse authority. The caller owns the durable
/// rewrite transaction and publishes the returned manifest later.
#[cfg(test)]
pub(crate) fn stage_v4_ordinal_artifacts<I, F>(
    records: I,
    generation: u64,
    index: &graphforge_filesystem::StableDirectory,
    mut cancelled: F,
) -> Result<(crate::V4OrdinalIdentityManifest, V4OrdinalBuildMetrics), GfError>
where
    I: IntoIterator<Item = (Uuid, u64)>,
    F: FnMut() -> bool,
{
    let bundle = stage_v4_ordinal_bundle(records, generation, index, &mut cancelled)?;
    index.sync().map_err(storage_err)?;
    let V4ConstructionArtifactBundle {
        manifest,
        metrics,
        publications,
    } = bundle;
    commit_v4_publications(publications, V4AuthorityTransactionProof)?;
    Ok((manifest, metrics))
}

#[cfg(test)]
fn stage_v4_ordinal_bundle<I, F>(
    records: I,
    generation: u64,
    index: &graphforge_filesystem::StableDirectory,
    cancelled: &mut F,
) -> Result<V4ConstructionArtifactBundle, GfError>
where
    I: IntoIterator<Item = (Uuid, u64)>,
    F: FnMut() -> bool,
{
    let mut writer = V4OrdinalConstructionWriter::start(generation, index)?;
    for (uuid, node_id) in records {
        writer.push_pair(uuid, node_id, cancelled)?;
    }
    writer.finish()
}

/// Incremental v4 encoder used to tee the already-assigned fresh-construction
/// node stream without retaining it or reading it a second time.
pub(crate) struct V4OrdinalConstructionWriter<'a> {
    generation: u64,
    index: &'a graphforge_filesystem::StableDirectory,
    cache_window: std::num::NonZeroU64,
    forward: StreamingV4Artifact,
    ranges: Vec<crate::V4OrdinalRange>,
    publications: Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
    current: Option<V4OrdinalRangeWriter>,
    previous_forward_uuid: Option<[u8; 16]>,
    previous_ordinal_node_id: u64,
    forward_count: u64,
    ordinal_count: u64,
    forward_commitment: [u8; 32],
    ordinal_commitment: [u8; 32],
    forward_commitment_two: [u8; 32],
    ordinal_commitment_two: [u8; 32],
    metrics: V4OrdinalBuildMetrics,
}

impl<'a> V4OrdinalConstructionWriter<'a> {
    pub(crate) fn start(
        generation: u64,
        index: &'a graphforge_filesystem::StableDirectory,
    ) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage_err)?;
        Self::start_with_cache_window(generation, index, cache_window)
    }

    pub(crate) fn start_with_cache_window(
        generation: u64,
        index: &'a graphforge_filesystem::StableDirectory,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        if generation == 0 {
            return Err(storage_err("v4 ordinal generation is zero"));
        }
        let forward = StreamingV4Artifact::create_with_window(index, "forward", cache_window)?;
        let validated = graphforge_filesystem::validate_cache_release_operation_windows(&[forward
            .writer
            .get_ref()
            .window_bytes()])
        .map_err(storage_err)
        .and_then(|_| v4_publication_failure("window_validation"));
        if let Err(primary) = validated {
            let (_, cleanup) = forward.cleanup_unpublished();
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup cleanup");
        }
        Ok(Self {
            generation,
            index,
            cache_window,
            forward,
            ranges: Vec::new(),
            publications: Vec::new(),
            current: None,
            previous_forward_uuid: None,
            previous_ordinal_node_id: 0,
            forward_count: 0,
            ordinal_count: 0,
            forward_commitment: [0; 32],
            ordinal_commitment: [0; 32],
            forward_commitment_two: [0; 32],
            ordinal_commitment_two: [0; 32],
            metrics: V4OrdinalBuildMetrics {
                // Forward BufWriter + ordinal BufWriter + ordinal authentication block.
                peak_buffer_bytes: V4_ORDINAL_BLOCK_BYTES * 3,
                ..Default::default()
            },
        })
    }

    pub(crate) fn push_pair(
        &mut self,
        uuid: Uuid,
        node_id: u64,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_forward_inner(uuid, node_id)?;
        self.push_ordinal_inner(node_id, uuid)
    }

    pub(crate) fn push_forward(
        &mut self,
        uuid: Uuid,
        node_id: u64,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_forward_inner(uuid, node_id)
    }

    pub(crate) fn push_ordinal(
        &mut self,
        node_id: u64,
        uuid: Uuid,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_ordinal_inner(node_id, uuid)
    }

    fn poll_cancelled(&mut self, cancelled: &mut impl FnMut() -> bool) -> Result<(), GfError> {
        self.metrics.cancellation_polls = self
            .metrics
            .cancellation_polls
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 cancellation poll count overflow"))?;
        if cancelled() {
            return Err(storage_err("v4 ordinal construction cancelled"));
        }
        Ok(())
    }

    fn push_forward_inner(&mut self, uuid: Uuid, node_id: u64) -> Result<(), GfError> {
        let uuid_bytes = *uuid.as_bytes();
        if uuid_bytes == [0; 16]
            || self
                .previous_forward_uuid
                .is_some_and(|prior| prior >= uuid_bytes)
            || node_id == 0
        {
            return Err(storage_err(
                "v4 forward identities are not canonical and increasing",
            ));
        }
        self.forward.push(&uuid_bytes)?;
        self.forward.push(&node_id.to_be_bytes())?;
        add_v4_mapping_commitment(&mut self.forward_commitment, 0, uuid_bytes, node_id);
        add_v4_mapping_commitment(&mut self.forward_commitment_two, 1, uuid_bytes, node_id);
        self.forward_count = self
            .forward_count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 forward record count overflow"))?;
        self.previous_forward_uuid = Some(uuid_bytes);
        self.metrics.peak_temporary_bytes =
            self.metrics.peak_temporary_bytes.max(self.forward.bytes);
        Ok(())
    }

    fn push_ordinal_inner(&mut self, node_id: u64, uuid: Uuid) -> Result<(), GfError> {
        let uuid_bytes = *uuid.as_bytes();
        if uuid_bytes == [0; 16] || node_id == 0 || node_id <= self.previous_ordinal_node_id {
            return Err(storage_err(
                "v4 ordinal identities are not canonical and increasing",
            ));
        }
        if self.current.is_some()
            && self
                .previous_ordinal_node_id
                .checked_add(1)
                .is_none_or(|expected| node_id != expected)
        {
            finish_streamed_v4_range(
                self.generation,
                self.current.take().expect("range exists"),
                &mut self.ranges,
                &mut self.publications,
                &mut self.metrics,
            )?;
        }
        if self.current.is_none() {
            if self.ranges.len() >= V4_MAX_RANGES {
                return Err(storage_err("v4 ordinal range inventory exceeds bound"));
            }
            self.current = Some(V4OrdinalRangeWriter::new(
                self.index,
                self.ranges.len(),
                node_id,
                self.cache_window,
            )?);
        }
        self.current
            .as_mut()
            .expect("range exists")
            .push(uuid_bytes)?;
        add_v4_mapping_commitment(&mut self.ordinal_commitment, 0, uuid_bytes, node_id);
        add_v4_mapping_commitment(&mut self.ordinal_commitment_two, 1, uuid_bytes, node_id);
        self.ordinal_count = self
            .ordinal_count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 ordinal record count overflow"))?;
        self.previous_ordinal_node_id = node_id;
        let live_temporary_bytes = self
            .metrics
            .artifact_bytes
            .checked_add(self.forward.bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.current
                        .as_ref()
                        .map_or(0, |range| range.artifact.bytes),
                )
            })
            .ok_or_else(|| storage_err("v4 live temporary byte count overflow"))?;
        self.metrics.peak_temporary_bytes =
            self.metrics.peak_temporary_bytes.max(live_temporary_bytes);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<V4ConstructionArtifactBundle, GfError> {
        if self.forward_count != self.ordinal_count
            || self.forward_commitment != self.ordinal_commitment
            || self.forward_commitment_two != self.ordinal_commitment_two
        {
            return Err(storage_err(
                "v4 forward and ordinal projections describe different mappings",
            ));
        }
        self.metrics.input_records = self.forward_count;
        if let Some(writer) = self.current.take() {
            finish_streamed_v4_range(
                self.generation,
                writer,
                &mut self.ranges,
                &mut self.publications,
                &mut self.metrics,
            )?;
        }
        let forward = finish_streamed_v4_artifact(
            self.forward,
            "forward-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::ForwardIdentities,
            &mut self.metrics,
        )?;

        // Construction has no deletions, but an explicit authenticated empty run
        // commits that fact instead of leaving tombstone authority implicit.
        let tombstone = finish_streamed_v4_artifact(
            StreamingV4Artifact::create_with_window(self.index, "tombstones", self.cache_window)?,
            "tombstones-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::NodeTombstones,
            &mut self.metrics,
        )?;
        self.metrics.ranges = self.ranges.len();
        let manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: self.generation,
            forward_identities: vec![forward.artifact],
            ordinal_ranges: self.ranges,
            tombstones: vec![crate::V4OrdinalTombstones {
                generation: self.generation,
                artifact: tombstone.artifact,
                blocks: Vec::new(),
            }],
        };
        v4_publication_failure("manifest_update")?;
        admit_v4_construction_manifest(&manifest)?;
        retain_v4_publication(
            &mut self.publications,
            manifest.forward_identities[0].name.clone(),
            forward.publication,
        );
        retain_v4_publication(
            &mut self.publications,
            manifest.tombstones[0].artifact.name.clone(),
            tombstone.publication,
        );
        Ok(V4ConstructionArtifactBundle {
            manifest,
            metrics: self.metrics,
            publications: self.publications,
        })
    }
}

fn admit_v4_construction_manifest(
    manifest: &crate::V4OrdinalIdentityManifest,
) -> Result<(), GfError> {
    let bytes = serde_json::to_vec(manifest).map_err(storage_err)?;
    if bytes.len() as u64 > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES {
        return Err(storage_err(
            "v4 ordinal manifest exceeds the reader admission bound",
        ));
    }
    Ok(())
}

fn add_v4_mapping_commitment(commitment: &mut [u8; 32], domain: u8, uuid: [u8; 16], node_id: u64) {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-v4-mapping-v1\0");
    digest.update([domain]);
    digest.update(uuid);
    digest.update(node_id.to_be_bytes());
    let mapping: [u8; 32] = digest.finalize().into();
    let mut carry = 0_u16;
    for (target, value) in commitment.iter_mut().rev().zip(mapping.iter().rev()) {
        let sum = u16::from(*target) + u16::from(*value) + carry;
        *target = sum.to_be_bytes()[1];
        carry = sum >> 8;
    }
}

struct StreamingV4Artifact {
    writer: BufWriter<graphforge_filesystem::DurableFileCacheWriter>,
    publication: graphforge_filesystem::UnpublishedArtifactGuard,
    digest: Sha256,
    bytes: u64,
}

impl StreamingV4Artifact {
    fn create_with_window(
        index: &graphforge_filesystem::StableDirectory,
        role: &str,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        let temporary_name = format!(".v4-{role}-{}.tmp", Uuid::new_v4().simple());
        let mut publication = index
            .create_unpublished_replaceable_child(std::ffi::OsStr::new(&temporary_name))
            .map_err(storage_err)?;
        if let Err(primary) = publication
            .verify_identity_with(|| v4_publication_io_failure("initial_file_identity"))
            .map_err(storage_err)
        {
            let cleanup = cleanup_v4_publication(&mut publication);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup cleanup");
        }
        let file = publication.take_file().map_err(storage_err)?;
        let writer = match graphforge_filesystem::DurableFileCacheWriter::with_window_bytes_checked(
            file,
            cache_window,
            || v4_publication_io_failure("writer_construction"),
        ) {
            Ok(writer) => writer,
            Err(primary) => {
                let cleanup = cleanup_v4_publication(&mut publication);
                return combine_v4_cleanup(Err(storage_err(primary)), cleanup, "v4 setup cleanup");
            }
        };
        Ok(Self {
            writer: BufWriter::with_capacity(V4_ORDINAL_BLOCK_BYTES, writer),
            publication,
            digest: Sha256::new(),
            bytes: 0,
        })
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), GfError> {
        self.writer.write_all(bytes).map_err(storage_err)?;
        self.digest.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(storage_err)?)
            .ok_or_else(|| storage_err("v4 artifact length overflow"))?;
        Ok(())
    }

    fn cleanup_unpublished(
        mut self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        let flushed = self.writer.flush().map_err(storage_err);
        let synchronized = self
            .writer
            .get_mut()
            .sync_all_and_release()
            .map_err(storage_err);
        let cache_release = self.writer.get_ref().evidence();
        let finalized = combine_v4_cleanup(flushed, synchronized, "v4 output synchronization");
        drop(self.writer);
        let removed = cleanup_v4_publication(&mut self.publication);
        (
            cache_release,
            combine_v4_cleanup(finalized, removed, "v4 output removal"),
        )
    }
}

#[derive(Debug)]
struct GuardedV4Artifact {
    artifact: crate::V4OrdinalArtifact,
    publication: Option<graphforge_filesystem::UnpublishedArtifactGuard>,
}

#[allow(clippy::too_many_lines)]
fn finish_streamed_v4_artifact(
    mut writer: StreamingV4Artifact,
    prefix: &str,
    generation: u64,
    kind: crate::V4OrdinalArtifactKind,
    metrics: &mut V4OrdinalBuildMetrics,
) -> Result<GuardedV4Artifact, GfError> {
    let finalized = writer.writer.flush().map_err(storage_err).and_then(|()| {
        writer
            .writer
            .get_mut()
            .sync_all_and_release()
            .map_err(storage_err)
    });
    if let Err(primary) = finalized {
        let (cache_release, cleanup) = writer.cleanup_unpublished();
        merge_cache_release_evidence(&mut metrics.cache_release, cache_release);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 failed output cleanup");
    }
    let cache_release = writer.writer.get_ref().evidence();
    let prepared = (|| -> Result<(_, _), GfError> {
        v4_publication_failure("fsync_evidence_overflow")?;
        let mut committed_metrics = metrics.clone();
        merge_cache_release_evidence(&mut committed_metrics.cache_release, cache_release);
        committed_metrics.fsync_operations = committed_metrics
            .fsync_operations
            .checked_add(cache_release.sync_operations)
            .ok_or_else(|| storage_err("v4 fsync count overflow"))?;
        v4_publication_failure("final_file_identity")?;
        let identity = graphforge_filesystem::file_identity(writer.writer.get_ref().file())
            .map_err(storage_err)?;
        if identity != writer.publication.identity().map_err(storage_err)? {
            return Err(storage_err("v4 final output identity changed"));
        }
        v4_publication_failure("file_space_usage")?;
        let space = graphforge_filesystem::file_space_usage(writer.writer.get_ref().file())
            .map_err(storage_err)?;
        if space.logical_bytes != writer.bytes {
            return Err(storage_err("v4 final output length changed"));
        }
        let sha256 = hex_bytes(&writer.digest.clone().finalize());
        let artifact = crate::V4OrdinalArtifact {
            name: format!("{prefix}-{generation}-{}.uuidx", &sha256[..16]),
            kind,
            generation,
            bytes: writer.bytes,
            sha256,
        };
        committed_metrics.artifact_bytes = committed_metrics
            .artifact_bytes
            .checked_add(artifact.bytes)
            .ok_or_else(|| storage_err("v4 aggregate artifact length overflow"))?;
        committed_metrics.peak_temporary_bytes = committed_metrics
            .peak_temporary_bytes
            .max(committed_metrics.artifact_bytes);
        committed_metrics.write_blocks = committed_metrics
            .write_blocks
            .checked_add(artifact.bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64))
            .ok_or_else(|| storage_err("v4 write block count overflow"))?;
        Ok((artifact, committed_metrics))
    })();
    let (artifact, committed_metrics) = match prepared {
        Ok(prepared) => prepared,
        Err(primary) => {
            let (cleanup_evidence, cleanup) = writer.cleanup_unpublished();
            merge_cache_release_evidence(&mut metrics.cache_release, cleanup_evidence);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup publication cleanup");
        }
    };
    drop(writer.writer);
    let published = (|| -> Result<bool, GfError> {
        v4_publication_failure("replace_child")?;
        match writer
            .publication
            .install_child(std::ffi::OsStr::new(&artifact.name))
        {
            Ok(()) => {
                writer.publication.sync_parent().map_err(storage_err)?;
                v4_publication_failure("directory_sync")?;
                v4_publication_failure("post_publication_metric_overflow")?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = writer
                    .publication
                    .open_sibling(std::ffi::OsStr::new(&artifact.name))
                    .map_err(storage_err)?;
                authenticate_private_v4_artifact_file(existing, &artifact)?;
                cleanup_v4_publication(&mut writer.publication)?;
                Ok(false)
            }
            Err(error) => Err(storage_err(error)),
        }
    })();
    let owns_publication = match published {
        Ok(owns_publication) => owns_publication,
        Err(primary) => {
            let cleanup = cleanup_v4_publication(&mut writer.publication);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 publication cleanup");
        }
    };
    let publication = if owns_publication {
        Some(writer.publication)
    } else {
        None
    };
    *metrics = committed_metrics;
    Ok(GuardedV4Artifact {
        artifact,
        publication,
    })
}

struct V4OrdinalRangeWriter {
    artifact: StreamingV4Artifact,
    first_node_id: u64,
    count: u64,
    block: Vec<u8>,
    blocks: Vec<crate::V4OrdinalBlock>,
}

impl V4OrdinalRangeWriter {
    fn new(
        index: &graphforge_filesystem::StableDirectory,
        ordinal: usize,
        first_node_id: u64,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        Ok(Self {
            artifact: StreamingV4Artifact::create_with_window(
                index,
                &format!("ordinal-{ordinal:08}"),
                cache_window,
            )?,
            first_node_id,
            count: 0,
            block: Vec::with_capacity(V4_ORDINAL_BLOCK_BYTES),
            blocks: Vec::new(),
        })
    }

    fn push(&mut self, uuid: [u8; 16]) -> Result<(), GfError> {
        self.artifact.push(&uuid)?;
        self.block.extend_from_slice(&uuid);
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 ordinal range count overflow"))?;
        if self.block.len() == V4_ORDINAL_BLOCK_BYTES {
            self.finish_block()?;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> Result<(), GfError> {
        if self.block.is_empty() {
            return Ok(());
        }
        let offset = u64::try_from(self.blocks.len())
            .map_err(storage_err)?
            .checked_mul(V4_ORDINAL_BLOCK_BYTES as u64)
            .ok_or_else(|| storage_err("v4 ordinal block offset overflow"))?;
        self.blocks.push(crate::V4OrdinalBlock {
            offset,
            count: u64::try_from(self.block.len() / 16).map_err(storage_err)?,
            sha256: hex_sha256(&self.block),
        });
        self.block.clear();
        Ok(())
    }

    fn cleanup_unpublished(
        self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        self.artifact.cleanup_unpublished()
    }
}

fn finish_streamed_v4_range(
    generation: u64,
    mut writer: V4OrdinalRangeWriter,
    ranges: &mut Vec<crate::V4OrdinalRange>,
    publications: &mut Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
    metrics: &mut V4OrdinalBuildMetrics,
) -> Result<(), GfError> {
    let prepared = if writer.count == 0 {
        Err(storage_err("v4 ordinal range is empty"))
    } else {
        writer.finish_block()
    };
    if let Err(primary) = prepared {
        let (cache_release, cleanup) = writer.cleanup_unpublished();
        merge_cache_release_evidence(&mut metrics.cache_release, cache_release);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 ordinal failed output cleanup");
    }
    let artifact = finish_streamed_v4_artifact(
        writer.artifact,
        "ordinal-v4",
        generation,
        crate::V4OrdinalArtifactKind::OrdinalUuids,
        metrics,
    )?;
    let artifact_name = artifact.artifact.name.clone();
    ranges.push(crate::V4OrdinalRange {
        first_node_id: writer.first_node_id,
        count: writer.count,
        artifact: artifact.artifact,
        blocks: writer.blocks,
    });
    retain_v4_publication(publications, artifact_name, artifact.publication);
    Ok(())
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
    let key_width = if width == IDENTITY_RECORD_WIDTH {
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

#[derive(Clone, Copy)]
enum ProbeFileKind {
    Identity,
    Surrogate,
}

fn authenticated_probe_block(
    file: &mut File,
    block: &BlockRecord,
    width: usize,
    kind: ProbeFileKind,
    metrics: &mut UuidProbeMetrics,
) -> Result<Vec<u8>, GfError> {
    file.seek(SeekFrom::Start(block.offset))
        .map_err(storage_err)?;
    metrics.file_seeks = metrics.file_seeks.saturating_add(1);
    let mut bytes = vec![0_u8; block.len as usize];
    file.read_exact(&mut bytes).map_err(storage_err)?;
    let key_width = match width {
        IDENTITY_RECORD_WIDTH => 16,
        NODE_LOOKUP_RECORD_WIDTH => 8,
        _ => return Err(storage_err("unsupported UUID probe record width")),
    };
    if bytes.is_empty()
        || !bytes.len().is_multiple_of(width)
        || hex_sha256(&bytes) != block.sha256
        || hex_sha256_key(&bytes[..key_width]) != block.first_key
        || hex_sha256_key(&bytes[bytes.len() - width..bytes.len() - width + key_width])
            != block.last_key
    {
        return Err(storage_err("UUID probe block authentication failed"));
    }
    match kind {
        ProbeFileKind::Identity => {
            metrics.identity_blocks_read = metrics.identity_blocks_read.saturating_add(1);
            metrics.identity_bytes_read = metrics
                .identity_bytes_read
                .saturating_add(bytes.len() as u64);
        }
        ProbeFileKind::Surrogate => {
            metrics.surrogate_blocks_read = metrics.surrogate_blocks_read.saturating_add(1);
            metrics.surrogate_bytes_read = metrics
                .surrogate_bytes_read
                .saturating_add(bytes.len() as u64);
        }
    }
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
        let mut record_index = 0_usize;
        for uuid in candidates {
            while record_index < bytes.len() / IDENTITY_RECORD_WIDTH {
                let start = record_index * IDENTITY_RECORD_WIDTH;
                let record = &bytes[start..start + IDENTITY_RECORD_WIDTH];
                match record[..16].cmp(uuid.as_bytes()) {
                    std::cmp::Ordering::Less => record_index += 1,
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
                                    record[24..32].try_into().expect("fixed record"),
                                ),
                            },
                        );
                        record_index += 1;
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
    for (index, items) in groups {
        let bytes = authenticated_block(
            &mut run.identities,
            &run.descriptor.identities.blocks[index],
            IDENTITY_RECORD_WIDTH,
            metrics,
        )?;
        for (uuid, kind, surrogate) in items {
            let found = block_record_index(&bytes, IDENTITY_RECORD_WIDTH, uuid.as_bytes())
                .map(|at| &bytes[at * 32..at * 32 + 32]);
            if let Some(record) = found {
                let retained_kind = record[16];
                let retained_surrogate =
                    u64::from_be_bytes(record[24..32].try_into().expect("fixed"));
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
    manifest_file: File,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct V4OrdinalPublicationMetrics {
    pub(crate) write_bytes: u64,
    pub(crate) write_operations: u64,
    pub(crate) fsync_operations: u64,
    pub(crate) peak_temporary_bytes: u64,
}

pub(crate) fn publish_v4_construction_artifacts(
    encoded: &graphforge_filesystem::StableDirectory,
    bundle: V4ConstructionArtifactBundle,
    generation: u64,
    topology_delta_sha256: &str,
) -> Result<
    (
        Vec<ConstructionIndexOutput>,
        V4OrdinalPublicationMetrics,
        V4OrdinalBuildMetrics,
    ),
    GfError,
> {
    let V4ConstructionArtifactBundle {
        manifest,
        metrics,
        mut publications,
    } = bundle;
    let published = publish_v4_construction_artifacts_inner(
        encoded,
        &manifest,
        &metrics,
        &mut publications,
        generation,
        topology_delta_sha256,
    );
    match published {
        Ok(published) => {
            commit_v4_publications(publications, V4AuthorityTransactionProof)?;
            Ok(published)
        }
        Err(primary) => {
            let cleanup = cleanup_v4_publications(&mut publications);
            combine_v4_cleanup(Err(primary), cleanup, "v4 authority transaction cleanup")
        }
    }
}

fn cleanup_v4_publications(
    publications: &mut Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
) -> Result<(), GfError> {
    let mut cleanup = Ok(());
    for (_, mut publication) in publications.drain(..).rev() {
        cleanup = combine_v4_cleanup(
            cleanup,
            cleanup_v4_publication(&mut publication),
            "v4 publication guard cleanup",
        );
    }
    cleanup
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn publish_v4_construction_artifacts_inner(
    encoded: &graphforge_filesystem::StableDirectory,
    manifest: &crate::V4OrdinalIdentityManifest,
    metrics: &V4OrdinalBuildMetrics,
    publications: &mut Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
    generation: u64,
    topology_delta_sha256: &str,
) -> Result<
    (
        Vec<ConstructionIndexOutput>,
        V4OrdinalPublicationMetrics,
        V4OrdinalBuildMetrics,
    ),
    GfError,
> {
    let graph = encoded
        .open_child_directory(std::ffi::OsStr::new("graph"))
        .map_err(storage_err)?;
    let topology = graph
        .open_child_directory(std::ffi::OsStr::new("topology"))
        .map_err(storage_err)?;
    let index = topology
        .open_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .map_err(storage_err)?;
    let manifest_body = serde_json::to_vec(manifest).map_err(storage_err)?;
    let receipt = TopologyIndexReceipt {
        nonce: Uuid::new_v4().simple().to_string(),
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256.to_owned(),
        manifest_sha256: hex_sha256(&manifest_body),
    };
    let receipt_body = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let mut work = ConstructionIndexWork::default();
    let mut outputs = manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .map(|artifact| ConstructionIndexOutput {
            name: artifact.name.clone(),
            bytes: artifact.bytes,
            sha256: artifact.sha256.clone(),
        })
        .collect::<Vec<_>>();
    crate::graph_construction::construction_failpoint("v4_publish.after_artifacts");
    v4_authority_failure("after_artifacts")?;
    index.sync().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    crate::graph_construction::construction_failpoint("v4_publish.after_artifacts_fsync");
    let artifact_bytes = outputs.iter().try_fold(0_u64, |total, output| {
        total
            .checked_add(output.bytes)
            .ok_or_else(|| storage_err("v4 publication artifact byte count overflow"))
    })?;
    // The selected project generation is still unpublished. Install the
    // receipt first, then its manifest, and create the lock last so every
    // visible construction inventory is complete and reopenable.
    let (receipt_output, receipt_publication) =
        install_construction_bytes(&index, V4_ORDINAL_RECEIPT, &receipt_body, &mut work)?;
    outputs.push(receipt_output);
    publications.push((V4_ORDINAL_RECEIPT.to_owned(), receipt_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_receipt_install");
    let (manifest_output, manifest_publication) =
        install_construction_bytes(&index, V4_ORDINAL_MANIFEST, &manifest_body, &mut work)?;
    outputs.push(manifest_output);
    publications.push((V4_ORDINAL_MANIFEST.to_owned(), manifest_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_manifest_install");
    let (lock_output, lock_publication) =
        install_construction_bytes(&index, "ordinal-v4.lock", &[], &mut work)?;
    outputs.push(lock_output);
    publications.push(("ordinal-v4.lock".to_owned(), lock_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_lock_install");
    index.sync().map_err(storage_err)?;
    topology.sync().map_err(storage_err)?;
    graph.sync().map_err(storage_err)?;
    encoded.sync().map_err(storage_err)?;
    v4_authority_failure("directory_sync")?;
    work.fsync_operations = work.fsync_operations.saturating_add(4);
    let publication_metrics = V4OrdinalPublicationMetrics {
        write_bytes: work.write_bytes,
        write_operations: work.write_operations,
        fsync_operations: work.fsync_operations,
        peak_temporary_bytes: artifact_bytes
            .checked_add(work.write_bytes)
            .ok_or_else(|| storage_err("v4 publication temporary byte count overflow"))?,
    };
    Ok((outputs, publication_metrics, metrics.clone()))
}

#[derive(Default)]
struct ConstructionIndexWork {
    read_bytes: u64,
    read_operations: u64,
    write_bytes: u64,
    write_operations: u64,
    fsync_operations: u64,
    created_runs: u64,
    retained_runs: u64,
    retained_payload_bytes: u64,
    peak_buffer_bytes: u64,
    peak_temporary_bytes: u64,
    cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

fn merge_cache_release_evidence(
    target: &mut graphforge_filesystem::FileCacheReleaseEvidence,
    source: graphforge_filesystem::FileCacheReleaseEvidence,
) {
    target.sync_operations = target
        .sync_operations
        .saturating_add(source.sync_operations);
    target.release_operations = target
        .release_operations
        .saturating_add(source.release_operations);
    target.unsupported_operations = target
        .unsupported_operations
        .saturating_add(source.unsupported_operations);
    target.released_bytes = target.released_bytes.saturating_add(source.released_bytes);
    target.peak_window_bytes = target.peak_window_bytes.max(source.peak_window_bytes);
}

#[derive(Serialize, Deserialize)]
struct ConstructionRecoveryIntent {
    format_version: u32,
    generation: u64,
    parent_generation: u64,
    identities_name: String,
    source_volume: u64,
    source_file_id: String,
    source_bytes: u64,
    source_sha256: String,
    authority_sha256: String,
}

struct ConstructionIndexCleanupGuard<'a> {
    encoded: &'a graphforge_filesystem::StableDirectory,
    armed: bool,
}

impl ConstructionIndexCleanupGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ConstructionIndexCleanupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = cleanup_private_construction_index(self.encoded);
        }
    }
}

impl ConstructionRecoveryIntent {
    fn authenticate(&self) -> Result<(), GfError> {
        let expected = construction_intent_digest(
            self.format_version,
            self.generation,
            self.parent_generation,
            &self.identities_name,
            self.source_volume,
            &self.source_file_id,
            self.source_bytes,
            &self.source_sha256,
        );
        if self.format_version != FORMAT_VERSION || self.authority_sha256 != expected {
            return Err(storage_err(
                "construction recovery intent authentication failed",
            ));
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn construction_intent_digest(
    format_version: u32,
    generation: u64,
    parent_generation: u64,
    identities_name: &str,
    source_volume: u64,
    source_file_id: &str,
    source_bytes: u64,
    source_sha256: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge.uuid-membership.construction-intent.v2\0");
    digest.update(format_version.to_be_bytes());
    digest.update(generation.to_be_bytes());
    digest.update(parent_generation.to_be_bytes());
    digest.update((identities_name.len() as u64).to_be_bytes());
    digest.update(identities_name.as_bytes());
    digest.update(source_volume.to_be_bytes());
    digest.update(source_file_id.as_bytes());
    digest.update(source_bytes.to_be_bytes());
    digest.update(source_sha256.as_bytes());
    hex_bytes(&digest.finalize())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConstructionUuidIdentity {
    pub uuid: Uuid,
    pub kind: UuidIndexKind,
    pub surrogate: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UuidConstructionSnapshotWork {
    pub authentication_bytes: u64,
    pub authentication_blocks: u64,
    pub live_nodes: u64,
    pub live_edges: u64,
    pub max_node_surrogate: u64,
}

/// Retained authority for a UUID snapshot authenticated exactly once while its
/// live identities were emitted as one bounded sorted stream.
#[cfg(test)]
pub(crate) struct UuidConstructionSnapshot {
    root: graphforge_filesystem::StableDirectory,
    root_identity: graphforge_filesystem::FileIdentity,
    manifest_file: File,
    manifest_identity: graphforge_filesystem::FileIdentity,
    manifest_sha256: String,
    manifest_bytes: u64,
    manifest: Manifest,
    named_files: Vec<(String, graphforge_filesystem::FileIdentity)>,
    payload_consumed: bool,
}

#[cfg(test)]
impl UuidConstructionSnapshot {
    pub(crate) fn revalidate(&self) -> Result<(), GfError> {
        self.root.revalidate_named().map_err(storage_err)?;
        if self.root.identity() != self.root_identity
            || graphforge_filesystem::file_identity(&self.manifest_file).map_err(storage_err)?
                != self.manifest_identity
            || graphforge_filesystem::file_link_count(&self.manifest_file).map_err(storage_err)?
                != 1
        {
            return Err(storage_err("construction UUID snapshot authority changed"));
        }
        let mut manifest = self
            .root
            .open_child_file(std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        let body = read_bounded(&mut manifest, MAX_MANIFEST_BYTES)?;
        if graphforge_filesystem::file_identity(&manifest).map_err(storage_err)?
            != self.manifest_identity
            || hex_sha256(&body) != self.manifest_sha256
            || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
        {
            return Err(storage_err("construction UUID manifest changed"));
        }
        for (name, identity) in &self.named_files {
            let file = self
                .root
                .open_child_file(std::ffi::OsStr::new(name))
                .map_err(storage_err)?;
            if graphforge_filesystem::file_identity(&file).map_err(storage_err)? != *identity
                || graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1
            {
                return Err(storage_err("construction UUID run identity changed"));
            }
        }
        Ok(())
    }

    /// Authenticate each retained payload block exactly once and emit the live
    /// identity domain as a bounded UUID-ordered stream.
    pub(crate) fn stream_authenticated(
        &mut self,
        mut emit: impl FnMut(ConstructionUuidIdentity) -> Result<(), GfError>,
    ) -> Result<UuidConstructionSnapshotWork, GfError> {
        if self.payload_consumed {
            return Err(storage_err(
                "construction UUID payload was already consumed",
            ));
        }
        self.revalidate()?;
        let mut identity_cursors = Vec::with_capacity(self.manifest.runs.len());
        let mut work = UuidConstructionSnapshotWork {
            authentication_bytes: self.manifest_bytes,
            authentication_blocks: 1,
            ..Default::default()
        };
        for run in &self.manifest.runs {
            let identities = self
                .root
                .open_child_file(std::ffi::OsStr::new(&run.identities.name))
                .map_err(storage_err)?;
            identity_cursors.push(ConstructionRunCursor::new(
                identities,
                run.identities.clone(),
                IDENTITY_RECORD_WIDTH,
            ));
            let surrogates = self
                .root
                .open_child_file(std::ffi::OsStr::new(&run.node_surrogates.name))
                .map_err(storage_err)?;
            let mut cursor = ConstructionRunCursor::new(
                surrogates,
                run.node_surrogates.clone(),
                NODE_LOOKUP_RECORD_WIDTH,
            );
            while cursor.next_record()?.is_some() {}
            work.authentication_bytes = work.authentication_bytes.saturating_add(cursor.bytes);
            work.authentication_blocks = work.authentication_blocks.saturating_add(cursor.blocks);
        }
        let mut heads = identity_cursors
            .iter_mut()
            .map(ConstructionRunCursor::next_record)
            .collect::<Result<Vec<_>, _>>()?;
        loop {
            let Some(next_uuid) = heads
                .iter()
                .flatten()
                .map(|record| &record[..16])
                .min()
                .map(<[u8]>::to_vec)
            else {
                break;
            };
            let indexes = heads
                .iter()
                .enumerate()
                .filter_map(|(index, record)| {
                    record
                        .as_ref()
                        .is_some_and(|record| record[..16] == next_uuid)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            let selected = *indexes
                .iter()
                .max_by_key(|index| self.manifest.runs[**index].last_generation)
                .expect("one UUID head was selected");
            let record = heads[selected].as_ref().expect("selected head exists");
            let kind = record[16];
            if !matches!(kind, 0..=3) || record[17..24].iter().any(|byte| *byte != 0) {
                return Err(storage_err(
                    "construction UUID identity record is malformed",
                ));
            }
            if matches!(kind, 0 | 1) {
                let identity = ConstructionUuidIdentity {
                    uuid: Uuid::from_bytes(record[..16].try_into().expect("fixed UUID width")),
                    kind: if kind == 0 {
                        UuidIndexKind::Node
                    } else {
                        UuidIndexKind::Edge
                    },
                    surrogate: u64::from_be_bytes(record[24..32].try_into().expect("fixed")),
                };
                if identity.kind == UuidIndexKind::Node {
                    if identity.surrogate == 0 {
                        return Err(storage_err("live node has zero surrogate"));
                    }
                    work.live_nodes = work.live_nodes.saturating_add(1);
                    work.max_node_surrogate = work.max_node_surrogate.max(identity.surrogate);
                } else {
                    work.live_edges = work.live_edges.saturating_add(1);
                }
                emit(identity)?;
            }
            for index in indexes {
                heads[index] = identity_cursors[index].next_record()?;
            }
        }
        for cursor in &identity_cursors {
            work.authentication_bytes = work.authentication_bytes.saturating_add(cursor.bytes);
            work.authentication_blocks = work.authentication_blocks.saturating_add(cursor.blocks);
        }
        if work.live_nodes != self.manifest.live_node_count
            || work.live_edges != self.manifest.live_edge_count
        {
            return Err(storage_err(
                "construction UUID live counts differ from manifest",
            ));
        }
        self.payload_consumed = true;
        self.revalidate()?;
        Ok(work)
    }
}

#[cfg(test)]
struct ConstructionRunCursor {
    file: File,
    descriptor: FileRecord,
    width: usize,
    block_index: usize,
    block: Vec<u8>,
    within: usize,
    records: u64,
    bytes: u64,
    blocks: u64,
    digest: Sha256,
    finished: bool,
}

#[cfg(test)]
impl ConstructionRunCursor {
    fn new(file: File, descriptor: FileRecord, width: usize) -> Self {
        Self {
            file,
            descriptor,
            width,
            block_index: 0,
            block: Vec::new(),
            within: 0,
            records: 0,
            bytes: 0,
            blocks: 0,
            digest: Sha256::new(),
            finished: false,
        }
    }

    fn next_record(&mut self) -> Result<Option<Vec<u8>>, GfError> {
        if self.finished {
            return Ok(None);
        }
        if self.within == self.block.len() {
            if self.block_index == self.descriptor.blocks.len() {
                self.finished = true;
                if self.records != self.descriptor.count
                    || self.bytes != self.descriptor.count.saturating_mul(self.width as u64)
                    || hex_bytes(&self.digest.clone().finalize()) != self.descriptor.sha256
                {
                    return Err(storage_err("construction UUID run authentication failed"));
                }
                return Ok(None);
            }
            let descriptor = &self.descriptor.blocks[self.block_index];
            if descriptor.offset != self.bytes
                || descriptor.len as usize % self.width != 0
                || descriptor.len == 0
            {
                return Err(storage_err("construction UUID block framing changed"));
            }
            self.block.resize(descriptor.len as usize, 0);
            self.file.read_exact(&mut self.block).map_err(storage_err)?;
            let key_width = if self.width == IDENTITY_RECORD_WIDTH {
                16
            } else {
                8
            };
            if hex_sha256(&self.block) != descriptor.sha256
                || hex_sha256_key(&self.block[..key_width]) != descriptor.first_key
                || hex_sha256_key(
                    &self.block
                        [self.block.len() - self.width..self.block.len() - self.width + key_width],
                ) != descriptor.last_key
            {
                return Err(storage_err("construction UUID block digest changed"));
            }
            self.digest.update(&self.block);
            self.bytes = self.bytes.saturating_add(self.block.len() as u64);
            self.blocks = self.blocks.saturating_add(1);
            self.block_index += 1;
            self.within = 0;
        }
        let end = self.within + self.width;
        let record = self.block[self.within..end].to_vec();
        self.within = end;
        self.records = self.records.saturating_add(1);
        Ok(Some(record))
    }
}

/// Authenticate each retained UUID byte once and emit the live identity set in
/// UUID order. The returned token revalidates inode/name authority without
/// rereading retained payload bytes.
#[cfg(test)]
pub(crate) fn open_uuid_construction_snapshot(
    project_dir: &Path,
    generation: u64,
    emit: impl FnMut(ConstructionUuidIdentity) -> Result<(), GfError>,
) -> Result<(UuidConstructionSnapshot, UuidConstructionSnapshotWork), GfError> {
    let mut token = pin_uuid_construction_snapshot(project_dir, generation)?;
    let work = token.stream_authenticated(emit)?;
    Ok((token, work))
}

/// Pin the generation's manifest and every run inode without reading retained
/// payload bytes.  The caller later consumes those bytes exactly once through
/// [`UuidConstructionSnapshot::stream_authenticated`].
#[cfg(test)]
pub(crate) fn pin_uuid_construction_snapshot(
    project_dir: &Path,
    generation: u64,
) -> Result<UuidConstructionSnapshot, GfError> {
    let root = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
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
        return Err(storage_err(
            "construction UUID snapshot generation is stale",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let mut named_files = Vec::with_capacity(manifest.runs.len().saturating_mul(2));
    for run in &manifest.runs {
        let identities = root
            .open_child_file(std::ffi::OsStr::new(&run.identities.name))
            .map_err(storage_err)?;
        let identity = graphforge_filesystem::file_identity(&identities).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&identities).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "construction UUID identity run has extra links",
            ));
        }
        named_files.push((run.identities.name.clone(), identity));

        let surrogates = root
            .open_child_file(std::ffi::OsStr::new(&run.node_surrogates.name))
            .map_err(storage_err)?;
        let surrogate_identity =
            graphforge_filesystem::file_identity(&surrogates).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&surrogates).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "construction UUID surrogate run has extra links",
            ));
        }
        named_files.push((run.node_surrogates.name.clone(), surrogate_identity));
    }
    root.revalidate_named().map_err(storage_err)?;
    let token = UuidConstructionSnapshot {
        root,
        root_identity,
        manifest_file,
        manifest_identity,
        manifest_sha256,
        manifest_bytes: body.len() as u64,
        manifest,
        named_files,
        payload_consumed: false,
    };
    token.revalidate()?;
    Ok(token)
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

impl AuthenticatedUuidIndexSnapshot {
    fn open_retained_file(&self, record: &FileRecord) -> Result<File, GfError> {
        let (held, expected) = self
            .runs
            .iter()
            .find_map(|run| {
                if run.descriptor.identities == *record {
                    Some((&run.identities, run.identities_identity))
                } else if run.descriptor.node_surrogates == *record {
                    Some((&run.node_surrogates, run.node_surrogates_identity))
                } else {
                    None
                }
            })
            .ok_or_else(|| storage_err("retained UUID descriptor is not authenticated"))?;
        let mut file = held.try_clone().map_err(storage_err)?;
        let identity_changed =
            graphforge_filesystem::file_identity(&file).map_err(storage_err)? != expected;
        let path_native_link_changed = self.cas_source_paths.is_none()
            && graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1;
        if identity_changed || path_native_link_changed {
            return Err(storage_err("retained UUID run identity changed"));
        }
        file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
        Ok(file)
    }

    fn retained_reference(
        &self,
        record: &FileRecord,
    ) -> Result<ConstructionIndexReference, GfError> {
        let file = self.open_retained_file(record)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        let source_path = self
            .cas_source_paths
            .as_ref()
            .and_then(|paths| paths.get(&record.name))
            .map_or_else(
                || format!("{INDEX_DIR}/{}", record.name),
                |(path, _, _)| path.clone(),
            );
        Ok(ConstructionIndexReference {
            source_root: self.graph_root_path.to_string_lossy().into_owned(),
            source_root_volume: self.graph_root_identity.volume_serial,
            source_root_file_id: hex_bytes(&self.graph_root_identity.file_id),
            source_path,
            source_volume: identity.volume_serial,
            source_file_id: hex_bytes(&identity.file_id),
            target_path: format!("{INDEX_DIR}/{}", record.name),
            bytes: record
                .count
                .saturating_mul(if record.name.starts_with("identities-") {
                    IDENTITY_RECORD_BYTES
                } else {
                    NODE_LOOKUP_RECORD_BYTES
                }),
            sha256: record.sha256.clone(),
            parent_manifest_sha256: self.manifest_sha256.clone(),
        })
    }

    pub(crate) fn authenticate_construction_references(
        &self,
        references: &[ConstructionReferenceAuthentication<'_>],
    ) -> Result<ConstructionReferenceAuthenticationWork, GfError> {
        self.authenticate_construction_references_with(references, || {})
    }

    fn authenticate_construction_references_with(
        &self,
        references: &[ConstructionReferenceAuthentication<'_>],
        before_final_revalidation: impl FnOnce(),
    ) -> Result<ConstructionReferenceAuthenticationWork, GfError> {
        self.revalidate()?;
        let mut referenced_payload_bytes = 0_u64;
        for reference in references {
            referenced_payload_bytes = referenced_payload_bytes
                .saturating_add(self.authenticate_construction_reference_once(reference)?);
        }
        before_final_revalidation();
        self.revalidate()?;
        Ok(ConstructionReferenceAuthenticationWork {
            global_revalidation_bytes: self.snapshot_authentication_bytes().saturating_mul(2),
            referenced_payload_bytes,
        })
    }

    fn snapshot_authentication_bytes(&self) -> u64 {
        let manifest_bytes = self
            .manifest_file
            .metadata()
            .map_or(0, |metadata| metadata.len());
        self.manifest
            .runs
            .iter()
            .fold(manifest_bytes, |total, run| {
                total
                    .saturating_add(run.identities.count.saturating_mul(IDENTITY_RECORD_BYTES))
                    .saturating_add(
                        run.node_surrogates
                            .count
                            .saturating_mul(NODE_LOOKUP_RECORD_BYTES),
                    )
            })
    }

    fn authenticate_construction_reference_once(
        &self,
        reference: &ConstructionReferenceAuthentication<'_>,
    ) -> Result<u64, GfError> {
        if reference.source_root != self.graph_root_path.to_string_lossy()
            || reference.source_root_volume != self.graph_root_identity.volume_serial
            || reference.source_root_file_id != hex_bytes(&self.graph_root_identity.file_id)
            || reference.parent_manifest_sha256 != self.manifest_sha256
            || !reference.target_path.starts_with(&format!("{INDEX_DIR}/"))
        {
            return Err(storage_err(
                "retained construction reference authority changed",
            ));
        }
        let name = reference
            .target_path
            .strip_prefix(&format!("{INDEX_DIR}/"))
            .ok_or_else(|| storage_err("retained construction target path is invalid"))?;
        let expected_source = self
            .cas_source_paths
            .as_ref()
            .and_then(|paths| paths.get(name))
            .map_or(reference.target_path, |(path, _, _)| path.as_str());
        if reference.source_path != expected_source {
            return Err(storage_err("retained construction source path changed"));
        }
        let record = self
            .manifest
            .runs
            .iter()
            .flat_map(|run| [&run.identities, &run.node_surrogates])
            .find(|record| record.name == name)
            .ok_or_else(|| storage_err("retained construction run is absent"))?;
        let mut file = self.open_retained_file(record)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        let expected_bytes =
            record
                .count
                .saturating_mul(if record.name.starts_with("identities-") {
                    IDENTITY_RECORD_BYTES
                } else {
                    NODE_LOOKUP_RECORD_BYTES
                });
        file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
        let mut digest = Sha256::new();
        let mut actual_bytes = 0_u64;
        let mut block = vec![0_u8; BULK_IO_BYTES];
        loop {
            let count = file.read(&mut block).map_err(storage_err)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            actual_bytes = actual_bytes.saturating_add(count as u64);
        }
        if identity.volume_serial != reference.source_volume
            || hex_bytes(&identity.file_id) != reference.source_file_id
            || reference.bytes != expected_bytes
            || actual_bytes != expected_bytes
            || reference.sha256 != record.sha256
            || hex_bytes(&digest.finalize()) != record.sha256
        {
            return Err(storage_err(format!(
                "retained construction reference changed: volume={} expected_volume={} file_id={} expected_file_id={} bytes={} expected_bytes={} reference_sha={} manifest_sha={}",
                identity.volume_serial,
                reference.source_volume,
                hex_bytes(&identity.file_id),
                reference.source_file_id,
                actual_bytes,
                expected_bytes,
                reference.sha256,
                record.sha256
            )));
        }
        Ok(actual_bytes)
    }

    pub(crate) fn open_at_generation(project_dir: &Path, generation: u64) -> Result<Self, GfError> {
        let graph_root =
            graphforge_filesystem::StableDirectory::open(project_dir).map_err(storage_err)?;
        let graph_root_identity = graph_root.identity();
        let root_path = project_dir.join(INDEX_DIR);
        let root = graphforge_filesystem::StableDirectory::open(&root_path).map_err(storage_err)?;
        let root_identity = root.identity();
        let mut manifest_file = open_uuid_child_file(&root, std::ffi::OsStr::new(MANIFEST))?;
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
            graph_root,
            graph_root_path: project_dir.to_path_buf(),
            graph_root_identity,
            root,
            root_identity,
            manifest_file,
            manifest_identity,
            manifest_sha256,
            manifest,
            runs,
            authenticated_bytes,
            authenticated_blocks,
            cas_source_paths: None,
            _cas_leases: Vec::new(),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one descriptor-lifetime authentication pass keeps every CAS file and manifest binding in scope"
    )]
    pub(crate) fn open_from_compact_inventory(
        container_root: &Path,
        inventory: &crate::GraphFilesInventory,
        generation: u64,
    ) -> Result<Self, GfError> {
        let graph_root =
            graphforge_filesystem::StableDirectory::open(container_root).map_err(storage_err)?;
        let graph_root_identity = graph_root.identity();
        let root =
            graphforge_filesystem::StableDirectory::open(container_root).map_err(storage_err)?;
        let root_identity = root.identity();
        let manifest_path = format!("{INDEX_DIR}/{MANIFEST}");
        let manifest_entry = inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == manifest_path)
            .ok_or_else(|| storage_err("compact UUID manifest is absent"))?;
        let mut manifest_lease = crate::graph_object_store::open_graph_object_by_digest(
            container_root,
            &manifest_entry.content_sha256,
            manifest_entry.byte_length,
        )?;
        let manifest_identity =
            graphforge_filesystem::file_identity(manifest_lease.as_ref()).map_err(storage_err)?;
        let body = read_bounded(&mut manifest_lease, MAX_MANIFEST_BYTES)?;
        let manifest_file = manifest_lease.try_clone_file().map_err(storage_err)?;
        let manifest_sha256 = hex_sha256(&body);
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION || manifest.current_generation != generation {
            return Err(storage_err(
                "authenticated compact snapshot generation is stale",
            ));
        }
        validate_run_descriptors(&manifest)?;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        let mut paths = BTreeMap::new();
        let manifest_physical =
            crate::graph_object_path(container_root, &manifest_entry.content_sha256)?;
        paths.insert(
            MANIFEST.to_owned(),
            (
                manifest_physical
                    .strip_prefix(container_root)
                    .map_err(storage_err)?
                    .to_string_lossy()
                    .into_owned(),
                manifest_entry.content_sha256.clone(),
                manifest_entry.byte_length,
            ),
        );
        let mut cas_leases = vec![manifest_lease];
        let mut authenticated_bytes = body.len() as u64;
        let mut authenticated_blocks = 1_u64;
        for descriptor in &manifest.runs {
            let mut open_record = |record: &FileRecord, width: u64| -> Result<File, GfError> {
                let logical = format!("{INDEX_DIR}/{}", record.name);
                let entry = inventory
                    .files
                    .iter()
                    .find(|entry| entry.relative_path == logical)
                    .ok_or_else(|| storage_err("compact UUID run is absent"))?;
                if entry.content_sha256 != record.sha256
                    || entry.byte_length != record.count.saturating_mul(width)
                {
                    return Err(storage_err("compact UUID run authority changed"));
                }
                let lease = crate::graph_object_store::open_graph_object_by_digest(
                    container_root,
                    &entry.content_sha256,
                    entry.byte_length,
                )?;
                let physical = crate::graph_object_path(container_root, &entry.content_sha256)?;
                let relative = physical
                    .strip_prefix(container_root)
                    .map_err(storage_err)?
                    .to_string_lossy()
                    .into_owned();
                paths.insert(
                    record.name.clone(),
                    (relative, entry.content_sha256.clone(), entry.byte_length),
                );
                let file = lease.try_clone_file().map_err(storage_err)?;
                cas_leases.push(lease);
                Ok(file)
            };
            let identities = open_record(&descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_record(&descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            authenticate_file_blocks(
                &mut identities.try_clone().map_err(storage_err)?,
                &descriptor.identities,
                IDENTITY_RECORD_BYTES,
                None,
            )?;
            authenticate_file_blocks(
                &mut node_surrogates.try_clone().map_err(storage_err)?,
                &descriptor.node_surrogates,
                NODE_LOOKUP_RECORD_BYTES,
                None,
            )?;
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
            graph_root,
            graph_root_path: container_root.to_path_buf(),
            graph_root_identity,
            root,
            root_identity,
            manifest_file,
            manifest_identity,
            manifest_sha256,
            manifest,
            runs,
            authenticated_bytes,
            authenticated_blocks,
            cas_source_paths: Some(paths),
            _cas_leases: cas_leases,
        })
    }

    pub(crate) fn topology_generation(&self) -> u64 {
        self.manifest.current_generation
    }

    pub(crate) fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.manifest.live_node_count,
            UuidIndexKind::Edge => self.manifest.live_edge_count,
        }
    }

    pub(crate) fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }
    pub(crate) fn take_authentication_work(&mut self) -> (u64, u64) {
        (
            std::mem::take(&mut self.authenticated_bytes),
            std::mem::take(&mut self.authenticated_blocks),
        )
    }

    pub(crate) fn revalidate(&self) -> Result<(), GfError> {
        self.graph_root.revalidate_named().map_err(storage_err)?;
        if self.graph_root.identity() != self.graph_root_identity {
            return Err(storage_err("UUID graph root identity changed"));
        }
        if let Some(objects) = &self.cas_source_paths {
            let (_, manifest_digest, manifest_length) = objects
                .get(MANIFEST)
                .ok_or_else(|| storage_err("compact UUID manifest authority is absent"))?;
            let mut manifest_lease = crate::graph_object_store::open_graph_object_by_digest(
                &self.graph_root_path,
                manifest_digest,
                *manifest_length,
            )?;
            if graphforge_filesystem::file_identity(manifest_lease.as_ref()).map_err(storage_err)?
                != self.manifest_identity
            {
                return Err(storage_err("compact UUID manifest identity changed"));
            }
            let body = read_bounded(&mut manifest_lease, MAX_MANIFEST_BYTES)?;
            if hex_sha256(&body) != self.manifest_sha256
                || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
            {
                return Err(storage_err("compact UUID manifest authentication changed"));
            }
            for run in &self.runs {
                for (record, identity) in [
                    (&run.descriptor.identities, run.identities_identity),
                    (
                        &run.descriptor.node_surrogates,
                        run.node_surrogates_identity,
                    ),
                ] {
                    let (_, digest, length) = objects
                        .get(&record.name)
                        .ok_or_else(|| storage_err("compact UUID run authority is absent"))?;
                    let file = crate::graph_object_store::open_graph_object_by_digest(
                        &self.graph_root_path,
                        digest,
                        *length,
                    )?;
                    if graphforge_filesystem::file_identity(file.as_ref()).map_err(storage_err)?
                        != identity
                    {
                        return Err(storage_err("compact UUID run identity changed"));
                    }
                }
            }
            return Ok(());
        }
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
        let mut named_manifest = open_uuid_child_file(&self.root, std::ffi::OsStr::new(MANIFEST))?;
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
                let named = open_uuid_child_file(&self.root, std::ffi::OsStr::new(&record.name))?;
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
        let mut manifest_file = open_uuid_child_file(&self.root, std::ffi::OsStr::new(MANIFEST))?;
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
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                kind,
                &unresolved,
                &mut metrics,
            )?;
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                metrics.found = metrics.found.saturating_add(u64::from(state.present));
                resolved.insert(uuid, state.present);
            }
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().unwrap_or(false))
                .collect(),
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
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                UuidIndexKind::Node,
                &unresolved,
                &mut metrics,
            )?;
            let mut pairs = Vec::new();
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                let value = state.present.then_some(state.surrogate);
                if let Some(surrogate) = value {
                    pairs.push((surrogate, uuid));
                    metrics.found = metrics.found.saturating_add(1);
                }
                resolved.insert(uuid, value);
            }
            validate_surrogate_pairs(
                &mut run.node_surrogates,
                &run.descriptor.node_surrogates,
                &pairs,
                &mut metrics,
            )?;
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().flatten())
                .collect(),
            metrics,
        ))
    }
}

/// Encode the shaper's UUID-ordered 32-byte delta directly as a v3 membership
/// participant. Retained descriptors are cloned, not rebuilt from topology;
/// retained payloads are read only when binary-carry compaction is required.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn encode_construction_index(
    source: &graphforge_filesystem::StableDirectory,
    identities_name: &str,
    identities_sha256: &str,
    encoded: &graphforge_filesystem::StableDirectory,
    generation: u64,
    parent_generation: u64,
    parent: Option<&AuthenticatedUuidIndexSnapshot>,
    live_nodes: u64,
    live_edges: u64,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<ConstructionIndexEncoding, GfError> {
    cleanup_private_construction_index(encoded)?;
    let mut cleanup_guard = ConstructionIndexCleanupGuard {
        encoded,
        armed: true,
    };
    let result = encode_construction_index_inner(
        source,
        identities_name,
        identities_sha256,
        encoded,
        generation,
        parent_generation,
        parent,
        live_nodes,
        live_edges,
        cancelled,
    );
    match result {
        Ok(value) => {
            cleanup_guard.disarm();
            Ok(value)
        }
        Err(original) => {
            cleanup_private_construction_index(encoded).map_err(|cleanup| {
                storage_err(format!("{original}; exact cleanup also failed: {cleanup}"))
            })?;
            Err(original)
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_construction_index_inner(
    source: &graphforge_filesystem::StableDirectory,
    identities_name: &str,
    identities_sha256: &str,
    encoded: &graphforge_filesystem::StableDirectory,
    generation: u64,
    parent_generation: u64,
    parent: Option<&AuthenticatedUuidIndexSnapshot>,
    live_nodes: u64,
    live_edges: u64,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<ConstructionIndexEncoding, GfError> {
    let graph = encoded
        .create_child_directory(std::ffi::OsStr::new("graph"))
        .map_err(storage_err)?;
    let topology = graph
        .create_child_directory(std::ffi::OsStr::new("topology"))
        .map_err(storage_err)?;
    let index = topology
        .create_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .map_err(storage_err)?;
    let mut manifest = if parent_generation == 0 {
        if parent.is_some() {
            return Err(storage_err("empty construction parent has UUID snapshot"));
        }
        Manifest {
            format_version: FORMAT_VERSION,
            base_generation: 0,
            current_generation: 0,
            live_node_count: 0,
            live_edge_count: 0,
            runs: Vec::new(),
        }
    } else {
        let parent = parent.ok_or_else(|| {
            storage_err("nonempty construction parent lacks authenticated UUID snapshot")
        })?;
        parent.revalidate()?;
        if parent.manifest.current_generation != parent_generation {
            return Err(storage_err("construction UUID parent generation changed"));
        }
        parent.manifest.clone()
    };
    if generation != parent_generation.saturating_add(1) {
        return Err(storage_err(
            "construction UUID generation is not consecutive",
        ));
    }

    let mut work = ConstructionIndexWork::default();
    let identity_temp = format!(".construction-identities-{}.tmp", Uuid::new_v4().simple());
    let surrogate_temp = format!(".construction-surrogates-{}.tmp", Uuid::new_v4().simple());
    let input = source
        .open_child_file(std::ffi::OsStr::new(identities_name))
        .map_err(storage_err)?;
    let input_len = input.metadata().map_err(storage_err)?.len();
    if input_len % IDENTITY_RECORD_BYTES != 0 {
        return Err(storage_err("construction identity stream is truncated"));
    }
    let source_identity = graphforge_filesystem::file_identity(&input).map_err(storage_err)?;
    let source_file_id = hex_bytes(&source_identity.file_id);
    let mut intent = ConstructionRecoveryIntent {
        format_version: FORMAT_VERSION,
        generation,
        parent_generation,
        identities_name: identities_name.to_owned(),
        source_volume: source_identity.volume_serial,
        source_file_id: source_file_id.clone(),
        source_bytes: input_len,
        source_sha256: identities_sha256.to_owned(),
        authority_sha256: String::new(),
    };
    intent.authority_sha256 = construction_intent_digest(
        intent.format_version,
        intent.generation,
        intent.parent_generation,
        &intent.identities_name,
        intent.source_volume,
        &intent.source_file_id,
        intent.source_bytes,
        &intent.source_sha256,
    );
    write_construction_intent(&index, &intent, &mut work)?;
    crate::graph_construction::construction_failpoint("uuid_encode.after_intent");
    let identity_writer = index
        .create_replaceable_child_file(std::ffi::OsStr::new(&identity_temp))
        .map_err(storage_err)?;
    let surrogate_writer = index
        .create_replaceable_child_file(std::ffi::OsStr::new(&surrogate_temp))
        .map_err(storage_err)?;
    let identity_identity =
        graphforge_filesystem::file_identity(&identity_writer).map_err(storage_err)?;
    let surrogate_identity =
        graphforge_filesystem::file_identity(&surrogate_writer).map_err(storage_err)?;
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(3).map_err(storage_err)?;
    let mut input = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
        input,
        cache_window,
        graphforge_filesystem::FileCacheReleaseTracker::default(),
    )
    .map_err(storage_err)?;
    let mut identity_writer = graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(
        identity_writer,
        cache_window,
    )
    .map_err(storage_err)?;
    let mut surrogate_writer = graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(
        surrogate_writer,
        cache_window,
    )
    .map_err(storage_err)?;
    crate::graph_construction::construction_failpoint("uuid_encode.after_temps");
    let aligned_input_bytes = (BULK_IO_BYTES / IDENTITY_RECORD_WIDTH) * IDENTITY_RECORD_WIDTH;
    let aligned_surrogate_bytes =
        (BULK_IO_BYTES / NODE_LOOKUP_RECORD_WIDTH) * NODE_LOOKUP_RECORD_WIDTH;
    let mut input_block = vec![0_u8; aligned_input_bytes];
    let mut surrogate_block = Vec::with_capacity(aligned_surrogate_bytes);
    work.peak_buffer_bytes = (input_block.len() + surrogate_block.capacity()) as u64;
    let mut previous_uuid = None;
    let mut previous_surrogate = 0_u64;
    let mut node_count = 0_u64;
    let mut edge_count = 0_u64;
    let mut source_digest = Sha256::new();
    let mut remaining = input_len;
    let streamed = (|| -> Result<(), GfError> {
        while remaining != 0 {
            if cancelled() {
                return Err(storage_err("construction index encoding cancelled"));
            }
            let count =
                usize::try_from(remaining.min(input_block.len() as u64)).map_err(storage_err)?;
            input
                .read_exact(&mut input_block[..count])
                .map_err(storage_err)?;
            source_digest.update(&input_block[..count]);
            work.read_bytes = work.read_bytes.saturating_add(count as u64);
            work.read_operations = work.read_operations.saturating_add(1);
            for record in input_block[..count].chunks_exact_mut(IDENTITY_RECORD_WIDTH) {
                let uuid: [u8; 16] = record[..16].try_into().expect("fixed UUID");
                if previous_uuid.is_some_and(|prior| prior >= uuid)
                    || record[17..24].iter().any(|byte| *byte != 0)
                {
                    return Err(storage_err("construction identity stream is not canonical"));
                }
                previous_uuid = Some(uuid);
                match record[16] {
                    0 => {
                        let surrogate =
                            u64::from_be_bytes(record[24..32].try_into().expect("fixed"));
                        if surrogate == 0 || surrogate <= previous_surrogate {
                            return Err(storage_err(
                                "construction node surrogate stream is not increasing",
                            ));
                        }
                        previous_surrogate = surrogate;
                        if surrogate_block.len() + NODE_LOOKUP_RECORD_WIDTH
                            > aligned_surrogate_bytes
                        {
                            surrogate_writer
                                .write_all(&surrogate_block)
                                .map_err(storage_err)?;
                            work.write_bytes = work
                                .write_bytes
                                .saturating_add(surrogate_block.len() as u64);
                            work.write_operations = work.write_operations.saturating_add(1);
                            surrogate_block.clear();
                        }
                        surrogate_block.extend_from_slice(&surrogate.to_be_bytes());
                        surrogate_block.extend_from_slice(&uuid);
                        node_count = node_count.saturating_add(1);
                    }
                    1 => {
                        // Construction assigns edge surrogates for topology; v3
                        // edge membership deliberately stores zero.
                        record[24..32].fill(0);
                        edge_count = edge_count.saturating_add(1);
                    }
                    _ => return Err(storage_err("construction identity kind is invalid")),
                }
            }
            identity_writer
                .write_all(&input_block[..count])
                .map_err(storage_err)?;
            work.write_bytes = work.write_bytes.saturating_add(count as u64);
            work.write_operations = work.write_operations.saturating_add(1);
            remaining -= count as u64;
        }
        if hex_bytes(&source_digest.finalize()) != identities_sha256 {
            return Err(storage_err("construction identity source digest changed"));
        }
        Ok(())
    })();
    let released = input.finish().map_err(storage_err);
    let input_cache_release = match (streamed, released) {
        (Ok(()), Ok(released)) => released,
        (Ok(()), Err(release)) => return Err(release),
        (Err(primary), Ok(_)) => return Err(primary),
        (Err(primary), Err(release)) => {
            return Err(storage_err(format!(
                "{primary}; construction identity cache release also failed: {release}"
            )));
        }
    };
    merge_cache_release_evidence(&mut work.cache_release, input_cache_release);
    if !surrogate_block.is_empty() {
        surrogate_writer
            .write_all(&surrogate_block)
            .map_err(storage_err)?;
        work.write_bytes = work
            .write_bytes
            .saturating_add(surrogate_block.len() as u64);
        work.write_operations = work.write_operations.saturating_add(1);
    }
    if manifest.live_node_count.saturating_add(node_count) != live_nodes
        || manifest.live_edge_count.saturating_add(edge_count) != live_edges
    {
        return Err(storage_err(
            "construction UUID delta counts differ from shaped counts",
        ));
    }
    identity_writer.flush().map_err(storage_err)?;
    surrogate_writer.flush().map_err(storage_err)?;
    identity_writer
        .sync_all_and_release()
        .map_err(storage_err)?;
    surrogate_writer
        .sync_all_and_release()
        .map_err(storage_err)?;
    let identity_cache_release = identity_writer.evidence();
    let surrogate_cache_release = surrogate_writer.evidence();
    work.fsync_operations = work
        .fsync_operations
        .saturating_add(identity_cache_release.sync_operations)
        .saturating_add(surrogate_cache_release.sync_operations);
    merge_cache_release_evidence(&mut work.cache_release, identity_cache_release);
    merge_cache_release_evidence(&mut work.cache_release, surrogate_cache_release);
    let aggregate_peak = input_cache_release
        .peak_window_bytes
        .checked_add(identity_cache_release.peak_window_bytes)
        .and_then(|peak| peak.checked_add(surrogate_cache_release.peak_window_bytes))
        .ok_or_else(|| storage_err("construction index aggregate cache window overflow"))?;
    work.cache_release.peak_window_bytes = work.cache_release.peak_window_bytes.max(aggregate_peak);
    drop(identity_writer.into_file());
    drop(surrogate_writer.into_file());

    let mut artifacts = Vec::new();
    let identity_record = describe_and_install_construction_run(
        &index,
        &identity_temp,
        identity_identity,
        "identities-v3",
        generation,
        IDENTITY_RECORD_WIDTH,
        &mut artifacts,
        &mut work,
    )?;
    let surrogate_record = describe_and_install_construction_run(
        &index,
        &surrogate_temp,
        surrogate_identity,
        "node-surrogates-v3",
        generation,
        NODE_LOOKUP_RECORD_WIDTH,
        &mut artifacts,
        &mut work,
    )?;
    let mut output_names = artifacts
        .iter()
        .map(|artifact| artifact.name.clone())
        .collect::<BTreeSet<_>>();
    crate::graph_construction::construction_failpoint("uuid_encode.after_delta_runs");

    if parent_generation == 0 {
        let base_identity = install_empty_construction_run(
            &index,
            "identities-v3-base",
            0,
            IDENTITY_RECORD_WIDTH,
            &mut artifacts,
            &mut work,
        )?;
        let base_surrogate = install_empty_construction_run(
            &index,
            "node-surrogates-v3-base",
            0,
            NODE_LOOKUP_RECORD_WIDTH,
            &mut artifacts,
            &mut work,
        )?;
        output_names.insert(base_identity.name.clone());
        output_names.insert(base_surrogate.name.clone());
        manifest.runs.push(RunRecord {
            base: true,
            level: 0,
            first_generation: 0,
            last_generation: 0,
            identities: base_identity,
            node_surrogates: base_surrogate,
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
        node_count,
        edge_count,
        deleted_node_count: 0,
        deleted_edge_count: 0,
    });

    let mut retained_payload_bytes = 0_u64;
    compact_construction_levels(
        &index,
        parent,
        generation,
        &mut manifest,
        &mut artifacts,
        &mut output_names,
        &mut retained_payload_bytes,
        &mut work,
        cancelled,
    )?;
    work.retained_payload_bytes = retained_payload_bytes;
    manifest.current_generation = generation;
    manifest.live_node_count = live_nodes;
    manifest.live_edge_count = live_edges;
    manifest
        .runs
        .sort_unstable_by_key(|run| run.first_generation);
    validate_run_descriptors(&manifest)?;

    let body = serde_json::to_vec(&manifest).map_err(storage_err)?;
    let (manifest_output, manifest_publication) =
        install_construction_bytes(&index, MANIFEST, &body, &mut work)?;
    crate::graph_construction::construction_failpoint("uuid_encode.after_manifest");
    artifacts.push(manifest_output);
    let retained_names = manifest_file_names(&manifest);
    let created_payload_bytes = artifacts
        .iter()
        .filter(|artifact| artifact.name != MANIFEST)
        .map(|artifact| artifact.bytes)
        .sum::<u64>();
    work.created_runs = artifacts
        .iter()
        .filter(|artifact| artifact.name != MANIFEST)
        .count() as u64;
    // All byte and operation counters above are updated at the actual read,
    // write, flush, and durability sites. Never reconstruct I/O from file
    // lengths here: short reads and discarded carry outputs are observable.
    work.peak_buffer_bytes = work.peak_buffer_bytes.max((3 * BULK_IO_BYTES) as u64);
    work.peak_temporary_bytes = created_payload_bytes.saturating_add(body.len() as u64);
    for artifact in artifacts
        .iter()
        .filter(|artifact| artifact.name != MANIFEST && !retained_names.contains(&artifact.name))
    {
        let file = index
            .open_child_file(std::ffi::OsStr::new(&artifact.name))
            .map_err(storage_err)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        index
            .unlink_child_if_identity(std::ffi::OsStr::new(&artifact.name), identity)
            .map_err(storage_err)?;
    }
    artifacts
        .retain(|artifact| artifact.name == MANIFEST || retained_names.contains(&artifact.name));
    artifacts.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let final_write_bytes = artifacts.iter().map(|artifact| artifact.bytes).sum();
    let mut retained_references = Vec::new();
    let locally_owned = artifacts
        .iter()
        .map(|artifact| artifact.name.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(parent) = parent {
        let mut referenced = BTreeSet::new();
        for record in manifest
            .runs
            .iter()
            .flat_map(|run| [&run.identities, &run.node_surrogates])
        {
            if !locally_owned.contains(record.name.as_str())
                && referenced.insert(record.name.clone())
            {
                retained_references.push(parent.retained_reference(record)?);
            }
        }
        work.retained_runs = manifest
            .runs
            .iter()
            .filter(|run| {
                !locally_owned.contains(run.identities.name.as_str())
                    && !locally_owned.contains(run.node_surrogates.name.as_str())
            })
            .count() as u64;
        parent.revalidate()?;
    }
    retained_references.sort_unstable_by(|left, right| left.target_path.cmp(&right.target_path));
    let intent_file = index
        .open_child_file(std::ffi::OsStr::new(CONSTRUCTION_INTENT))
        .map_err(storage_err)?;
    let intent_identity =
        graphforge_filesystem::file_identity(&intent_file).map_err(storage_err)?;
    index
        .unlink_child_if_identity(std::ffi::OsStr::new(CONSTRUCTION_INTENT), intent_identity)
        .map_err(storage_err)?;
    crate::graph_construction::construction_failpoint("uuid_encode.after_intent_removal");
    index.sync().map_err(storage_err)?;
    topology.sync().map_err(storage_err)?;
    graph.sync().map_err(storage_err)?;
    encoded.sync().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(4);
    commit_v4_publications(
        vec![(MANIFEST.to_owned(), manifest_publication)],
        V4AuthorityTransactionProof,
    )?;
    Ok(ConstructionIndexEncoding {
        artifacts,
        retained_references,
        input_records: node_count.saturating_add(edge_count),
        read_bytes: work.read_bytes,
        read_operations: work.read_operations,
        final_write_bytes,
        write_bytes: work.write_bytes,
        write_operations: work.write_operations,
        fsync_operations: work.fsync_operations,
        created_runs: work.created_runs,
        retained_runs: work.retained_runs,
        retained_payload_bytes: work.retained_payload_bytes,
        peak_buffer_bytes: work.peak_buffer_bytes,
        peak_temporary_bytes: work.peak_temporary_bytes,
        cache_release: work.cache_release,
    })
}

fn cleanup_private_construction_index(
    encoded: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    let graph = match encoded.open_child_directory(std::ffi::OsStr::new("graph")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    let topology = match graph.open_child_directory(std::ffi::OsStr::new("topology")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    let index = match topology.open_child_directory(std::ffi::OsStr::new("uuid-membership")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    match index.open_child_file(std::ffi::OsStr::new(CONSTRUCTION_INTENT)) {
        Ok(mut file) => {
            if file.metadata().map_err(storage_err)?.len() > 16 * 1024 {
                return Err(storage_err("construction recovery intent is oversized"));
            }
            let mut body = Vec::new();
            file.read_to_end(&mut body).map_err(storage_err)?;
            let intent: ConstructionRecoveryIntent =
                serde_json::from_slice(&body).map_err(storage_err)?;
            intent.authenticate()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage_err(error)),
    }
    let names = index.child_names().map_err(storage_err)?;
    let v4_allowed = authenticate_private_v4_residue(&index, &names)?;
    for name in names {
        let name_text = name
            .to_str()
            .ok_or_else(|| storage_err("construction recovery inventory name is not UTF-8"))?;
        if name_text != CONSTRUCTION_INTENT
            && name_text != MANIFEST
            && !name_text.starts_with(".construction-")
            && !name_text.starts_with(".manifest.json-")
            && !name_text.starts_with("identities-v3")
            && !name_text.starts_with("node-surrogates-v3")
            && !v4_allowed.contains(name_text)
        {
            return Err(storage_err(
                "construction recovery inventory contains an unauthorised object",
            ));
        }
        let file = index.open_child_file(&name).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "private construction index artifact has extra links",
            ));
        }
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        index
            .unlink_child_if_identity(&name, identity)
            .map_err(storage_err)?;
    }
    index.sync().map_err(storage_err)?;
    topology.sync().map_err(storage_err)?;
    graph.sync().map_err(storage_err)?;
    encoded.sync().map_err(storage_err)
}

fn authenticate_private_v4_residue(
    index: &graphforge_filesystem::StableDirectory,
    names: &[std::ffi::OsString],
) -> Result<BTreeSet<String>, GfError> {
    let text = names
        .iter()
        .map(|name| {
            name.to_str()
                .map(str::to_owned)
                .ok_or_else(|| storage_err("construction recovery inventory name is not UTF-8"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let receipt_present = text.contains(V4_ORDINAL_RECEIPT);
    let manifest_present = text.contains(V4_ORDINAL_MANIFEST);
    let lock_present = text.contains("ordinal-v4.lock");
    let mut allowed = BTreeSet::new();

    if manifest_present && !receipt_present || lock_present && !manifest_present {
        return Err(storage_err(
            "private v4 construction controls are not an install-order prefix",
        ));
    }
    authenticate_private_v4_control_temp(
        index,
        &text,
        receipt_present,
        manifest_present,
        lock_present,
        &mut allowed,
    )?;
    if receipt_present {
        let receipt_body =
            read_private_construction_child(index, V4_ORDINAL_RECEIPT, MAX_MANIFEST_BYTES)?;
        let receipt: TopologyIndexReceipt =
            serde_json::from_slice(&receipt_body).map_err(storage_err)?;
        if !canonical_lower_hex(&receipt.nonce, 32)
            || !canonical_lower_hex(&receipt.topology_delta_sha256, 64)
            || !canonical_lower_hex(&receipt.manifest_sha256, 64)
        {
            return Err(storage_err(
                "private v4 construction receipt is noncanonical",
            ));
        }
        allowed.insert(V4_ORDINAL_RECEIPT.to_owned());
        if !manifest_present {
            return authenticate_private_v4_artifact_residue(index, &text, allowed);
        }
        let manifest_body = read_private_construction_child(
            index,
            V4_ORDINAL_MANIFEST,
            crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
        )?;
        if hex_sha256(&manifest_body) != receipt.manifest_sha256 {
            return Err(storage_err(
                "private v4 construction manifest does not match its receipt",
            ));
        }
        let manifest: crate::V4OrdinalIdentityManifest =
            serde_json::from_slice(&manifest_body).map_err(storage_err)?;
        if manifest.topology_generation != receipt.expected_generation {
            return Err(storage_err(
                "private v4 construction generation does not match its receipt",
            ));
        }
        admit_v4_construction_manifest(&manifest)?;
        allowed.insert(V4_ORDINAL_MANIFEST.to_owned());
        if lock_present {
            let lock = index
                .open_child_file(std::ffi::OsStr::new("ordinal-v4.lock"))
                .map_err(storage_err)?;
            if lock.metadata().map_err(storage_err)?.len() != 0 {
                return Err(storage_err("private v4 construction lock is nonempty"));
            }
            allowed.insert("ordinal-v4.lock".to_owned());
        }
        for artifact in manifest
            .forward_identities
            .iter()
            .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
            .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        {
            authenticate_private_v4_artifact(index, artifact)?;
            if !allowed.insert(artifact.name.clone()) {
                return Err(storage_err(
                    "private v4 construction manifest repeats an artifact",
                ));
            }
        }
    }

    authenticate_private_v4_artifact_residue(index, &text, allowed)
}

fn authenticate_private_v4_control_temp(
    index: &graphforge_filesystem::StableDirectory,
    text: &BTreeSet<String>,
    receipt_present: bool,
    manifest_present: bool,
    lock_present: bool,
    allowed: &mut BTreeSet<String>,
) -> Result<(), GfError> {
    let temporaries = text
        .iter()
        .filter_map(|name| {
            [V4_ORDINAL_RECEIPT, V4_ORDINAL_MANIFEST, "ordinal-v4.lock"]
                .into_iter()
                .find_map(|control| {
                    name.strip_prefix(&format!(".{control}-"))
                        .and_then(|suffix| suffix.strip_suffix(".tmp"))
                        .filter(|nonce| canonical_lower_hex(nonce, 32))
                        .map(|_| (name, control))
                })
        })
        .collect::<Vec<_>>();
    if temporaries.len() > 1 {
        return Err(storage_err(
            "private v4 construction has multiple control temporaries",
        ));
    }
    let Some((name, control)) = temporaries.first().copied() else {
        return Ok(());
    };
    match control {
        V4_ORDINAL_RECEIPT if !receipt_present && !manifest_present && !lock_present => {
            let body = read_private_construction_child(index, name, MAX_MANIFEST_BYTES)?;
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&body).map_err(storage_err)?;
            if !canonical_lower_hex(&receipt.nonce, 32)
                || !canonical_lower_hex(&receipt.topology_delta_sha256, 64)
                || !canonical_lower_hex(&receipt.manifest_sha256, 64)
            {
                return Err(storage_err("private v4 receipt temporary is noncanonical"));
            }
        }
        V4_ORDINAL_MANIFEST if receipt_present && !manifest_present && !lock_present => {
            let receipt_body =
                read_private_construction_child(index, V4_ORDINAL_RECEIPT, MAX_MANIFEST_BYTES)?;
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&receipt_body).map_err(storage_err)?;
            let body = read_private_construction_child(
                index,
                name,
                crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
            )?;
            let manifest: crate::V4OrdinalIdentityManifest =
                serde_json::from_slice(&body).map_err(storage_err)?;
            if hex_sha256(&body) != receipt.manifest_sha256
                || manifest.topology_generation != receipt.expected_generation
            {
                return Err(storage_err(
                    "private v4 manifest temporary is not receipt-bound",
                ));
            }
            admit_v4_construction_manifest(&manifest)?;
        }
        "ordinal-v4.lock" if receipt_present && manifest_present && !lock_present => {
            let body = read_private_construction_child(index, name, 0)?;
            if !body.is_empty() {
                return Err(storage_err("private v4 lock temporary is nonempty"));
            }
        }
        _ => {
            return Err(storage_err(
                "private v4 control temporary is outside its install boundary",
            ));
        }
    }
    allowed.insert(name.clone());
    Ok(())
}

fn authenticate_private_v4_artifact_residue(
    index: &graphforge_filesystem::StableDirectory,
    text: &BTreeSet<String>,
    mut allowed: BTreeSet<String>,
) -> Result<BTreeSet<String>, GfError> {
    for name in text {
        if allowed.contains(name) || !is_exact_private_v4_name(name) {
            continue;
        }
        if name.starts_with(".v4-") {
            allowed.insert(name.clone());
            continue;
        }
        let file = index
            .open_child_file(std::ffi::OsStr::new(name))
            .map_err(storage_err)?;
        let length = file.metadata().map_err(storage_err)?.len();
        let digest = sha256_reader_streaming(file)?;
        let suffix = name
            .strip_suffix(".uuidx")
            .and_then(|name| name.rsplit_once('-'))
            .map(|(_, digest)| digest)
            .ok_or_else(|| storage_err("private v4 artifact name is malformed"))?;
        if length == 0 && !name.starts_with("tombstones-v4-") || !digest.starts_with(suffix) {
            return Err(storage_err(
                "private v4 construction artifact fails content authentication",
            ));
        }
        allowed.insert(name.clone());
    }
    Ok(allowed)
}

fn read_private_construction_child(
    index: &graphforge_filesystem::StableDirectory,
    name: &str,
    maximum: u64,
) -> Result<Vec<u8>, GfError> {
    let mut file = index
        .open_child_file(std::ffi::OsStr::new(name))
        .map_err(storage_err)?;
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
        return Err(storage_err(
            "private v4 construction control has extra links",
        ));
    }
    read_bounded(&mut file, maximum)
}

fn authenticate_private_v4_artifact(
    index: &graphforge_filesystem::StableDirectory,
    artifact: &crate::V4OrdinalArtifact,
) -> Result<(), GfError> {
    if !is_exact_private_v4_name(&artifact.name) || artifact.name.starts_with(".v4-") {
        return Err(storage_err("private v4 artifact name is noncanonical"));
    }
    let file = index
        .open_child_file(std::ffi::OsStr::new(&artifact.name))
        .map_err(storage_err)?;
    authenticate_private_v4_artifact_file(file, artifact)
}

fn authenticate_private_v4_artifact_file(
    file: File,
    artifact: &crate::V4OrdinalArtifact,
) -> Result<(), GfError> {
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1
        || file.metadata().map_err(storage_err)?.len() != artifact.bytes
        || sha256_reader_streaming(file)? != artifact.sha256
    {
        return Err(storage_err("private v4 artifact authentication failed"));
    }
    Ok(())
}

pub(crate) fn is_exact_private_v4_name(name: &str) -> bool {
    if let Some(rest) = name.strip_prefix(".v4-") {
        let Some((role, nonce)) = rest.strip_suffix(".tmp").and_then(|v| v.rsplit_once('-')) else {
            return false;
        };
        let role_ok = role == "forward"
            || role == "tombstones"
            || role.strip_prefix("ordinal-").is_some_and(|ordinal| {
                ordinal.len() == 8 && ordinal.bytes().all(|b| b.is_ascii_digit())
            });
        return role_ok && canonical_lower_hex(nonce, 32);
    }
    let prefix = if name.starts_with("forward-v4-") {
        "forward-v4-"
    } else if name.starts_with("ordinal-v4-") {
        "ordinal-v4-"
    } else if name.starts_with("tombstones-v4-") {
        "tombstones-v4-"
    } else {
        return false;
    };
    let Some((generation, digest)) = name
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(".uuidx"))
        .and_then(|value| value.rsplit_once('-'))
    else {
        return false;
    };
    generation
        .parse::<u64>()
        .is_ok_and(|value| value != 0 && value.to_string() == generation)
        && canonical_lower_hex(digest, 16)
}

fn canonical_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_reader_streaming(file: File) -> Result<String, GfError> {
    let mut reader =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage_err)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let hashed = (|| -> Result<String, GfError> {
        loop {
            let read = reader.read(&mut buffer).map_err(storage_err)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(hex_bytes(&digest.finalize()))
    })();
    let released = reader.finish().map_err(storage_err);
    match (hashed, released) {
        (Ok(digest), Ok(_)) => Ok(digest),
        (Ok(_), Err(error)) => Err(error),
        (Err(primary), Ok(_)) => Err(primary),
        (Err(primary), Err(release)) => Err(storage_err(format!(
            "{primary}; private v4 cache release also failed: {release}"
        ))),
    }
}

fn write_construction_intent(
    index: &graphforge_filesystem::StableDirectory,
    intent: &ConstructionRecoveryIntent,
    work: &mut ConstructionIndexWork,
) -> Result<(), GfError> {
    intent.authenticate()?;
    let body = serde_json::to_vec(intent).map_err(storage_err)?;
    let temporary = format!(".construction-intent-{}.tmp", Uuid::new_v4().simple());
    let mut file = index
        .create_replaceable_child_file(std::ffi::OsStr::new(&temporary))
        .map_err(storage_err)?;
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
    file.write_all(&body).map_err(storage_err)?;
    work.write_bytes = work.write_bytes.saturating_add(body.len() as u64);
    work.write_operations = work.write_operations.saturating_add(1);
    file.sync_all().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    drop(file);
    index
        .replace_child(
            std::ffi::OsStr::new(&temporary),
            identity,
            std::ffi::OsStr::new(CONSTRUCTION_INTENT),
        )
        .map_err(storage_err)?;
    index.sync().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compact_construction_levels(
    output: &graphforge_filesystem::StableDirectory,
    parent: Option<&AuthenticatedUuidIndexSnapshot>,
    generation: u64,
    manifest: &mut Manifest,
    artifacts: &mut Vec<ConstructionIndexOutput>,
    output_names: &mut BTreeSet<String>,
    retained_payload_bytes: &mut u64,
    work: &mut ConstructionIndexWork,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), GfError> {
    for level in 0_u8..=63 {
        loop {
            if cancelled() {
                return Err(storage_err("construction index encoding cancelled"));
            }
            let mut indexes = manifest
                .runs
                .iter()
                .enumerate()
                .filter_map(|(index, run)| (!run.base && run.level == level).then_some(index))
                .collect::<Vec<_>>();
            if indexes.len() < 2 {
                break;
            }
            if indexes.len() != 2 {
                return Err(storage_err("construction manifest level overflow"));
            }
            indexes.sort_unstable_by_key(|index| manifest.runs[*index].first_generation);
            let right = manifest.runs.remove(indexes[1]);
            let left = manifest.runs.remove(indexes[0]);
            if left.last_generation.saturating_add(1) != right.first_generation {
                return Err(storage_err(
                    "construction index intervals are discontinuous",
                ));
            }
            let identities = merge_construction_records(
                output,
                parent,
                &left.identities,
                &right.identities,
                output_names,
                &format!("identities-v3-l{}", level + 1),
                generation,
                IDENTITY_RECORD_WIDTH,
                artifacts,
                retained_payload_bytes,
                work,
                cancelled,
            )?;
            let surrogates = merge_construction_records(
                output,
                parent,
                &left.node_surrogates,
                &right.node_surrogates,
                output_names,
                &format!("node-surrogates-v3-l{}", level + 1),
                generation,
                NODE_LOOKUP_RECORD_WIDTH,
                artifacts,
                retained_payload_bytes,
                work,
                cancelled,
            )?;
            output_names.insert(identities.name.clone());
            output_names.insert(surrogates.name.clone());
            manifest.runs.push(RunRecord {
                base: false,
                level: level + 1,
                first_generation: left.first_generation,
                last_generation: right.last_generation,
                identities,
                node_surrogates: surrogates,
                node_count: left.node_count.saturating_add(right.node_count),
                edge_count: left.edge_count.saturating_add(right.edge_count),
                deleted_node_count: left
                    .deleted_node_count
                    .saturating_add(right.deleted_node_count),
                deleted_edge_count: left
                    .deleted_edge_count
                    .saturating_add(right.deleted_edge_count),
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Streamed merge keeps both reader cleanups coupled to the primary result.
fn merge_construction_records(
    output: &graphforge_filesystem::StableDirectory,
    parent: Option<&AuthenticatedUuidIndexSnapshot>,
    left: &FileRecord,
    right: &FileRecord,
    output_names: &BTreeSet<String>,
    prefix: &str,
    generation: u64,
    width: usize,
    artifacts: &mut Vec<ConstructionIndexOutput>,
    retained_payload_bytes: &mut u64,
    work: &mut ConstructionIndexWork,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<FileRecord, GfError> {
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(3).map_err(storage_err)?;
    let mut left_reader = ConstructionBlockCursor::new(
        open_construction_source(output, parent, left, output_names, retained_payload_bytes)?,
        left.clone(),
        width,
        cache_window,
    )?;
    let right_source =
        open_construction_source(output, parent, right, output_names, retained_payload_bytes);
    let right_source = match right_source {
        Ok(source) => source,
        Err(primary) => {
            return combine_cache_cleanup(
                Err(primary),
                left_reader.finish_cache(),
                "left construction merge source",
            );
        }
    };
    let right_reader =
        ConstructionBlockCursor::new(right_source, right.clone(), width, cache_window);
    let mut right_reader = match right_reader {
        Ok(reader) => reader,
        Err(primary) => {
            return combine_cache_cleanup(
                Err(primary),
                left_reader.finish_cache(),
                "left construction merge source",
            );
        }
    };
    let temporary = format!(".construction-merge-{}.tmp", Uuid::new_v4().simple());
    let file = output
        .create_replaceable_child_file(std::ffi::OsStr::new(&temporary))
        .map_err(storage_err);
    let file = match file {
        Ok(file) => file,
        Err(primary) => {
            return finish_construction_cursor_pair(primary, &mut left_reader, &mut right_reader);
        }
    };
    let identity = match graphforge_filesystem::file_identity(&file).map_err(storage_err) {
        Ok(identity) => identity,
        Err(primary) => {
            return finish_construction_cursor_pair(primary, &mut left_reader, &mut right_reader);
        }
    };
    let mut writer =
        match graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(file, cache_window)
            .map_err(storage_err)
        {
            Ok(writer) => writer,
            Err(primary) => {
                return finish_construction_cursor_pair(
                    primary,
                    &mut left_reader,
                    &mut right_reader,
                );
            }
        };
    let key_width = if width == IDENTITY_RECORD_WIDTH {
        16
    } else {
        8
    };
    let output_bytes = (BULK_IO_BYTES / width) * width;
    let mut output_block = Vec::with_capacity(output_bytes);
    let merged = (|| -> Result<(), GfError> {
        while left_reader.current().is_some() || right_reader.current().is_some() {
            let take_left = match (left_reader.current(), right_reader.current()) {
                (Some(left), Some(right)) => {
                    if left[..key_width] == right[..key_width] {
                        return Err(storage_err("construction index merge found duplicate key"));
                    }
                    left[..key_width] < right[..key_width]
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let selected = if take_left {
                left_reader.current().expect("left exists")
            } else {
                right_reader.current().expect("right exists")
            };
            output_block.extend_from_slice(selected);
            if take_left {
                left_reader.advance()?;
            } else {
                right_reader.advance()?;
            }
            if output_block.len() + width > output_bytes {
                if cancelled() {
                    return Err(storage_err("construction index encoding cancelled"));
                }
                writer.write_all(&output_block).map_err(storage_err)?;
                work.write_bytes = work.write_bytes.saturating_add(output_block.len() as u64);
                work.write_operations = work.write_operations.saturating_add(1);
                output_block.clear();
            }
        }
        if !output_block.is_empty() {
            writer.write_all(&output_block).map_err(storage_err)?;
            work.write_bytes = work.write_bytes.saturating_add(output_block.len() as u64);
            work.write_operations = work.write_operations.saturating_add(1);
        }
        writer.flush().map_err(storage_err)
    })();
    let merged = combine_cache_cleanup(
        merged,
        left_reader.finish_cache(),
        "left construction merge source",
    );
    let merged = combine_cache_cleanup(
        merged,
        right_reader.finish_cache(),
        "right construction merge source",
    );
    merged?;
    writer.sync_all_and_release().map_err(storage_err)?;
    let write_cache_release = writer.evidence();
    work.fsync_operations = work
        .fsync_operations
        .saturating_add(write_cache_release.sync_operations);
    merge_cache_release_evidence(&mut work.cache_release, write_cache_release);
    work.read_bytes = work
        .read_bytes
        .saturating_add(left_reader.read_bytes)
        .saturating_add(right_reader.read_bytes);
    work.read_operations = work
        .read_operations
        .saturating_add(left_reader.read_operations)
        .saturating_add(right_reader.read_operations);
    merge_cache_release_evidence(&mut work.cache_release, left_reader.cache_release);
    merge_cache_release_evidence(&mut work.cache_release, right_reader.cache_release);
    drop(writer.into_file());
    describe_and_install_construction_run(
        output, &temporary, identity, prefix, generation, width, artifacts, work,
    )
}

struct ConstructionBlockCursor {
    file: graphforge_filesystem::FileCacheReleasingReader,
    descriptor: FileRecord,
    width: usize,
    block: Vec<u8>,
    block_index: usize,
    within: usize,
    records: u64,
    digest: Sha256,
    finished: bool,
    read_bytes: u64,
    read_operations: u64,
    cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
    cache_finished: bool,
}

impl ConstructionBlockCursor {
    fn new(
        file: File,
        descriptor: FileRecord,
        width: usize,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        let mut cursor = Self {
            file: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                file,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage_err)?,
            descriptor,
            width,
            block: Vec::new(),
            block_index: 0,
            within: 0,
            records: 0,
            digest: Sha256::new(),
            finished: false,
            read_bytes: 0,
            read_operations: 0,
            cache_release: graphforge_filesystem::FileCacheReleaseEvidence::default(),
            cache_finished: false,
        };
        if let Err(primary) = cursor.fill() {
            combine_cache_cleanup::<()>(
                Err(primary),
                cursor.finish_cache(),
                "construction merge source",
            )?;
            unreachable!("failed construction cursor initialization returned success");
        }
        Ok(cursor)
    }

    fn current(&self) -> Option<&[u8]> {
        (!self.finished).then(|| &self.block[self.within..self.within + self.width])
    }

    fn advance(&mut self) -> Result<(), GfError> {
        if self.finished {
            return Ok(());
        }
        self.within += self.width;
        self.records = self.records.saturating_add(1);
        if self.within == self.block.len() {
            self.fill()?;
        }
        Ok(())
    }

    fn fill(&mut self) -> Result<(), GfError> {
        if self.block_index == self.descriptor.blocks.len() {
            self.finished = true;
            let authenticated = if self.records != self.descriptor.count
                || hex_bytes(&self.digest.clone().finalize()) != self.descriptor.sha256
            {
                Err(storage_err(
                    "construction merge source authentication failed",
                ))
            } else {
                Ok(())
            };
            return combine_cache_cleanup(
                authenticated,
                self.finish_cache(),
                "construction merge source",
            );
        }
        let expected = &self.descriptor.blocks[self.block_index];
        if expected.offset != self.records.saturating_mul(self.width as u64)
            || expected.len == 0
            || !(expected.len as usize).is_multiple_of(self.width)
        {
            return Err(storage_err("construction merge block framing changed"));
        }
        self.block.resize(expected.len as usize, 0);
        self.file.read_exact(&mut self.block).map_err(storage_err)?;
        self.read_bytes = self.read_bytes.saturating_add(self.block.len() as u64);
        self.read_operations = self.read_operations.saturating_add(1);
        let key_width = if self.width == IDENTITY_RECORD_WIDTH {
            16
        } else {
            8
        };
        if hex_sha256(&self.block) != expected.sha256
            || hex_sha256_key(&self.block[..key_width]) != expected.first_key
            || hex_sha256_key(
                &self.block
                    [self.block.len() - self.width..self.block.len() - self.width + key_width],
            ) != expected.last_key
        {
            return Err(storage_err("construction merge source block changed"));
        }
        self.digest.update(&self.block);
        self.block_index += 1;
        self.within = 0;
        Ok(())
    }

    fn finish_cache(&mut self) -> Result<(), GfError> {
        if !self.cache_finished {
            self.cache_release = self.file.finish().map_err(storage_err)?;
            self.cache_finished = true;
        }
        Ok(())
    }
}

fn combine_cache_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    source: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage_err(format!(
            "{primary}; {source} cache release also failed: {cleanup}"
        ))),
    }
}

fn combine_v4_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    context: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage_err(format!(
            "{primary}; {context} also failed: {cleanup}"
        ))),
    }
}

fn finish_construction_cursor_pair<T>(
    primary: GfError,
    left: &mut ConstructionBlockCursor,
    right: &mut ConstructionBlockCursor,
) -> Result<T, GfError> {
    let result = combine_cache_cleanup(
        Err(primary),
        left.finish_cache(),
        "left construction merge source",
    );
    combine_cache_cleanup(
        result,
        right.finish_cache(),
        "right construction merge source",
    )
}

fn open_construction_source(
    output: &graphforge_filesystem::StableDirectory,
    parent: Option<&AuthenticatedUuidIndexSnapshot>,
    record: &FileRecord,
    output_names: &BTreeSet<String>,
    retained_payload_bytes: &mut u64,
) -> Result<File, GfError> {
    if output_names.contains(&record.name) {
        output
            .open_child_file(std::ffi::OsStr::new(&record.name))
            .map_err(storage_err)
    } else {
        let parent =
            parent.ok_or_else(|| storage_err("construction merge lacks retained source"))?;
        *retained_payload_bytes =
            retained_payload_bytes.saturating_add(record.count.saturating_mul(
                if record.name.starts_with("identities-") {
                    IDENTITY_RECORD_BYTES
                } else {
                    NODE_LOOKUP_RECORD_BYTES
                },
            ));
        parent.open_retained_file(record)
    }
}

fn install_empty_construction_run(
    output: &graphforge_filesystem::StableDirectory,
    prefix: &str,
    generation: u64,
    width: usize,
    artifacts: &mut Vec<ConstructionIndexOutput>,
    work: &mut ConstructionIndexWork,
) -> Result<FileRecord, GfError> {
    let temporary = format!(".construction-empty-{}.tmp", Uuid::new_v4().simple());
    let file = output
        .create_replaceable_child_file(std::ffi::OsStr::new(&temporary))
        .map_err(storage_err)?;
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
    file.sync_all().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    drop(file);
    describe_and_install_construction_run(
        output, &temporary, identity, prefix, generation, width, artifacts, work,
    )
}

#[allow(clippy::too_many_arguments)]
fn describe_and_install_construction_run(
    output: &graphforge_filesystem::StableDirectory,
    temporary: &str,
    identity: graphforge_filesystem::FileIdentity,
    prefix: &str,
    generation: u64,
    width: usize,
    artifacts: &mut Vec<ConstructionIndexOutput>,
    work: &mut ConstructionIndexWork,
) -> Result<FileRecord, GfError> {
    let file = output
        .open_child_file(std::ffi::OsStr::new(temporary))
        .map_err(storage_err)?;
    let bytes = file.metadata().map_err(storage_err)?.len();
    if bytes % width as u64 != 0 {
        return Err(storage_err("construction index run is truncated"));
    }
    let block_bytes = (BULK_IO_BYTES / width) * width;
    let key_width = if width == IDENTITY_RECORD_WIDTH {
        16
    } else {
        8
    };
    let mut file =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage_err)?;
    let mut digest = Sha256::new();
    let mut blocks = Vec::new();
    let mut offset = 0_u64;
    let mut buffer = vec![0_u8; block_bytes];
    let described = (|| -> Result<(), GfError> {
        loop {
            let mut filled = 0;
            while filled < buffer.len() {
                let read = file.read(&mut buffer[filled..]).map_err(storage_err)?;
                if read == 0 {
                    break;
                }
                filled += read;
                work.read_bytes = work.read_bytes.saturating_add(read as u64);
                work.read_operations = work.read_operations.saturating_add(1);
            }
            if filled == 0 {
                break;
            }
            if filled % width != 0 {
                return Err(storage_err("construction index block framing changed"));
            }
            let block = &buffer[..filled];
            digest.update(block);
            blocks.push(BlockRecord {
                offset,
                len: u32::try_from(filled).map_err(storage_err)?,
                first_key: hex_bytes(&block[..key_width]),
                last_key: hex_bytes(&block[filled - width..filled - width + key_width]),
                sha256: hex_sha256(block),
            });
            offset = offset.saturating_add(filled as u64);
            if filled < buffer.len() {
                break;
            }
        }
        Ok(())
    })();
    let released = file.finish().map_err(storage_err);
    let read_cache_release = match (described, released) {
        (Ok(()), Ok(released)) => released,
        (Ok(()), Err(release)) => return Err(release),
        (Err(primary), Ok(_)) => return Err(primary),
        (Err(primary), Err(release)) => {
            return Err(storage_err(format!(
                "{primary}; construction run cache release also failed: {release}"
            )));
        }
    };
    merge_cache_release_evidence(&mut work.cache_release, read_cache_release);
    let sha256 = hex_bytes(&digest.finalize());
    let name = format!("{prefix}-{generation}-{}.uuidx", &sha256[..16]);
    output
        .replace_child(
            std::ffi::OsStr::new(temporary),
            identity,
            std::ffi::OsStr::new(&name),
        )
        .map_err(storage_err)?;
    output.sync().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    artifacts.push(ConstructionIndexOutput {
        name: name.clone(),
        bytes,
        sha256: sha256.clone(),
    });
    Ok(FileRecord {
        name,
        count: bytes / width as u64,
        sha256,
        blocks,
    })
}

fn install_construction_bytes(
    output: &graphforge_filesystem::StableDirectory,
    name: &str,
    bytes: &[u8],
    work: &mut ConstructionIndexWork,
) -> Result<
    (
        ConstructionIndexOutput,
        graphforge_filesystem::UnpublishedArtifactGuard,
    ),
    GfError,
> {
    let temporary = format!(".{name}-{}.tmp", Uuid::new_v4().simple());
    let mut publication = output
        .create_unpublished_replaceable_child(std::ffi::OsStr::new(&temporary))
        .map_err(storage_err)?;
    let mut file = publication.take_file().map_err(storage_err)?;
    file.write_all(bytes).map_err(storage_err)?;
    work.write_bytes = work.write_bytes.saturating_add(bytes.len() as u64);
    work.write_operations = work.write_operations.saturating_add(1);
    work.peak_temporary_bytes = work
        .peak_temporary_bytes
        .max(u64::try_from(bytes.len()).map_err(storage_err)?);
    file.sync_all().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    let failpoint = match name {
        V4_ORDINAL_RECEIPT => Some("v4_publish.after_receipt_temp_fsync"),
        V4_ORDINAL_MANIFEST => Some("v4_publish.after_manifest_temp_fsync"),
        "ordinal-v4.lock" => Some("v4_publish.after_lock_temp_fsync"),
        _ => None,
    };
    if let Some(failpoint) = failpoint {
        crate::graph_construction::construction_failpoint(failpoint);
    }
    drop(file);
    let installed = publication
        .install_child(std::ffi::OsStr::new(name))
        .map_err(storage_err)
        .and_then(|()| publication.sync_parent().map_err(storage_err));
    if let Err(primary) = installed {
        let cleanup = cleanup_v4_publication(&mut publication);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 construction control cleanup");
    }
    let authority_point = match name {
        V4_ORDINAL_RECEIPT => Some("receipt_install"),
        V4_ORDINAL_MANIFEST => Some("manifest_install"),
        "ordinal-v4.lock" => Some("lock_install"),
        _ => None,
    };
    if let Some(point) = authority_point
        && let Err(primary) = v4_authority_failure(point)
    {
        let cleanup = cleanup_v4_publication(&mut publication);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 construction control cleanup");
    }
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    Ok((
        ConstructionIndexOutput {
            name: name.to_owned(),
            bytes: bytes.len() as u64,
            sha256: hex_sha256(bytes),
        },
        publication,
    ))
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
        let mut unresolved = unique;
        let mut membership = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                kind,
                &unresolved,
                &mut metrics,
            )?;
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                metrics.found = metrics.found.saturating_add(u64::from(state.present));
                membership.insert(uuid, state.present);
            }
        }
        Ok((
            requested
                .iter()
                .map(|uuid| membership.get(uuid).copied().unwrap_or(false))
                .collect(),
            metrics,
        ))
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
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                UuidIndexKind::Node,
                &unresolved,
                &mut metrics,
            )?;
            let mut pairs = Vec::new();
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                let value = state.present.then_some(state.surrogate);
                if let Some(surrogate) = value {
                    pairs.push((surrogate, uuid));
                    metrics.found = metrics.found.saturating_add(1);
                }
                resolved.insert(uuid, value);
            }
            validate_surrogate_pairs(
                &mut run.node_surrogates,
                &run.descriptor.node_surrogates,
                &pairs,
                &mut metrics,
            )?;
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().flatten())
                .collect(),
            metrics,
        ))
    }
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

/// Reclaim a bounded number of unreachable immutable UUID runs under the
/// recovered project rewrite lock.
pub fn maintain_uuid_membership_orphans(
    project_dir: &Path,
    maximum: usize,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    let selected = selected_generation_for_graph_root(project_dir)?;
    let ordinal_authority = selected
        .as_ref()
        .map(crate::ResolvedProjectGeneration::authenticated_v4_ordinal_authority)
        .transpose()?
        .flatten();
    let membership_authority = selected
        .as_ref()
        .map(authenticated_v3_membership_authority)
        .transpose()?
        .flatten();
    maintain_uuid_membership_orphans_with_authorities(
        project_dir,
        maximum,
        membership_authority.as_ref(),
        ordinal_authority.as_ref(),
    )
}

#[derive(Clone, Debug)]
struct AuthenticatedV3MembershipAuthority {
    topology_generation: u64,
    manifest_sha256: String,
}

fn authenticated_v3_membership_authority(
    selected: &crate::ResolvedProjectGeneration,
) -> Result<Option<AuthenticatedV3MembershipAuthority>, GfError> {
    let mut state = crate::graph_manifest::GraphManifestTargetedState::default();
    let receipt = selected.authenticated_graph_file_bytes_with_state(
        &format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
        MAX_MANIFEST_BYTES,
        Some(&mut state),
    )?;
    let manifest = selected.authenticated_graph_file_bytes_with_state(
        &format!("{INDEX_DIR}/{MANIFEST}"),
        MAX_MANIFEST_BYTES,
        Some(&mut state),
    )?;
    match (receipt, manifest) {
        (None, None) => Ok(None),
        (None, Some(_)) | (Some(_), None) => Err(storage_err(
            "selected UUID membership facet has incomplete authority residue",
        )),
        (Some((_, receipt_bytes)), Some((manifest_entry, manifest_bytes))) => {
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&receipt_bytes).map_err(storage_err)?;
            let generation = selected
                .authenticated_graph_file_bytes_with_state(
                    "topology/generation.json",
                    MAX_MANIFEST_BYTES,
                    Some(&mut state),
                )?
                .ok_or_else(|| storage_err("selected topology generation authority is absent"))?;
            let generation: serde_json::Value =
                serde_json::from_slice(&generation.1).map_err(storage_err)?;
            let topology_generation = generation
                .get("topology_generation")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| storage_err("selected topology generation is missing"))?;
            let manifest_sha256 = hex_sha256(&manifest_bytes);
            let canonical_hex = |value: &str, length: usize| {
                value.len() == length
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            };
            if !canonical_hex(&receipt.nonce, 32)
                || !canonical_hex(&receipt.topology_delta_sha256, 64)
                || !canonical_hex(&receipt.manifest_sha256, 64)
                || receipt.expected_generation != topology_generation
                || receipt.manifest_sha256 != manifest_entry.content_sha256
                || receipt.manifest_sha256 != manifest_sha256
            {
                return Err(storage_err(
                    "selected UUID membership receipt does not authenticate its manifest",
                ));
            }
            Ok(Some(AuthenticatedV3MembershipAuthority {
                topology_generation,
                manifest_sha256,
            }))
        }
    }
}

/// Retain the selected project generation whenever `graph_root` is its
/// generation-owned graph tree. Standalone graph roots deliberately have no
/// project-generation provenance and therefore cannot authorize a present v4
/// facet.
fn selected_generation_for_graph_root(
    graph_root: &Path,
) -> Result<Option<crate::ResolvedProjectGeneration>, GfError> {
    let Some(generation_root) = graph_root
        .file_name()
        .filter(|name| *name == std::ffi::OsStr::new("graph"))
        .and_then(|_| graph_root.parent())
    else {
        return Ok(None);
    };
    let Some(project_generations_dir) = generation_root.parent() else {
        return Ok(None);
    };
    if project_generations_dir.file_name() != Some(std::ffi::OsStr::new("generations")) {
        return Ok(None);
    }
    let Some(container_root) = project_generations_dir.parent() else {
        return Ok(None);
    };
    let selected = crate::resolve_project_generation(container_root)?;
    let supplied = graph_root.canonicalize().map_err(storage_err)?;
    let authenticated = selected
        .graph_tree_root()
        .canonicalize()
        .map_err(storage_err)?;
    if supplied != authenticated {
        return Err(storage_err(
            "graph root is not the currently selected project generation",
        ));
    }
    Ok(Some(selected))
}

/// Pin a standalone v4 facet through the admitted local receipt while the
/// caller is about to enter the one project rewrite critical section. Project
/// generation roots instead require their externally selected graph/files
/// authority and never use this local path.
fn standalone_v4_pinned_update(
    project_root: &Path,
    generation: u64,
) -> Result<Option<crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs>, GfError> {
    let index_path = project_root.join(INDEX_DIR);
    let index = match graphforge_filesystem::StableDirectory::open(&index_path) {
        Ok(index) => index,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let mut manifest_file = match index.open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_RECEIPT))
        .map_err(storage_err)?;
    let manifest = read_bounded(
        &mut manifest_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt = read_bounded(
        &mut receipt_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt).map_err(storage_err)?;
    let manifest_sha256 = hex_sha256(&manifest);
    if receipt.expected_generation != generation
        || receipt.manifest_sha256 != manifest_sha256
        || receipt.nonce.len() != 32
        || !receipt
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_err(
            "standalone v4 receipt does not authenticate current authority",
        ));
    }
    index.revalidate_named().map_err(storage_err)?;
    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
        topology_generation: generation,
        manifest_sha256,
    };
    match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
        project_root,
        &authority,
        crate::V4OrdinalIdentityLimits::default(),
    )
    .map_err(storage_err)?
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => {
            handle.pinned_update_inputs().map(Some).map_err(storage_err)
        }
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => Err(storage_err(
            "standalone v4 ordinal identity requires rebuild before mutation",
        )),
    }
}

#[cfg(test)]
pub(crate) fn maintain_uuid_membership_orphans_with_ordinal_authority(
    project_dir: &Path,
    maximum: usize,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    let manifest_bytes =
        fs::read(project_dir.join(INDEX_DIR).join(MANIFEST)).map_err(storage_err)?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(storage_err)?;
    let membership_authority = AuthenticatedV3MembershipAuthority {
        topology_generation: manifest.current_generation,
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    maintain_uuid_membership_orphans_with_authorities(
        project_dir,
        maximum,
        Some(&membership_authority),
        ordinal_authority,
    )
}

fn maintain_uuid_membership_orphans_with_authorities(
    project_dir: &Path,
    maximum: usize,
    membership_authority: Option<&AuthenticatedV3MembershipAuthority>,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    crate::durable_rewrite::with_rewrite_lock(project_dir, |project| {
        collect_uuid_orphans_locked(
            project,
            project_dir,
            maximum,
            membership_authority,
            ordinal_authority,
        )
    })
}

fn collect_uuid_orphans_locked(
    project: &graphforge_filesystem::StableDirectory,
    project_root: &Path,
    maximum: usize,
    membership_authority: Option<&AuthenticatedV3MembershipAuthority>,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    let topology = match project.open_child_directory(std::ffi::OsStr::new("topology")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UuidIndexOrphanGcWork::default());
        }
        Err(error) => return Err(storage_err(error)),
    };
    let index = match topology.open_child_directory(std::ffi::OsStr::new("uuid-membership")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UuidIndexOrphanGcWork::default());
        }
        Err(error) => return Err(storage_err(error)),
    };
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(MANIFEST))
        .map_err(storage_err)?;
    let manifest_bytes = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
    let membership_authority = membership_authority.ok_or_else(|| {
        storage_err("UUID membership orphan maintenance requires selected generation authority")
    })?;
    if hex_sha256(&manifest_bytes) != membership_authority.manifest_sha256 {
        return Err(storage_err(
            "UUID membership manifest differs from selected generation authority",
        ));
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(storage_err)?;
    if manifest.current_generation != membership_authority.topology_generation {
        return Err(storage_err(
            "UUID membership generation differs from selected generation authority",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let mut referenced = manifest_file_names(&manifest);
    referenced.extend(authenticated_v4_references(
        project_root,
        ordinal_authority,
    )?);
    let mut names = index.child_names().map_err(storage_err)?;
    names.sort();
    let mut work = UuidIndexOrphanGcWork::default();
    for name in names {
        let Some(text) = name.to_str() else { continue };
        if referenced.contains(text) || !is_canonical_run_name(text) {
            continue;
        }
        work.candidates = work.candidates.saturating_add(1);
        if usize::try_from(work.candidates).unwrap_or(usize::MAX) > maximum {
            work.deferred = work.deferred.saturating_add(1);
            work.deferred_limit = work.deferred_limit.saturating_add(1);
            continue;
        }
        let file = index.open_child_file(&name).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
            work.deferred = work.deferred.saturating_add(1);
            work.deferred_linked = work.deferred_linked.saturating_add(1);
            continue;
        }
        let bytes = file.metadata().map_err(storage_err)?.len();
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        if is_canonical_v4_artifact_prefix(
            text.strip_suffix(".uuidx")
                .and_then(|stem| stem.rsplit_once('-').map(|(prefix, _)| prefix))
                .unwrap_or_default(),
        ) {
            crate::project_failpoint::hit(
                "v4_cleanup.before_unlink",
                None,
                None,
                "V4_CLEANUP_BEFORE_UNLINK",
                false,
            )?;
        }
        index
            .unlink_child_if_identity(&name, identity)
            .map_err(storage_err)?;
        if text.starts_with("forward-v4-")
            || text.starts_with("ordinal-v4-")
            || text.starts_with("tombstones-v4-")
        {
            crate::project_failpoint::hit(
                "v4_cleanup.after_unlink",
                None,
                None,
                "V4_CLEANUP_AFTER_UNLINK",
                false,
            )?;
        }
        work.removed = work.removed.saturating_add(1);
        work.bytes = work.bytes.saturating_add(bytes);
    }
    if work.removed != 0 {
        index.sync().map_err(storage_err)?;
    }
    index.revalidate_named().map_err(storage_err)?;
    topology.revalidate_named().map_err(storage_err)?;
    project.revalidate_named().map_err(storage_err)?;
    Ok(work)
}

fn authenticated_v4_references(
    project_root: &Path,
    authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<BTreeSet<String>, GfError> {
    let Some(authority) = authority else {
        let index = graphforge_filesystem::StableDirectory::open(&project_root.join(INDEX_DIR))
            .map_err(storage_err)?;
        return match index.open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
            Ok(_) => Err(storage_err(
                "v4 ordinal authority requires an authenticated selected generation",
            )),
            Err(error) => Err(storage_err(error)),
        };
    };
    match authority
        .open(project_root, crate::V4OrdinalIdentityLimits::default())
        .map_err(storage_err)?
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => Ok(handle.referenced_file_names()),
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => {
            Err(storage_err("v4 ordinal authority requires rebuild"))
        }
    }
}

fn is_canonical_run_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".uuidx") else {
        return false;
    };
    let Some((prefix, digest)) = stem.rsplit_once('-') else {
        return false;
    };
    let is_v4 = is_canonical_v4_artifact_prefix(prefix);
    ((prefix.starts_with("identities-v3") || prefix.starts_with("node-surrogates-v3")) || is_v4)
        && digest.len() == 16
        && if is_v4 {
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        } else {
            digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }
}

fn is_canonical_v4_artifact_prefix(prefix: &str) -> bool {
    let Some((kind, generation)) = prefix.rsplit_once('-') else {
        return false;
    };
    matches!(kind, "forward-v4" | "ordinal-v4" | "tombstones-v4")
        && generation.parse::<u64>().is_ok_and(|value| value != 0)
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

/// Commit topology and its UUID participant under the one durable rewrite lock.
#[allow(clippy::too_many_lines)] // One sealed participant lifecycle; order is the invariant.
pub(crate) fn commit_uuid_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
    delta: &UuidTopologyDelta,
    snapshot: &mut Option<AuthenticatedUuidIndexSnapshot>,
) -> Result<CommittedUuidTopologyRewrite, GfError> {
    let delta_is_empty = delta.nodes.is_empty()
        && delta.edges.is_empty()
        && delta.deleted_nodes.is_empty()
        && delta.deleted_edges.is_empty();
    if delta_is_empty && staged.is_empty() {
        return Ok(CommittedUuidTopologyRewrite::NoTopologyChange);
    }
    ensure_uuid_membership_migrated(project_dir)?;
    let selected = selected_generation_for_graph_root(project_dir)?;
    let ordinal_authority = selected
        .as_ref()
        .map(crate::ResolvedProjectGeneration::authenticated_v4_ordinal_authority)
        .transpose()?
        .flatten();
    let ordinal_inputs = ordinal_authority
        .as_ref()
        .map(|authority| {
            match authority
                .open(project_dir, crate::V4OrdinalIdentityLimits::default())
                .map_err(storage_err)?
            {
                crate::V4OrdinalIdentityOpen::Ready(handle) => {
                    handle.pinned_update_inputs().map(Some).map_err(storage_err)
                }
                crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => Err(storage_err(
                    "v4 ordinal identity requires rebuild before mutation",
                )),
            }
        })
        .transpose()?
        .flatten();
    let ordinal_inputs = if ordinal_inputs.is_some() {
        ordinal_inputs
    } else if selected.is_none() {
        standalone_v4_pinned_update(project_dir, crate::read_topology_generation(project_dir)?)?
    } else {
        None
    };
    let membership_authority = selected
        .as_ref()
        .map(authenticated_v3_membership_authority)
        .transpose()?
        .flatten();
    let prepared = std::rc::Rc::new(std::cell::RefCell::new(None));
    let prepared_from_callback = std::rc::Rc::clone(&prepared);
    let prepared_v4 = std::rc::Rc::new(std::cell::RefCell::new(None));
    let prepared_v4_from_callback = std::rc::Rc::clone(&prepared_v4);
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
            if context.next.topology == context.prior.topology {
                if !delta_is_empty {
                    return Err(storage_err(
                        "UUID identity delta did not stage a topology transition",
                    ));
                }
                return Ok(None);
            }
            // Standalone graph roots have no selected project-generation
            // inventory capable of authenticating reachability. Topology
            // publication remains valid there, but orphan deletion must be
            // conservatively deferred rather than self-authorizing the live
            // manifest. Project-generation roots retain the authenticated GC
            // path, including the v3/v4 union.
            let orphan_gc = if membership_authority.is_some() {
                collect_uuid_orphans_locked(
                    context.project,
                    context.project_root,
                    DEFAULT_ORPHAN_GC_LIMIT,
                    membership_authority.as_ref(),
                    ordinal_authority.as_ref(),
                )?
            } else {
                UuidIndexOrphanGcWork::default()
            };
            if !uuid_membership_index_present(context.project_root) {
                return Err(storage_err(
                    "UUID membership index migration is required before topology mutation",
                ));
            }
            if snapshot
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
            let mut token = prepare_uuid_membership_delta(
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
            if let Some(token) = token.as_mut() {
                token.metrics.orphan_gc_candidates = orphan_gc.candidates;
                token.metrics.orphan_gc_removed = orphan_gc.removed;
                token.metrics.orphan_gc_deferred = orphan_gc.deferred;
                token.metrics.orphan_gc_deferred_limit = orphan_gc.deferred_limit;
                token.metrics.orphan_gc_deferred_linked = orphan_gc.deferred_linked;
                token.metrics.orphan_gc_bytes = orphan_gc.bytes;
            }
            let receipt = token
                .as_ref()
                .map(PreparedUuidIndexDelta::auxiliary_receipt);
            let mut prepared_ordinal = ordinal_inputs
                .as_ref()
                .map(|pinned| {
                    let deleted_node_ids = deleted_nodes
                        .iter()
                        .map(|(_, node_id)| *node_id)
                        .collect::<Vec<_>>();
                    prepare_v4_ordinal_delta(
                        context.project_root,
                        context.prior.topology,
                        context.next.topology,
                        pinned,
                        batch,
                        &delta.nodes,
                        &deleted_node_ids,
                        &topology_delta_sha256(
                            &delta.nodes,
                            &delta.edges,
                            &deleted_nodes,
                            &deleted_edges,
                        ),
                    )
                })
                .transpose()?;
            if let Some(token) = prepared_ordinal.as_mut() {
                token.metrics.orphan_gc_candidates = orphan_gc.candidates;
                token.metrics.orphan_gc_removed = orphan_gc.removed;
                token.metrics.orphan_gc_deferred = orphan_gc.deferred;
                token.metrics.orphan_gc_bytes = orphan_gc.bytes;
            }
            let receipt = prepared_ordinal
                .as_ref()
                .map(PreparedV4OrdinalDelta::auxiliary_receipt)
                .or(receipt);
            *prepared_from_callback.borrow_mut() = token;
            *prepared_v4_from_callback.borrow_mut() = prepared_ordinal;
            Ok(receipt)
        });
    let commit =
        crate::generation::commit_topology_aware_with_participant(staged, &root, participant);
    let token = prepared.borrow_mut().take();
    let v4_token = prepared_v4.borrow_mut().take();
    let committed = match commit {
        Ok(value) => value,
        Err(error) => {
            let (Some(token), Some((prior, next))) = (token.as_ref(), generations.get()) else {
                return Err(error);
            };
            let membership_outcome = reconcile_uuid_auxiliary(&root, prior, next, token)?;
            let outcome = if let Some(v4) = v4_token.as_ref() {
                let ordinal_outcome = reconcile_v4_ordinal_auxiliary(&root, prior, next, v4)?;
                if membership_outcome != ordinal_outcome {
                    return Err(storage_err(
                        "v3 and v4 auxiliary reconciliation outcomes disagree",
                    ));
                }
                ordinal_outcome
            } else {
                membership_outcome
            };
            match outcome {
                crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed => Some(next.topology),
                crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted => {
                    return Err(error);
                }
            }
        }
    };
    let mut committed_metrics = UuidIndexAppendMetrics::default();
    let committed_v4_metrics = v4_token.as_ref().map(|token| token.metrics().clone());
    if let (Some(generation), Some(token)) = (committed, token.as_ref()) {
        token.verify_generation(generation)?;
        if let Some(v4) = v4_token.as_ref() {
            v4.verify_generation(generation)?;
        }
        committed_metrics = token.metrics().clone();
        let refresh = injected_snapshot_refresh_failure().map_or_else(
            || {
                if let Some(value) = snapshot.as_mut() {
                    token.advance_snapshot(value).map(|_| ())
                } else {
                    AuthenticatedUuidIndexSnapshot::open_at_generation(&root, generation)
                        .map(|value| *snapshot = Some(value))
                }
            },
            Err,
        );
        if let Err(error) = refresh {
            *snapshot = None;
            return Ok(CommittedUuidTopologyRewrite::CommittedNeedsRefresh {
                generation,
                metrics: committed_metrics,
                v4_metrics: committed_v4_metrics,
                error,
            });
        }
    }
    Ok(committed.map_or(
        CommittedUuidTopologyRewrite::NoTopologyChange,
        |generation| CommittedUuidTopologyRewrite::Committed {
            generation,
            metrics: committed_metrics,
            v4_metrics: committed_v4_metrics,
        },
    ))
}

/// Commit a topology rewrite that changes no UUID membership while advancing
/// the authenticated membership manifest to the new topology generation.
pub(crate) fn commit_uuid_neutral_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
) -> Result<Option<u64>, GfError> {
    let mut snapshot = None;
    match commit_uuid_topology_rewrite(
        project_dir,
        staged,
        &UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )? {
        CommittedUuidTopologyRewrite::NoTopologyChange => Ok(None),
        CommittedUuidTopologyRewrite::Committed { generation, .. } => Ok(Some(generation)),
        CommittedUuidTopologyRewrite::CommittedNeedsRefresh {
            generation, error, ..
        } => Err(GfError::Storage(format!(
            "topology generation {generation} committed but UUID index snapshot refresh failed: {error}"
        ))),
    }
}

/// Stage one bounded v3 UUID-index delta and its authenticated receipt into the
/// caller's generation-last topology rewrite transaction.
#[allow(clippy::too_many_arguments)] // Mirrors the four disjoint UUID delta domains.
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
            digest: hex_bytes(&digest),
            bytes: receipt_bytes.len() as u64,
        },
    }))
}

fn hex_sha256(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
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
    let topology = project
        .open_child_directory(std::ffi::OsStr::new("topology"))
        .map_err(storage_err)?;
    let index = topology
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
    topology.revalidate_named().map_err(storage_err)?;
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

fn reconcile_v4_ordinal_auxiliary(
    project_dir: &Path,
    prior: crate::durable_rewrite::GenerationPair,
    next: crate::durable_rewrite::GenerationPair,
    prepared: &PreparedV4OrdinalDelta,
) -> Result<crate::durable_rewrite::AuxiliaryReconcileOutcome, GfError> {
    let outcome = crate::durable_rewrite::reconcile_auxiliary(
        project_dir,
        prior,
        next,
        &prepared.auxiliary_receipt(),
    )?;
    if outcome == crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted {
        return Ok(outcome);
    }
    let index = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_RECEIPT))
        .map_err(storage_err)?;
    let receipt_body = read_bounded(
        &mut receipt_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&receipt_body).map_err(storage_err)?;
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST))
        .map_err(storage_err)?;
    let manifest_body = read_bounded(
        &mut manifest_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    index.revalidate_named().map_err(storage_err)?;
    let expected = serde_json::to_vec(&prepared.manifest).map_err(storage_err)?;
    if receipt.expected_generation != next.topology
        || receipt.manifest_sha256 != hex_sha256(&manifest_body)
        || manifest_body != expected
    {
        return Err(storage_err(
            "committed v4 ordinal receipt does not authenticate the expected manifest",
        ));
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity, clippy::too_many_lines)] // Pure planner returns its authenticated transaction bundle.
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
    let mut manifest = if let Some(retained) = snapshot.as_deref_mut() {
        retained.revalidate()?;
        if retained.manifest.current_generation != current {
            return Err(storage_err("retained manifest generation is stale"));
        }
        retained.manifest.clone()
    } else if current == 0 {
        Manifest {
            format_version: FORMAT_VERSION,
            base_generation: 0,
            current_generation: 0,
            live_node_count: 0,
            live_edge_count: 0,
            runs: Vec::new(),
        }
    } else {
        return Err(storage_err("authenticated UUID snapshot is required"));
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
    if current == 0 && manifest.runs.is_empty() {
        let empty_identity_path = scratch.join("identities-base.run");
        let empty_surrogate_path = scratch.join("surrogates-base.run");
        let empty_identity = create_uuid_file(&empty_identity_path)?;
        sync_uuid_file(&empty_identity)?;
        let empty_surrogate = create_uuid_file(&empty_surrogate_path)?;
        sync_uuid_file(&empty_surrogate)?;
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
            let file = open_uuid_child_file(&snapshot.root, std::ffi::OsStr::new(name))?;
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
    let mut file = open_uuid_child_file(directory, std::ffi::OsStr::new(&record.name))?;
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
        let width = usize::try_from(record_bytes)
            .map_err(|_| storage_err("record width does not fit address space"))?;
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
    let digest = hex_bytes(&whole.finalize());
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
    let (sha256, blocks) = describe_blocks(&mut open_uuid_file(path)?, width)?;
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
    Ok((hex_bytes(&whole.finalize()), blocks))
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
        return open_uuid_file(path);
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
    open_uuid_file(&root.join(name))
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
            width: usize::try_from(width)
                .map_err(|_| storage_err("record width does not fit address space"))?,
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
    let mut out = create_uuid_file(output)?;
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
    sync_uuid_file(&out)?;
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
    let mut out = create_uuid_file(output)?;
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
    sync_uuid_file(&out)?;
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

fn read_bounded(file: &mut (impl Read + Seek), maximum: u64) -> Result<Vec<u8>, GfError> {
    let length = file.seek(SeekFrom::End(0)).map_err(storage_err)?;
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    if length > maximum {
        return Err(storage_err("recovery control record exceeds size limit"));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| storage_err("control record length does not fit address space"))?;
    let mut body = Vec::with_capacity(capacity);
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

fn write_identity_records(path: &Path, records: &[(Uuid, u8, u64)]) -> Result<(), GfError> {
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = create_uuid_file(path)?;
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
    sync_uuid_file(&file)
}

fn write_surrogate_records(path: &Path, records: &[(u64, Uuid)]) -> Result<(), GfError> {
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = create_uuid_file(path)?;
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
    sync_uuid_file(&file)
}

#[cfg(test)]
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

/// Stage one incremental v4 node-ordinal delta beside the canonical topology
/// mutation. The caller supplies a receipt-authenticated, lifetime-pinned prior
/// snapshot and owns the enclosing generation-last rewrite transaction.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
// One manifest-last planner lifecycle; the ordering between pinned admission,
// delta artifacts, compaction, receipt, manifest, and evidence is the invariant.
pub(crate) fn prepare_v4_ordinal_delta(
    project_dir: &Path,
    current: u64,
    generation: u64,
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    batch: &mut crate::staging::RewriteBatch,
    nodes: &[(Uuid, u64)],
    deleted_node_ids: &[u64],
    topology_delta_sha256: &str,
) -> Result<PreparedV4OrdinalDelta, GfError> {
    if generation
        != current
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 generation overflow"))?
        || pinned.manifest.topology_generation != current
    {
        return Err(storage_err(
            "prepared v4 ordinal delta is not the next authenticated generation",
        ));
    }
    if topology_delta_sha256.len() != 64
        || !topology_delta_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_err("v4 topology delta digest is noncanonical"));
    }

    let plan_root = open_v4_plan_root(project_dir)?;
    cleanup_abandoned_v4_plan_directories(&plan_root)?;
    let plan_root_path = project_dir.join(INDEX_DIR).join(V4_PLAN_ROOT);
    let scratch = tempfile::Builder::new()
        .prefix(V4_PLAN_PREFIX)
        .tempdir_in(&plan_root_path)
        .map_err(storage_err)?;
    // The retained capability proves that path lookup did not escape or replace
    // the authenticated, project-owned planner namespace during creation.
    plan_root.revalidate_named().map_err(storage_err)?;
    let artifacts_path = scratch.path().join("artifacts");
    fs::create_dir(&artifacts_path).map_err(storage_err)?;
    let artifacts =
        graphforge_filesystem::StableDirectory::open(&artifacts_path).map_err(storage_err)?;

    let mut build_metrics = UuidIndexBuildMetrics::default();
    let uuid_sorted = external_sort_v4_nodes(nodes, scratch.path(), &mut build_metrics)?;
    reject_prior_v4_uuid_reuse(pinned, &uuid_sorted)?;
    let ordinal_sorted = build_surrogate_run(
        &uuid_sorted,
        scratch.path(),
        UuidIndexBuildLimits::default(),
        &mut build_metrics,
    )?;
    let mut deleted = deleted_node_ids.to_vec();
    deleted.sort_unstable();
    if deleted.contains(&0) || deleted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(
            "v4 tombstone delta is not sorted unique nonzero",
        ));
    }

    let prior_max = pinned
        .manifest
        .ordinal_ranges
        .last()
        .map(|range| {
            range
                .first_node_id
                .checked_add(range.count.saturating_sub(1))
                .ok_or_else(|| storage_err("retained v4 ordinal range overflows"))
        })
        .transpose()?
        .unwrap_or(0);
    let mut ordinal_probe = BufReader::new(File::open(&ordinal_sorted).map_err(storage_err)?);
    let first_new = read_surrogate_record(&mut ordinal_probe)?;
    if first_new.is_some_and(|(id, _)| id <= prior_max) {
        return Err(storage_err(
            "v4 node surrogate is not monotonic or is reused",
        ));
    }
    if deleted.iter().any(|id| {
        !pinned.manifest.ordinal_ranges.iter().any(|range| {
            range
                .first_node_id
                .checked_add(range.count.saturating_sub(1))
                .is_some_and(|last| (range.first_node_id..=last).contains(id))
        })
    }) {
        return Err(storage_err(
            "v4 tombstone does not name retained ordinal authority",
        ));
    }

    let mut writer = V4OrdinalConstructionWriter::start(generation, &artifacts)?;
    let mut cancelled = || false;
    let mut forward = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&uuid_sorted).map_err(storage_err)?,
    );
    while let Some((uuid, node_id)) = read_node_surrogate_record(&mut forward)? {
        writer.push_forward(Uuid::from_bytes(uuid), node_id, &mut cancelled)?;
    }
    let mut ordinal = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&ordinal_sorted).map_err(storage_err)?,
    );
    while let Some((node_id, uuid)) = read_surrogate_record(&mut ordinal)? {
        writer.push_ordinal(node_id, Uuid::from_bytes(uuid), &mut cancelled)?;
    }
    let V4ConstructionArtifactBundle {
        manifest: delta_manifest,
        metrics: build,
        publications: delta_publications,
    } = writer.finish()?;
    let (tombstones, tombstone_bytes, tombstone_blocks) =
        write_v4_tombstone_artifact(&artifacts, generation, &deleted)?;
    let tombstone_name = tombstones.run.artifact.name.clone();
    crate::project_failpoint::hit(
        "v4_append.after_delta_artifacts",
        None,
        None,
        "V4_APPEND_ARTIFACTS",
        false,
    )?;

    let mut manifest = pinned.manifest.clone();
    manifest.topology_generation = generation;
    manifest
        .forward_identities
        .extend(delta_manifest.forward_identities);
    manifest
        .ordinal_ranges
        .extend(delta_manifest.ordinal_ranges);
    manifest.tombstones.push(tombstones.run);
    manifest
        .ordinal_ranges
        .sort_unstable_by_key(|range| range.first_node_id);

    let mut created = manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .filter(|artifact| artifact.generation == generation)
        .map(|artifact| (artifact.name.clone(), artifacts_path.join(&artifact.name)))
        .collect::<HashMap<_, _>>();
    let delta_created_artifacts = u64::try_from(created.len()).map_err(storage_err)?;
    let mut compaction = compact_v4_binary_carry(
        pinned,
        &artifacts,
        &artifacts_path,
        &mut manifest,
        &mut created,
    )?;
    if compaction.compactions != 0 {
        crate::project_failpoint::hit(
            "v4_compaction.after_outputs",
            None,
            None,
            "V4_COMPACTION_OUTPUTS",
            false,
        )?;
    }
    v4_publication_failure("manifest_update")?;
    admit_v4_construction_manifest(&manifest)?;

    let destination = project_dir.join(INDEX_DIR);
    let retained_names = v4_manifest_artifact_names(&manifest);
    let mut outputs = created
        .into_iter()
        .filter(|(name, _)| retained_names.contains(name))
        .collect::<Vec<_>>();
    outputs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, path) in &outputs {
        batch.stage_file(&destination.join(name), path)?;
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    let receipt = TopologyIndexReceipt {
        nonce: Uuid::new_v4().simple().to_string(),
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256.to_owned(),
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let receipt_path = destination.join(V4_ORDINAL_RECEIPT);
    let manifest_path = destination.join(V4_ORDINAL_MANIFEST);
    batch.stage_bytes(&receipt_path, &receipt_bytes)?;
    crate::project_failpoint::hit(
        "v4_append.after_receipt_stage",
        None,
        None,
        "V4_APPEND_RECEIPT",
        false,
    )?;
    batch.stage_bytes(&manifest_path, &manifest_bytes)?;
    batch.move_staged_destination_to_end(&manifest_path);
    crate::project_failpoint::hit(
        "v4_append.after_manifest_stage",
        None,
        None,
        "V4_APPEND_MANIFEST",
        false,
    )?;

    let prior_names = v4_manifest_artifact_names(&pinned.manifest);
    let staged_artifact_bytes = outputs.iter().try_fold(0_u64, |sum, (_, path)| {
        sum.checked_add(path.metadata().map_err(storage_err)?.len())
            .ok_or_else(|| storage_err("v4 staged artifact byte count overflow"))
    })?;
    let control_bytes =
        u64::try_from(receipt_bytes.len() + manifest_bytes.len()).map_err(storage_err)?;
    let submitted_write_bytes = build
        .artifact_bytes
        .saturating_add(tombstone_bytes)
        .saturating_add(compaction.write_bytes)
        .saturating_add(staged_artifact_bytes)
        .checked_add(control_bytes)
        .ok_or_else(|| storage_err("v4 submitted write byte count overflow"))?;
    let staged_write_blocks = outputs.iter().try_fold(0_u64, |sum, (_, path)| {
        let bytes = path.metadata().map_err(storage_err)?.len();
        sum.checked_add(bytes.div_ceil(crate::staging::STAGE_FILE_BLOCK_BYTES as u64))
            .ok_or_else(|| storage_err("v4 staged write block count overflow"))
    })?;
    let sorting_temporary_upper = u64::try_from(nodes.len())
        .map_err(storage_err)?
        .checked_mul(24 * 4)
        .ok_or_else(|| storage_err("v4 sorting temporary byte count overflow"))?;
    let external_peak_buffer = build_metrics
        .peak_buffered_records
        .checked_mul(24)
        .ok_or_else(|| storage_err("v4 external-sort buffer charge overflow"))?;
    let metrics = V4OrdinalAppendMetrics {
        input_identities: u64::try_from(nodes.len()).map_err(storage_err)?,
        input_tombstones: u64::try_from(deleted.len()).map_err(storage_err)?,
        created_artifacts: delta_created_artifacts.saturating_add(compaction.created_artifacts),
        retained_artifacts: u64::try_from(retained_names.intersection(&prior_names).count())
            .map_err(storage_err)?,
        physical_bytes_written: submitted_write_bytes,
        compactions: compaction.compactions,
        sequential_read_bytes: compaction.read_bytes,
        sequential_read_calls: compaction.read_calls,
        sequential_read_blocks: compaction.read_blocks,
        write_bytes: submitted_write_bytes,
        write_blocks: build
            .write_blocks
            .saturating_add(tombstone_blocks)
            .saturating_add(compaction.write_blocks)
            .saturating_add(staged_write_blocks)
            .saturating_add(2),
        peak_buffer_bytes: build
            .peak_buffer_bytes
            .max(V4_ORDINAL_BLOCK_BYTES * 3)
            .max(crate::staging::STAGE_FILE_BLOCK_BYTES)
            .max(external_peak_buffer),
        peak_temporary_bytes: build
            .peak_temporary_bytes
            .saturating_add(tombstone_bytes)
            .saturating_add(compaction.write_bytes)
            .saturating_add(staged_artifact_bytes)
            .saturating_add(control_bytes)
            .saturating_add(sorting_temporary_upper),
        fsync_operations: build
            .fsync_operations
            .saturating_add(1)
            .saturating_add(compaction.fsync_operations)
            .saturating_add(u64::try_from(outputs.len()).map_err(storage_err)?)
            .saturating_add(2),
        cache_release: compaction.cache_release,
        peak_configured_cache_window_bytes: compaction.peak_configured_cache_window_bytes,
        ..Default::default()
    };
    let prepared = PreparedV4OrdinalDelta {
        expected_generation: generation,
        auxiliary: crate::AuxiliaryReceipt {
            kind: "uuid-membership/v4".to_owned(),
            schema_version: crate::ORDINAL_IDENTITY_V4,
            path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
            digest: hex_sha256(&receipt_bytes),
            bytes: u64::try_from(receipt_bytes.len()).map_err(storage_err)?,
        },
        metrics,
        manifest,
    };
    let mut authority_publications = Vec::new();
    if retained_names.contains(&tombstone_name) {
        retain_v4_publication(
            &mut authority_publications,
            tombstone_name,
            tombstones.publication,
        );
    }
    for (name, publication) in delta_publications {
        if retained_names.contains(&name) {
            authority_publications.push((name, publication));
        }
    }
    for (name, publication) in compaction.publications.drain(..) {
        if retained_names.contains(&name) {
            authority_publications.push((name, publication));
        }
    }
    commit_v4_publications(authority_publications, V4AuthorityTransactionProof)?;
    Ok(prepared)
}

const V4_PLAN_PREFIX: &str = "uuid-membership-v4-plan-";
const V4_PLAN_ROOT: &str = ".uuid-membership-v4-plans";
const V4_PLAN_CLEANUP_CANDIDATES: usize = 16;
const V4_PLAN_CLEANUP_ENTRIES: usize = 4096;
const V4_PLAN_CLEANUP_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn open_v4_plan_root(
    project_dir: &Path,
) -> Result<graphforge_filesystem::StableDirectory, GfError> {
    let index = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
    let name = std::ffi::OsStr::new(V4_PLAN_ROOT);
    let root = match index.create_child_directory(name) {
        Ok(root) => {
            index.sync().map_err(storage_err)?;
            root
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            index.open_child_directory(name).map_err(storage_err)?
        }
        Err(error) => return Err(storage_err(error)),
    };
    index.revalidate_named().map_err(storage_err)?;
    Ok(root)
}

/// Reclaim planner directories from the authenticated project-owned namespace.
/// Atomic child creation makes every visible entry recognizable immediately;
/// no forgeable marker or shared-parent scan participates in ownership.
fn cleanup_abandoned_v4_plan_directories(
    plan_root: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    let candidates = plan_root
        .child_names_bounded(V4_PLAN_CLEANUP_CANDIDATES + 1)
        .map_err(|error| storage_err(format!("v4 planner namespace inventory: {error}")))?;
    if candidates.len() > V4_PLAN_CLEANUP_CANDIDATES {
        return Err(storage_err("v4 planner cleanup candidate bound exceeded"));
    }
    for name in candidates {
        if !name.to_str().is_some_and(|text| {
            text.strip_prefix(V4_PLAN_PREFIX).is_some_and(|nonce| {
                !nonce.is_empty() && nonce.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
        }) {
            return Err(storage_err(
                "v4 planner namespace contains an unknown child",
            ));
        }
        let directory = plan_root.open_child_directory(&name).map_err(storage_err)?;
        cleanup_v4_plan_directory(&directory)?;
        crate::project_failpoint::hit(
            "v4_plan_cleanup.before_unlink",
            None,
            None,
            "V4_PLAN_CLEANUP_BEFORE_UNLINK",
            false,
        )?;
        let directory_identity = directory.identity();
        drop(directory);
        plan_root
            .remove_child_directory_if_identity(&name, directory_identity)
            .map_err(storage_err)?;
        crate::project_failpoint::hit(
            "v4_plan_cleanup.after_unlink",
            None,
            None,
            "V4_PLAN_CLEANUP_AFTER_UNLINK",
            false,
        )?;
        plan_root.sync().map_err(storage_err)?;
    }
    plan_root.revalidate_named().map_err(storage_err)?;
    Ok(())
}

fn cleanup_v4_plan_directory(
    directory: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    let names = directory
        .child_names_bounded(V4_PLAN_CLEANUP_ENTRIES)
        .map_err(|error| storage_err(format!("v4 planner directory inventory: {error}")))?;
    let mut bytes = 0_u64;
    for name in names {
        if name == std::ffi::OsStr::new("artifacts") {
            let artifacts = directory.open_child_directory(&name).map_err(storage_err)?;
            cleanup_v4_plan_files(&artifacts, &mut bytes)?;
            let artifacts_identity = artifacts.identity();
            drop(artifacts);
            directory
                .remove_child_directory_if_identity(&name, artifacts_identity)
                .map_err(storage_err)?;
        } else {
            cleanup_v4_plan_file(directory, &name, &mut bytes)?;
        }
    }
    directory.sync().map_err(storage_err)
}

fn cleanup_v4_plan_files(
    directory: &graphforge_filesystem::StableDirectory,
    bytes: &mut u64,
) -> Result<(), GfError> {
    let names = directory
        .child_names_bounded(V4_PLAN_CLEANUP_ENTRIES)
        .map_err(|error| storage_err(format!("v4 planner artifact inventory: {error}")))?;
    for name in names {
        cleanup_v4_plan_file(directory, &name, bytes)?;
    }
    directory.sync().map_err(storage_err)
}

fn cleanup_v4_plan_file(
    directory: &graphforge_filesystem::StableDirectory,
    name: &std::ffi::OsStr,
    bytes: &mut u64,
) -> Result<(), GfError> {
    let file = directory.open_child_file(name).map_err(storage_err)?;
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
        return Err(storage_err("v4 planner cleanup file has unexpected links"));
    }
    *bytes = bytes
        .checked_add(file.metadata().map_err(storage_err)?.len())
        .ok_or_else(|| storage_err("v4 planner cleanup byte count overflow"))?;
    if *bytes > V4_PLAN_CLEANUP_BYTES {
        return Err(storage_err("v4 planner cleanup byte bound exceeded"));
    }
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
    drop(file);
    directory
        .unlink_child_if_identity(name, identity)
        .map_err(storage_err)
}

/// Prove that an appended UUID has never appeared in retained forward
/// authority. Tombstones do not release identity: ordinals are monotonic and
/// UUIDs are never reusable. Both sides are sorted, so each retained run is
/// checked by one bounded sequential merge before anything enters the caller's
/// rewrite batch.
fn reject_prior_v4_uuid_reuse(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    uuid_sorted: &Path,
) -> Result<(), GfError> {
    for artifact in &pinned.manifest.forward_identities {
        let mut prior = BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            clone_pinned_v4_file(pinned, &artifact.name)?,
        );
        let mut appended = BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            File::open(uuid_sorted).map_err(storage_err)?,
        );
        let mut prior_head = read_v4_forward_record(&mut prior)?;
        let mut appended_head = read_node_surrogate_record(&mut appended)?;
        while let (Some((prior_uuid, _)), Some((appended_uuid, _))) = (prior_head, appended_head) {
            match prior_uuid.cmp(&appended_uuid) {
                std::cmp::Ordering::Less => prior_head = read_v4_forward_record(&mut prior)?,
                std::cmp::Ordering::Greater => {
                    appended_head = read_node_surrogate_record(&mut appended)?;
                }
                std::cmp::Ordering::Equal => {
                    return Err(storage_err(
                        "v4 node UUID already exists in retained forward authority",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn external_sort_v4_nodes(
    nodes: &[(Uuid, u64)],
    scratch: &Path,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let limits = UuidIndexBuildLimits::default();
    let mut runs = Vec::new();
    let mut buffer = Vec::with_capacity(limits.run_records);
    for &(uuid, node_id) in nodes {
        if uuid.is_nil() || node_id == 0 {
            return Err(storage_err("v4 node delta contains a zero identity"));
        }
        buffer.push((*uuid.as_bytes(), node_id));
        metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
        if buffer.len() == limits.run_records {
            flush_entity_surrogate_run(&mut buffer, scratch, "v4-delta", &mut runs, metrics)?;
        }
    }
    if !buffer.is_empty() {
        flush_entity_surrogate_run(&mut buffer, scratch, "v4-delta", &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("v4-delta-empty.run");
        File::create(&path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        runs.push(path);
    }
    merge_node_surrogate_runs(runs, scratch, limits.merge_fan_in, metrics)
}

fn write_v4_tombstone_artifact(
    index: &graphforge_filesystem::StableDirectory,
    generation: u64,
    ids: &[u64],
) -> Result<(GuardedV4Tombstones, u64, u64), GfError> {
    let mut writer = V4TombstoneStreamWriter::new(index, generation)?;
    for &id in ids {
        writer.push(id)?;
    }
    writer.finish()
}

struct GuardedV4Tombstones {
    run: crate::V4OrdinalTombstones,
    publication: Option<graphforge_filesystem::UnpublishedArtifactGuard>,
}

struct V4TombstoneStreamWriter {
    generation: u64,
    artifact: StreamingV4Artifact,
    blocks: Vec<crate::V4OrdinalTombstoneBlock>,
    block: Vec<u8>,
    offset: u64,
    previous: Option<u64>,
}

impl V4TombstoneStreamWriter {
    fn new(
        index: &graphforge_filesystem::StableDirectory,
        generation: u64,
    ) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(1).map_err(storage_err)?;
        Self::new_with_cache_window(index, generation, cache_window)
    }

    fn new_with_cache_window(
        index: &graphforge_filesystem::StableDirectory,
        generation: u64,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        Ok(Self {
            generation,
            artifact: StreamingV4Artifact::create_with_window(
                index,
                "tombstones-delta",
                cache_window,
            )?,
            blocks: Vec::new(),
            block: Vec::with_capacity(V4_ORDINAL_BLOCK_BYTES),
            offset: 0,
            previous: None,
        })
    }

    fn push(&mut self, id: u64) -> Result<(), GfError> {
        if id == 0 || self.previous.is_some_and(|previous| previous >= id) {
            return Err(storage_err(
                "v4 tombstone stream is not strictly increasing",
            ));
        }
        self.block.extend_from_slice(&id.to_be_bytes());
        self.previous = Some(id);
        if self.block.len() == V4_ORDINAL_BLOCK_BYTES {
            finish_v4_tombstone_block(
                &mut self.artifact,
                &mut self.blocks,
                &mut self.block,
                &mut self.offset,
            )?;
        }
        Ok(())
    }

    fn finish(self) -> Result<(GuardedV4Tombstones, u64, u64), GfError> {
        let (run, bytes, fsyncs, _) = self.finish_with_cache_evidence()?;
        Ok((run, bytes, fsyncs))
    }

    fn finish_with_cache_evidence(
        mut self,
    ) -> Result<
        (
            GuardedV4Tombstones,
            u64,
            u64,
            graphforge_filesystem::FileCacheReleaseEvidence,
        ),
        GfError,
    > {
        let prepared = finish_v4_tombstone_block(
            &mut self.artifact,
            &mut self.blocks,
            &mut self.block,
            &mut self.offset,
        );
        if let Err(primary) = prepared {
            let (_, cleanup) = self.cleanup_unpublished();
            return combine_v4_cleanup(Err(primary), cleanup, "v4 tombstone failed output cleanup");
        }
        let byte_count = self.artifact.bytes;
        let mut metrics = V4OrdinalBuildMetrics::default();
        let artifact = finish_streamed_v4_artifact(
            self.artifact,
            "tombstones-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::NodeTombstones,
            &mut metrics,
        )?;
        Ok((
            GuardedV4Tombstones {
                run: crate::V4OrdinalTombstones {
                    generation: self.generation,
                    artifact: artifact.artifact,
                    blocks: self.blocks,
                },
                publication: artifact.publication,
            },
            byte_count,
            metrics.fsync_operations,
            metrics.cache_release,
        ))
    }

    fn cleanup_unpublished(
        mut self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        let prepared = finish_v4_tombstone_block(
            &mut self.artifact,
            &mut self.blocks,
            &mut self.block,
            &mut self.offset,
        );
        let (cache_release, cleanup) = self.artifact.cleanup_unpublished();
        (
            cache_release,
            combine_v4_cleanup(prepared, cleanup, "v4 tombstone output cleanup"),
        )
    }
}

fn finish_v4_tombstone_block(
    writer: &mut StreamingV4Artifact,
    blocks: &mut Vec<crate::V4OrdinalTombstoneBlock>,
    bytes: &mut Vec<u8>,
    offset: &mut u64,
) -> Result<(), GfError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let first = u64::from_be_bytes(bytes[..8].try_into().expect("fixed tombstone"));
    let last = u64::from_be_bytes(
        bytes[bytes.len() - 8..]
            .try_into()
            .expect("fixed tombstone"),
    );
    blocks.push(crate::V4OrdinalTombstoneBlock {
        offset: *offset,
        count: u64::try_from(bytes.len() / 8).map_err(storage_err)?,
        first,
        last,
        sha256: hex_sha256(bytes),
    });
    writer.push(bytes)?;
    *offset = offset
        .checked_add(u64::try_from(bytes.len()).map_err(storage_err)?)
        .ok_or_else(|| storage_err("v4 tombstone offset overflow"))?;
    bytes.clear();
    Ok(())
}

fn v4_manifest_artifact_names(manifest: &crate::V4OrdinalIdentityManifest) -> BTreeSet<String> {
    manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .map(|artifact| artifact.name.clone())
        .collect()
}

#[derive(Default)]
struct V4CompactionWork {
    compactions: u64,
    created_artifacts: u64,
    read_bytes: u64,
    read_calls: u64,
    read_blocks: u64,
    write_bytes: u64,
    write_blocks: u64,
    fsync_operations: u64,
    cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
    peak_configured_cache_window_bytes: u64,
    publications: Vec<(String, graphforge_filesystem::UnpublishedArtifactGuard)>,
}

fn compact_v4_binary_carry(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    artifacts: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
) -> Result<V4CompactionWork, GfError> {
    let prior_manifest = manifest.clone();
    let prior_created = created.clone();
    match compact_v4_binary_carry_inner(pinned, artifacts, artifacts_path, manifest, created) {
        Ok(work) => Ok(work),
        Err(error) => {
            *manifest = prior_manifest;
            *created = prior_created;
            Err(error)
        }
    }
}

fn compact_v4_binary_carry_inner(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    artifacts: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
) -> Result<V4CompactionWork, GfError> {
    let mut work = V4CompactionWork::default();
    loop {
        let intervals = v4_forward_intervals(&manifest.forward_identities)?;
        if intervals.len() < 3 {
            break;
        }
        let left = intervals[intervals.len() - 2];
        let right = intervals[intervals.len() - 1];
        if left.1 - left.0 != right.1 - right.0 {
            break;
        }
        let left_descriptor = manifest.forward_identities[left.2].clone();
        let right_descriptor = manifest.forward_identities[right.2].clone();
        let merged_forward = merge_v4_forward_artifacts(
            v4_planned_file(pinned, created, &left_descriptor.name)?,
            v4_planned_file(pinned, created, &right_descriptor.name)?,
            artifacts,
            right.1,
            &mut work,
        )?;
        created.insert(
            merged_forward.artifact.name.clone(),
            artifacts_path.join(&merged_forward.artifact.name),
        );
        work.created_artifacts = work.created_artifacts.saturating_add(1);
        manifest
            .forward_identities
            .splice(left.2..=right.2, std::iter::once(merged_forward.artifact));
        retain_v4_publication(
            &mut work.publications,
            manifest.forward_identities[left.2].name.clone(),
            merged_forward.publication,
        );

        compact_v4_ordinal_interval(
            pinned,
            artifacts,
            artifacts_path,
            manifest,
            created,
            left.0,
            right.1,
            &mut work,
        )?;
        compact_v4_tombstone_interval(
            pinned,
            artifacts,
            artifacts_path,
            manifest,
            created,
            left.0,
            right.1,
            &mut work,
        )?;
        work.compactions = work.compactions.saturating_add(1);
    }
    Ok(work)
}

/// Return `(first_generation, last_generation, descriptor_index)`. The first
/// artifact is the immutable construction base. Every later retained forward
/// run closes one contiguous delta interval, so binary-carry level is encoded
/// without expanding the public manifest schema.
fn v4_forward_intervals(
    forwards: &[crate::V4OrdinalArtifact],
) -> Result<Vec<(u64, u64, usize)>, GfError> {
    let Some(base_generation) = forwards.first().map(|artifact| artifact.generation) else {
        return Err(storage_err("v4 forward base generation is invalid"));
    };
    let mut intervals = vec![(1, base_generation, 0)];
    let mut prior = base_generation;
    for (index, artifact) in forwards.iter().enumerate().skip(1) {
        let first = prior
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 forward interval overflows"))?;
        if artifact.generation < first {
            return Err(storage_err("v4 forward generations do not increase"));
        }
        intervals.push((first, artifact.generation, index));
        prior = artifact.generation;
    }
    Ok(intervals)
}

fn v4_planned_file(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    created: &HashMap<String, PathBuf>,
    name: &str,
) -> Result<File, GfError> {
    created.get(name).map_or_else(
        || clone_pinned_v4_file(pinned, name),
        |path| File::open(path).map_err(storage_err),
    )
}

fn read_v4_forward_record(reader: &mut impl Read) -> Result<Option<([u8; 16], u64)>, GfError> {
    let Some(record) = read_exact_record::<24>(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        record[..16].try_into().expect("fixed UUID"),
        u64::from_be_bytes(record[16..].try_into().expect("fixed surrogate")),
    )))
}

type V4CompactionReader = BufReader<graphforge_filesystem::FileCacheReleasingReader>;

fn finish_v4_compaction_readers<T>(
    mut primary: Result<T, GfError>,
    readers: &mut [V4CompactionReader],
    work: &mut V4CompactionWork,
    source: &str,
) -> Result<T, GfError> {
    for reader in readers {
        let released = reader.get_mut().finish().map_err(storage_err);
        if let Ok(evidence) = released {
            merge_cache_release_evidence(&mut work.cache_release, evidence);
        }
        let released = inject_v4_input_release_result(released);
        primary = combine_cache_cleanup(primary, released.map(|_| ()), source);
    }
    primary
}

fn record_v4_compaction_windows(
    work: &mut V4CompactionWork,
    windows: &[std::num::NonZeroU64],
) -> Result<(), GfError> {
    let aggregate = graphforge_filesystem::validate_cache_release_operation_windows(windows)
        .map_err(storage_err)?;
    work.peak_configured_cache_window_bytes =
        work.peak_configured_cache_window_bytes.max(aggregate);
    Ok(())
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

fn fail_v4_compaction_with_output_cleanup<T>(
    primary: GfError,
    writer: StreamingV4Artifact,
    work: &mut V4CompactionWork,
    context: &str,
) -> Result<T, GfError> {
    let (cache_release, cleanup) = writer.cleanup_unpublished();
    merge_cache_release_evidence(&mut work.cache_release, cache_release);
    combine_v4_cleanup(Err(primary), cleanup, context)
}

#[allow(clippy::too_many_lines)]
fn merge_v4_forward_artifacts(
    left: File,
    right: File,
    index: &graphforge_filesystem::StableDirectory,
    generation: u64,
    work: &mut V4CompactionWork,
) -> Result<GuardedV4Artifact, GfError> {
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(3).map_err(storage_err)?;
    let mut readers = [
        BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                left,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage_err)?,
        ),
        BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                right,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage_err)?,
        ),
    ];
    let mut writer =
        StreamingV4Artifact::create_with_window(index, "forward-compact", cache_window)?;
    let configured = record_v4_compaction_windows(
        work,
        &[
            readers[0].get_ref().window_bytes(),
            readers[1].get_ref().window_bytes(),
            writer.writer.get_ref().window_bytes(),
        ],
    );
    if let Err(primary) = configured {
        let primary = finish_v4_compaction_readers::<()>(
            Err(primary),
            &mut readers,
            work,
            "v4 forward compaction source",
        )
        .expect_err("primary configuration failure is retained");
        return fail_v4_compaction_with_output_cleanup(
            primary,
            writer,
            work,
            "v4 forward output cleanup",
        );
    }
    let merged = (|| -> Result<(), GfError> {
        let mut heads = [
            read_v4_forward_record(&mut readers[0])?,
            read_v4_forward_record(&mut readers[1])?,
        ];
        let mut previous = None;
        loop {
            let source = match (heads[0], heads[1]) {
                (None, None) => break,
                (Some(_), None) => 0,
                (None, Some(_)) => 1,
                (Some(left), Some(right)) => usize::from(left.0 >= right.0),
            };
            let record = heads[source].expect("selected head");
            if heads[0].is_some_and(|candidate| candidate.0 == record.0)
                && heads[1].is_some_and(|candidate| candidate.0 == record.0)
            {
                let newer = heads[1].expect("right duplicate");
                heads[0] = read_v4_forward_record(&mut readers[0])?;
                heads[1] = read_v4_forward_record(&mut readers[1])?;
                if record.1 != newer.1 {
                    return Err(storage_err("v4 compaction observed UUID reuse"));
                }
                write_v4_forward_record(&mut writer, newer)?;
                v4_compaction_post_write_failure("forward")?;
                previous = Some(newer.0);
                continue;
            }
            if previous.is_some_and(|uuid| uuid >= record.0) {
                return Err(storage_err("v4 compaction input is not sorted unique"));
            }
            write_v4_forward_record(&mut writer, record)?;
            v4_compaction_post_write_failure("forward")?;
            previous = Some(record.0);
            heads[source] = read_v4_forward_record(&mut readers[source])?;
        }
        Ok(())
    })();
    let merged =
        finish_v4_compaction_readers(merged, &mut readers, work, "v4 forward compaction source");
    if let Err(primary) = merged {
        return fail_v4_compaction_with_output_cleanup(
            primary,
            writer,
            work,
            "v4 forward output cleanup",
        );
    }
    let input_bytes = readers
        .iter()
        .map(|reader| {
            reader
                .get_ref()
                .file()
                .metadata()
                .map_or(0, |metadata| metadata.len())
        })
        .sum::<u64>();
    work.read_bytes = work.read_bytes.saturating_add(input_bytes);
    work.read_calls = work
        .read_calls
        .saturating_add(input_bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64));
    work.read_blocks = work
        .read_blocks
        .saturating_add(input_bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64));
    let bytes = writer.bytes;
    let mut metrics = V4OrdinalBuildMetrics::default();
    let artifact = finish_streamed_v4_artifact(
        writer,
        "forward-v4",
        generation,
        crate::V4OrdinalArtifactKind::ForwardIdentities,
        &mut metrics,
    )?;
    work.write_bytes = work.write_bytes.saturating_add(bytes);
    work.write_blocks = work
        .write_blocks
        .saturating_add(bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64));
    work.fsync_operations = work
        .fsync_operations
        .saturating_add(metrics.fsync_operations);
    let reader_peak = readers.iter().try_fold(0_u64, |sum, reader| {
        sum.checked_add(reader.get_ref().tracker().evidence().peak_window_bytes)
            .ok_or_else(|| storage_err("v4 forward reader cache window overflow"))
    })?;
    let aggregate_peak = reader_peak
        .checked_add(metrics.cache_release.peak_window_bytes)
        .ok_or_else(|| storage_err("v4 forward aggregate cache window overflow"))?;
    merge_cache_release_evidence(&mut work.cache_release, metrics.cache_release);
    work.cache_release.peak_window_bytes = work.cache_release.peak_window_bytes.max(aggregate_peak);
    Ok(artifact)
}

fn write_v4_forward_record(
    writer: &mut StreamingV4Artifact,
    (uuid, node_id): ([u8; 16], u64),
) -> Result<(), GfError> {
    writer.push(&uuid)?;
    writer.push(&node_id.to_be_bytes())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn compact_v4_ordinal_interval(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    index: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
    first_generation: u64,
    last_generation: u64,
    work: &mut V4CompactionWork,
) -> Result<(), GfError> {
    let mut replacement = Vec::new();
    let mut cursor = 0;
    while cursor < manifest.ordinal_ranges.len() {
        let range = &manifest.ordinal_ranges[cursor];
        if !(first_generation..=last_generation).contains(&range.artifact.generation) {
            replacement.push(range.clone());
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut end = cursor + 1;
        let mut prior_end = range
            .first_node_id
            .checked_add(range.count - 1)
            .ok_or_else(|| storage_err("v4 ordinal range overflows"))?;
        while end < manifest.ordinal_ranges.len() {
            let next = &manifest.ordinal_ranges[end];
            if !(first_generation..=last_generation).contains(&next.artifact.generation)
                || prior_end.checked_add(1) != Some(next.first_node_id)
            {
                break;
            }
            prior_end = next
                .first_node_id
                .checked_add(next.count - 1)
                .ok_or_else(|| storage_err("v4 ordinal range overflows"))?;
            end += 1;
        }
        if end - start == 1 {
            replacement.push(manifest.ordinal_ranges[start].clone());
        } else {
            let cache_window =
                graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage_err)?;
            let mut writer = V4OrdinalRangeWriter::new(
                index,
                replacement.len(),
                manifest.ordinal_ranges[start].first_node_id,
                cache_window,
            )?;
            let compacted = (|| -> Result<u64, GfError> {
                let mut reader_peak = 0_u64;
                for source in &manifest.ordinal_ranges[start..end] {
                    let file = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                        v4_planned_file(pinned, created, &source.artifact.name)?,
                        cache_window,
                        graphforge_filesystem::FileCacheReleaseTracker::default(),
                    )
                    .map_err(storage_err)?;
                    let mut readers = [BufReader::with_capacity(V4_ORDINAL_BLOCK_BYTES, file)];
                    let copied = record_v4_compaction_windows(
                        work,
                        &[
                            readers[0].get_ref().window_bytes(),
                            writer.artifact.writer.get_ref().window_bytes(),
                        ],
                    )
                    .and_then(|()| {
                        while let Some(uuid) = read_exact_record::<16>(&mut readers[0])? {
                            writer.push(uuid)?;
                            v4_compaction_post_write_failure("ordinal")?;
                        }
                        Ok(())
                    });
                    finish_v4_compaction_readers(
                        copied,
                        &mut readers,
                        work,
                        "v4 ordinal compaction source",
                    )?;
                    reader_peak = reader_peak
                        .max(readers[0].get_ref().tracker().evidence().peak_window_bytes);
                    work.read_bytes = work.read_bytes.saturating_add(source.artifact.bytes);
                    work.read_calls = work.read_calls.saturating_add(
                        source
                            .artifact
                            .bytes
                            .div_ceil(V4_ORDINAL_BLOCK_BYTES as u64),
                    );
                    work.read_blocks = work.read_blocks.saturating_add(
                        source
                            .artifact
                            .bytes
                            .div_ceil(V4_ORDINAL_BLOCK_BYTES as u64),
                    );
                }
                Ok(reader_peak)
            })();
            let reader_peak = match compacted {
                Ok(reader_peak) => reader_peak,
                Err(primary) => {
                    let (cache_release, cleanup) = writer.cleanup_unpublished();
                    merge_cache_release_evidence(&mut work.cache_release, cache_release);
                    return combine_v4_cleanup(Err(primary), cleanup, "v4 ordinal output cleanup");
                }
            };
            let mut ranges = Vec::new();
            let mut metrics = V4OrdinalBuildMetrics::default();
            finish_streamed_v4_range(
                last_generation,
                writer,
                &mut ranges,
                &mut work.publications,
                &mut metrics,
            )?;
            let aggregate_peak = reader_peak
                .checked_add(metrics.cache_release.peak_window_bytes)
                .ok_or_else(|| storage_err("v4 ordinal aggregate cache window overflow"))?;
            merge_cache_release_evidence(&mut work.cache_release, metrics.cache_release);
            work.cache_release.peak_window_bytes =
                work.cache_release.peak_window_bytes.max(aggregate_peak);
            let merged = ranges.pop().expect("one merged range");
            created.insert(
                merged.artifact.name.clone(),
                artifacts_path.join(&merged.artifact.name),
            );
            work.created_artifacts = work.created_artifacts.saturating_add(1);
            work.write_bytes = work.write_bytes.saturating_add(merged.artifact.bytes);
            work.write_blocks = work.write_blocks.saturating_add(
                merged
                    .artifact
                    .bytes
                    .div_ceil(V4_ORDINAL_BLOCK_BYTES as u64),
            );
            work.fsync_operations = work
                .fsync_operations
                .saturating_add(metrics.fsync_operations);
            replacement.push(merged);
        }
        cursor = end;
    }
    manifest.ordinal_ranges = replacement;
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn compact_v4_tombstone_interval(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    index: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
    first_generation: u64,
    last_generation: u64,
    work: &mut V4CompactionWork,
) -> Result<(), GfError> {
    let selected = manifest
        .tombstones
        .iter()
        .filter(|run| (first_generation..=last_generation).contains(&run.generation))
        .cloned()
        .collect::<Vec<_>>();
    if selected.len() < 2 {
        return Ok(());
    }
    let active_streams = selected
        .len()
        .checked_add(1)
        .ok_or_else(|| storage_err("v4 tombstone compaction stream count overflow"))?;
    let cache_window = graphforge_filesystem::cache_release_window_for_streams(active_streams)
        .map_err(storage_err)?;
    let mut readers = selected
        .iter()
        .map(|run| {
            graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                v4_planned_file(pinned, created, &run.artifact.name)?,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map(|file| BufReader::with_capacity(V4_ORDINAL_BLOCK_BYTES, file))
            .map_err(storage_err)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut merged =
        V4TombstoneStreamWriter::new_with_cache_window(index, last_generation, cache_window)?;
    let mut windows = readers
        .iter()
        .map(|reader| reader.get_ref().window_bytes())
        .collect::<Vec<_>>();
    windows.push(merged.artifact.writer.get_ref().window_bytes());
    let combined = record_v4_compaction_windows(work, &windows).and_then(|()| {
        let mut heap = BinaryHeap::<Reverse<(u64, usize)>>::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(bytes) = read_exact_record::<8>(reader)? {
                heap.push(Reverse((u64::from_be_bytes(bytes), source)));
            }
        }
        let mut previous = None;
        while let Some(Reverse((id, source))) = heap.pop() {
            if previous != Some(id) {
                merged.push(id)?;
                v4_compaction_post_write_failure("tombstone")?;
                previous = Some(id);
            }
            if let Some(bytes) = read_exact_record::<8>(&mut readers[source])? {
                heap.push(Reverse((u64::from_be_bytes(bytes), source)));
            }
        }
        Ok(())
    });
    let combined = finish_v4_compaction_readers(
        combined,
        &mut readers,
        work,
        "v4 tombstone compaction source",
    );
    if let Err(primary) = combined {
        let (cache_release, cleanup) = merged.cleanup_unpublished();
        merge_cache_release_evidence(&mut work.cache_release, cache_release);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 tombstone output cleanup");
    }
    let reader_peak = readers.iter().try_fold(0_u64, |sum, reader| {
        sum.checked_add(reader.get_ref().tracker().evidence().peak_window_bytes)
            .ok_or_else(|| storage_err("v4 tombstone reader cache window overflow"))
    })?;
    let (run, bytes, fsyncs, writer_cache_release) = merged.finish_with_cache_evidence()?;
    let aggregate_peak = reader_peak
        .checked_add(writer_cache_release.peak_window_bytes)
        .ok_or_else(|| storage_err("v4 tombstone aggregate cache window overflow"))?;
    merge_cache_release_evidence(&mut work.cache_release, writer_cache_release);
    work.cache_release.peak_window_bytes = work.cache_release.peak_window_bytes.max(aggregate_peak);
    created.insert(
        run.run.artifact.name.clone(),
        artifacts_path.join(&run.run.artifact.name),
    );
    work.created_artifacts = work.created_artifacts.saturating_add(1);
    manifest
        .tombstones
        .retain(|candidate| !(first_generation..=last_generation).contains(&candidate.generation));
    manifest.tombstones.push(run.run);
    retain_v4_publication(
        &mut work.publications,
        manifest.tombstones.last().unwrap().artifact.name.clone(),
        run.publication,
    );
    manifest
        .tombstones
        .sort_unstable_by_key(|run| run.generation);
    work.read_bytes = work
        .read_bytes
        .saturating_add(selected.iter().map(|run| run.artifact.bytes).sum::<u64>());
    work.read_calls = work.read_calls.saturating_add(
        selected
            .iter()
            .map(|run| run.artifact.bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64))
            .sum::<u64>(),
    );
    work.read_blocks = work.read_blocks.saturating_add(
        selected
            .iter()
            .map(|run| run.artifact.bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64))
            .sum::<u64>(),
    );
    work.write_bytes = work.write_bytes.saturating_add(bytes);
    work.write_blocks = work
        .write_blocks
        .saturating_add(bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64));
    work.fsync_operations = work.fsync_operations.saturating_add(fsyncs);
    Ok(())
}

/// Explicit bounded rebuild/migration path. Immutable data files are completed
/// and synced first; `manifest.json` is atomically replaced last.
pub fn rebuild_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    migrate_uuid_membership_indexes(project_dir, limits, true)
}

/// Rebuild v4 ordinal identity from canonical topology, never v3 reverse state.
pub fn rebuild_v4_ordinal_identity(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    rebuild_v4_ordinal_identity_with_evidence(project_dir, limits).map(|evidence| evidence.build)
}

/// Rebuild v4 ordinal identity and return its explicit sanitized disposition.
pub fn rebuild_v4_ordinal_identity_with_evidence(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<V4OrdinalRebuildEvidence, GfError> {
    let metrics = std::rc::Rc::new(std::cell::RefCell::new(None));
    let callback_metrics = std::rc::Rc::clone(&metrics);
    let root = project_dir.to_path_buf();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(move |context, batch| {
            let mut staged = stage_v4_ordinal_rebuild_locked(
                context.project_root,
                context.prior.topology,
                limits,
                batch,
            )?;
            let manifest_path = context
                .project_root
                .join(INDEX_DIR)
                .join(V4_ORDINAL_MANIFEST);
            let manifest_bytes = fs::read(
                batch
                    .staged_temp(&manifest_path)
                    .ok_or_else(|| storage_err("v4 rebuild did not stage its manifest"))?,
            )
            .map_err(storage_err)?;
            let receipt = TopologyIndexReceipt {
                nonce: Uuid::new_v4().simple().to_string(),
                expected_generation: context.prior.topology,
                topology_delta_sha256: hex_sha256(b"uuid-membership-v4-canonical-rebuild"),
                manifest_sha256: hex_sha256(&manifest_bytes),
            };
            let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
            batch.stage_bytes(
                &context
                    .project_root
                    .join(INDEX_DIR)
                    .join(V4_ORDINAL_RECEIPT),
                &receipt_bytes,
            )?;
            staged.scratch.register_staged_control(
                u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("v4 rebuild receipt length overflow"))?,
            )?;
            *callback_metrics.borrow_mut() = Some(v4_rebuild_evidence(
                staged.generation,
                staged.build,
                &staged.artifacts,
                staged.scratch,
            )?);
            // Artifacts, then receipt, then facet manifest. The enclosing
            // generation record remains the durable transaction's last switch.
            batch.move_staged_destination_to_end(&manifest_path);
            Ok(Some(crate::AuxiliaryReceipt {
                kind: "uuid-membership/v4".to_owned(),
                schema_version: crate::ORDINAL_IDENTITY_V4,
                path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
                digest: hex_sha256(&receipt_bytes),
                bytes: u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("receipt length overflow"))?,
            }))
        });
    crate::generation::commit_topology_aware_with_participant(
        crate::staging::RewriteBatch::new(),
        &root,
        participant,
    )?;
    metrics
        .borrow_mut()
        .take()
        .ok_or_else(|| storage_err("v4 ordinal rebuild produced no evidence"))
}

#[allow(clippy::too_many_lines)]
fn stage_v4_ordinal_rebuild_locked(
    project_dir: &Path,
    generation: u64,
    limits: UuidIndexBuildLimits,
    batch: &mut crate::staging::RewriteBatch,
) -> Result<StagedV4OrdinalRebuild, GfError> {
    if generation == 0 {
        return Err(storage_err(
            "v4 ordinal identity requires a nonzero topology generation",
        ));
    }
    let limits = limits.validate()?;
    let destination = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&destination).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-v4-build-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let artifact_root = scratch.path().join("artifacts");
    fs::create_dir(&artifact_root).map_err(storage_err)?;
    let artifact_directory =
        graphforge_filesystem::StableDirectory::open(&artifact_root).map_err(storage_err)?;
    let mut metrics = UuidIndexBuildMetrics::default();
    let mut scratch_accounting = V4RebuildScratchAccounting::default();

    // Both bounded projections originate from this single canonical topology
    // scan. For an immutable project generation, authenticate its declared
    // graph inventory before opening any topology payload. Standalone graph
    // roots have no enclosing inventory authority and retain the admitted,
    // validated Parquet path used by the existing explicit v3 rebuild.
    let uuid_runs = if let Some(selected) = selected_generation_for_graph_root(project_dir)? {
        let mut pinned = selected
            .authenticated_graph_files_pinned_where(|entry| {
                canonical_node_topology_inventory_path(&entry.relative_path)
            })?
            .ok_or_else(|| {
                storage_err("selected project generation has no authenticated graph inventory")
            })?;
        pinned.sort_by(|left, right| left.entry.relative_path.cmp(&right.entry.relative_path));
        scan_pinned_entity_surrogate_runs(
            &pinned,
            "node_uuid",
            "node_id",
            "v4-node",
            scratch.path(),
            limits,
            &mut metrics,
        )?
    } else {
        // Standalone roots have no immutable project inventory authority.
        let node_paths = crate::mutator::node_parquet_files(project_dir).map_err(storage_err)?;
        scan_entity_surrogate_runs(
            &node_paths,
            "node_uuid",
            "node_id",
            "v4-node",
            scratch.path(),
            limits,
            &mut metrics,
        )?
    };
    // No v3 index file is opened or used as migration authority.
    let uuid_sorted =
        merge_node_surrogate_runs(uuid_runs, scratch.path(), limits.merge_fan_in, &mut metrics)?;
    scratch_accounting.register_sorted_projection(&uuid_sorted)?;
    let ordinal_sorted = build_surrogate_run(&uuid_sorted, scratch.path(), limits, &mut metrics)?;
    scratch_accounting.register_surrogate_projection(&ordinal_sorted)?;

    let mut writer = V4OrdinalConstructionWriter::start(generation, &artifact_directory)?;
    let mut cancelled = || false;
    let mut forward = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&uuid_sorted).map_err(storage_err)?,
    );
    while let Some((uuid, node_id)) = read_node_surrogate_record(&mut forward)? {
        writer.push_forward(Uuid::from_bytes(uuid), node_id, &mut cancelled)?;
    }
    let mut ordinal = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&ordinal_sorted).map_err(storage_err)?,
    );
    while let Some((node_id, uuid)) = read_surrogate_record(&mut ordinal)? {
        writer.push_ordinal(node_id, Uuid::from_bytes(uuid), &mut cancelled)?;
    }
    let V4ConstructionArtifactBundle {
        manifest,
        metrics: v4_metrics,
        publications,
    } = writer.finish()?;
    scratch_accounting.register_artifacts(v4_metrics.peak_temporary_bytes)?;
    metrics.node_count = v4_metrics.input_records;
    stage_v4_rebuild_artifacts(
        &manifest,
        &destination,
        &artifact_root,
        batch,
        &mut scratch_accounting,
    )?;
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    if manifest_bytes.len() as u64 > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES {
        return Err(storage_err("v4 ordinal manifest exceeds bound"));
    }
    batch.stage_bytes(&destination.join(V4_ORDINAL_MANIFEST), &manifest_bytes)?;
    scratch_accounting.register_staged_control(
        u64::try_from(manifest_bytes.len())
            .map_err(|_| storage_err("v4 ordinal manifest length overflow"))?,
    )?;
    scratch_accounting.release_scratch()?;
    commit_v4_publications(publications, V4AuthorityTransactionProof)?;
    Ok(StagedV4OrdinalRebuild {
        generation,
        build: metrics,
        artifacts: v4_metrics,
        scratch: scratch_accounting,
    })
}

fn stage_v4_rebuild_artifacts(
    manifest: &crate::V4OrdinalIdentityManifest,
    destination: &Path,
    artifact_root: &Path,
    batch: &mut crate::staging::RewriteBatch,
    scratch: &mut V4RebuildScratchAccounting,
) -> Result<(), GfError> {
    for artifact in manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
    {
        batch.stage_file(
            &destination.join(&artifact.name),
            &artifact_root.join(&artifact.name),
        )?;
        scratch.register_staged_artifact(artifact.bytes)?;
    }
    Ok(())
}

fn v4_rebuild_evidence(
    generation: u64,
    build: UuidIndexBuildMetrics,
    artifacts: &V4OrdinalBuildMetrics,
    scratch: V4RebuildScratchAccounting,
) -> Result<V4OrdinalRebuildEvidence, GfError> {
    Ok(V4OrdinalRebuildEvidence {
        disposition: V4OrdinalRebuildDisposition::CanonicalTopology,
        topology_generation: generation,
        input_identities: artifacts.input_records,
        ordinal_ranges: u64::try_from(artifacts.ranges).map_err(storage_err)?,
        artifact_bytes: artifacts.artifact_bytes,
        write_blocks: artifacts.write_blocks,
        peak_buffer_bytes: u64::try_from(artifacts.peak_buffer_bytes).map_err(storage_err)?,
        peak_temporary_bytes: scratch.peak_bytes,
        fsync_operations: artifacts.fsync_operations,
        build,
    })
}

/// Ensure the current topology generation has a v3 UUID index before a
/// topology mutation enters its sealed rewrite callback.
pub(crate) fn ensure_uuid_membership_migrated(project_dir: &Path) -> Result<(), GfError> {
    migrate_uuid_membership_indexes(project_dir, UuidIndexBuildLimits::default(), false).map(|_| ())
}

fn migrate_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
    force: bool,
) -> Result<UuidIndexBuildMetrics, GfError> {
    if !force && uuid_membership_index_is_fresh(project_dir)? {
        return Ok(UuidIndexBuildMetrics::default());
    }
    let metrics = std::rc::Rc::new(std::cell::RefCell::new(None));
    let callback_metrics = std::rc::Rc::clone(&metrics);
    let root = project_dir.to_path_buf();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(move |context, batch| {
            if !force && manifest_generation(context.project_root)? == Some(context.prior.topology)
            {
                return Ok(None);
            }
            let built = stage_uuid_membership_rebuild_locked(
                context.project_root,
                context.prior.topology,
                limits,
                batch,
            )?;
            *callback_metrics.borrow_mut() = Some(built);
            let manifest_destination = context.project_root.join(INDEX_DIR).join(MANIFEST);
            let manifest_temp = batch.staged_temp(&manifest_destination).ok_or_else(|| {
                storage_err("UUID migration did not stage its canonical manifest")
            })?;
            let manifest_bytes = fs::read(manifest_temp).map_err(storage_err)?;
            let receipt = TopologyIndexReceipt {
                nonce: Uuid::new_v4().simple().to_string(),
                expected_generation: context.prior.topology,
                topology_delta_sha256: hex_sha256(b"uuid-membership-migration"),
                manifest_sha256: hex_sha256(&manifest_bytes),
            };
            let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
            batch.stage_bytes(
                &context.project_root.join(INDEX_DIR).join(TOPOLOGY_RECEIPT),
                &receipt_bytes,
            )?;
            Ok(Some(crate::AuxiliaryReceipt {
                kind: "uuid-membership/v3".to_owned(),
                schema_version: FORMAT_VERSION,
                path: format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
                digest: hex_sha256(&receipt_bytes),
                bytes: u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("receipt length overflow"))?,
            }))
        });
    crate::generation::commit_topology_aware_with_participant(
        crate::staging::RewriteBatch::new(),
        &root,
        participant,
    )?;
    let result = metrics.borrow_mut().take().unwrap_or_default();
    Ok(result)
}

#[allow(clippy::too_many_lines)] // Sequential bounded rebuild pipeline with one authority output.
fn stage_uuid_membership_rebuild_locked(
    project_dir: &Path,
    generation: u64,
    limits: UuidIndexBuildLimits,
    batch: &mut crate::staging::RewriteBatch,
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
    reject_cross_kind_identities(&node_tmp, &edge_tmp)?;
    let identity_tmp = scratch.path().join("identities-v3.run");
    build_identity_run(&node_surrogates_tmp, &edge_tmp, &identity_tmp)?;
    let surrogate_tmp =
        build_surrogate_run(&node_surrogates_tmp, scratch.path(), limits, &mut metrics)?;
    let identities = describe_staged_data(
        &identity_tmp,
        "identities-v3",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let node_surrogates = describe_staged_data(
        &surrogate_tmp,
        "node-surrogates-v3",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
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
    batch.stage_file(&root.join(&manifest.runs[0].identities.name), &identity_tmp)?;
    batch.stage_file(
        &root.join(&manifest.runs[0].node_surrogates.name),
        &surrogate_tmp,
    )?;
    batch.stage_bytes(
        &root.join(MANIFEST),
        &serde_json::to_vec(&manifest).map_err(storage_err)?,
    )?;
    Ok(metrics)
}

fn manifest_generation(project_dir: &Path) -> Result<Option<u64>, GfError> {
    let bytes = match fs::read(project_dir.join(INDEX_DIR).join(MANIFEST)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(storage_err)?;
    validate_run_descriptors(&manifest)?;
    Ok((manifest.format_version == FORMAT_VERSION).then_some(manifest.current_generation))
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
    if length % record_bytes != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let mut input = File::open(source).map_err(storage_err)?;
    let (sha256, blocks) = describe_blocks(&mut input, record_bytes)?;
    Ok(FileRecord {
        name: format!("{kind}-{generation}-{}.uuidx", &sha256[..16]),
        count: length / record_bytes,
        sha256,
        blocks,
    })
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

fn canonical_node_topology_inventory_path(relative: &str) -> bool {
    relative == "topology/nodes.parquet"
        || relative
            .strip_prefix("topology/nodes/")
            .is_some_and(|name| !name.contains('/') && name.ends_with(".parquet"))
}

#[allow(clippy::too_many_arguments)]
fn scan_pinned_entity_surrogate_runs(
    inputs: &[crate::project_generation::PinnedGraphFile],
    uuid_column: &str,
    surrogate_column: &str,
    prefix: &str,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<([u8; 16], u64)>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for input in inputs {
        let identity = graphforge_filesystem::file_identity(&input.file).map_err(storage_err)?;
        if identity != input.identity
            || input.file.metadata().map_err(storage_err)?.len() != input.entry.byte_length
        {
            return Err(storage_err(
                "authenticated topology payload identity changed before scan",
            ));
        }
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(input.file.try_clone().map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let uuids = batch
                .column_by_name(uuid_column)
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| {
                    storage_err(format!("{} lacks {uuid_column}", input.entry.relative_path))
                })?;
            let surrogates = batch
                .column_by_name(surrogate_column)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    storage_err(format!(
                        "{} lacks {surrogate_column}",
                        input.entry.relative_path
                    ))
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
        if graphforge_filesystem::file_identity(&input.file).map_err(storage_err)? != input.identity
        {
            return Err(storage_err(
                "authenticated topology payload identity changed during scan",
            ));
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

#[cfg(test)]
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

#[cfg(test)]
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
pub(crate) mod tests {
    use std::io::BufWriter;
    use std::sync::{Arc, Barrier};

    use arrow::array::{FixedSizeBinaryArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn pinned_v4_update(
        root: &Path,
        manifest: crate::V4OrdinalIdentityManifest,
    ) -> crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs {
        let index = root.join(INDEX_DIR);
        let artifacts = manifest
            .forward_identities
            .iter()
            .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
            .chain(manifest.tombstones.iter().map(|run| &run.artifact))
            .map(
                |descriptor| crate::ordinal_identity_v4::PinnedV4OrdinalArtifact {
                    descriptor: descriptor.clone(),
                    file: File::open(index.join(&descriptor.name)).unwrap(),
                },
            )
            .collect();
        crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs {
            manifest,
            artifacts,
        }
    }

    fn install_v4_plan(batch: &crate::staging::RewriteBatch) {
        let destinations = batch
            .staged_paths()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        for destination in destinations {
            let temporary = batch.staged_temp(&destination).unwrap();
            fs::copy(temporary, &destination).unwrap();
        }
    }

    #[test]
    fn v4_delta_append_delete_binary_carry_reopens_without_resurrection() {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        let (base, _) = stage_v4_ordinal_artifacts(
            [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 2_u64)],
            1,
            &index,
            || false,
        )
        .unwrap();
        fs::write(
            index_path.join(V4_ORDINAL_MANIFEST),
            serde_json::to_vec(&base).unwrap(),
        )
        .unwrap();
        fs::write(index_path.join("ordinal-v4.lock"), []).unwrap();

        let pinned = pinned_v4_update(root.path(), base);
        let mut second = crate::staging::RewriteBatch::new();
        let planned_second = prepare_v4_ordinal_delta(
            root.path(),
            1,
            2,
            &pinned,
            &mut second,
            &[(Uuid::from_u128(3), 3_u64), (Uuid::from_u128(4), 4_u64)],
            &[1],
            &"11".repeat(32),
        )
        .unwrap();
        assert_eq!(planned_second.metrics.input_identities, 2);
        assert_eq!(planned_second.metrics.input_tombstones, 1);
        assert_eq!(planned_second.metrics.prior_topology_rows_decoded, 0);
        assert_eq!(planned_second.metrics.per_record_seeks, 0);
        assert_eq!(planned_second.metrics.compactions, 0);
        install_v4_plan(&second);

        let pinned = pinned_v4_update(root.path(), planned_second.manifest);
        let mut third = crate::staging::RewriteBatch::new();
        let planned_third = prepare_v4_ordinal_delta(
            root.path(),
            2,
            3,
            &pinned,
            &mut third,
            &[(Uuid::from_u128(5), 5_u64)],
            &[2],
            &"22".repeat(32),
        )
        .unwrap();
        assert_eq!(planned_third.metrics.compactions, 1);
        assert_eq!(
            planned_third.metrics.peak_configured_cache_window_bytes,
            graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
        assert!(
            planned_third.metrics.cache_release.peak_window_bytes
                <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
        #[cfg(target_os = "linux")]
        assert!(planned_third.metrics.cache_release.release_operations > 0);
        assert_eq!(planned_third.manifest.forward_identities.len(), 2);
        assert_eq!(planned_third.manifest.ordinal_ranges.len(), 2);
        assert_eq!(planned_third.manifest.tombstones.len(), 2);
        assert_eq!(
            planned_third.metrics.peak_buffer_bytes,
            crate::staging::STAGE_FILE_BLOCK_BYTES
        );
        install_v4_plan(&third);

        let manifest_bytes = serde_json::to_vec(&planned_third.manifest).unwrap();
        fs::write(index_path.join(V4_ORDINAL_MANIFEST), &manifest_bytes).unwrap();
        let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
            topology_generation: 3,
            manifest_sha256: hex_sha256(&manifest_bytes),
        };
        let mut handle = match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
            root.path(),
            &authority,
            crate::V4OrdinalIdentityLimits::default(),
        )
        .unwrap()
        {
            crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
            crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => panic!("v4 expected"),
        };
        let lookup = handle.lookup_node_uuids(&[1, 2, 3, 4, 5]).unwrap();
        assert_eq!(
            lookup.values,
            vec![
                None,
                None,
                Some(Uuid::from_u128(3)),
                Some(Uuid::from_u128(4)),
                Some(Uuid::from_u128(5)),
            ]
        );
    }

    #[test]
    fn v4_delta_rejects_reuse_and_invalid_tombstones_before_staging() {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        let (base, _) =
            stage_v4_ordinal_artifacts([(Uuid::from_u128(1), 7_u64)], 1, &index, || false).unwrap();
        let pinned = pinned_v4_update(root.path(), base);

        let mut reused_uuid = crate::staging::RewriteBatch::new();
        let reused = prepare_v4_ordinal_delta(
            root.path(),
            1,
            2,
            &pinned,
            &mut reused_uuid,
            &[(Uuid::from_u128(1), 8_u64)],
            &[],
            &"33".repeat(32),
        );
        assert!(reused.is_err(), "prior UUID reuse unexpectedly passed");
        assert!(reused_uuid.is_empty());

        for (nodes, tombstones) in [
            (vec![(Uuid::from_u128(2), 7_u64)], vec![]),
            (vec![(Uuid::from_u128(2), 8_u64)], vec![0]),
            (vec![(Uuid::from_u128(2), 8_u64)], vec![99]),
            (
                vec![(Uuid::from_u128(2), 8_u64), (Uuid::from_u128(2), 9_u64)],
                vec![],
            ),
        ] {
            let mut batch = crate::staging::RewriteBatch::new();
            assert!(
                prepare_v4_ordinal_delta(
                    root.path(),
                    1,
                    2,
                    &pinned,
                    &mut batch,
                    &nodes,
                    &tombstones,
                    &"33".repeat(32),
                )
                .is_err()
            );
            assert!(batch.is_empty());
        }
    }

    #[test]
    fn v4_compaction_fan_in_budget_uses_opened_reader_and_writer_windows() {
        let root = tempfile::tempdir().unwrap();
        let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
        let input_count = 8_usize;
        let window =
            graphforge_filesystem::cache_release_window_for_streams(input_count + 1).unwrap();
        let mut readers = Vec::new();
        for ordinal in 0..input_count {
            let path = root.path().join(format!("input-{ordinal}"));
            std::fs::write(&path, []).unwrap();
            readers.push(
                graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                    File::open(path).unwrap(),
                    window,
                    graphforge_filesystem::FileCacheReleaseTracker::default(),
                )
                .unwrap(),
            );
        }
        let writer = V4TombstoneStreamWriter::new_with_cache_window(&index, 9, window).unwrap();
        let mut windows = readers
            .iter()
            .map(graphforge_filesystem::FileCacheReleasingReader::window_bytes)
            .collect::<Vec<_>>();
        windows.push(writer.artifact.writer.get_ref().window_bytes());

        let aggregate =
            graphforge_filesystem::validate_cache_release_operation_windows(&windows).unwrap();
        assert!(aggregate <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES);
        assert!(windows.iter().all(|configured| configured.get() > 0));
    }

    #[test]
    fn v4_forward_compaction_failure_releases_consumed_input_without_masking_primary() {
        let root = tempfile::tempdir().unwrap();
        let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
        let left_path = root.path().join("left");
        let right_path = root.path().join("right");
        let mut left = Vec::new();
        left.extend_from_slice(Uuid::from_u128(1).as_bytes());
        left.extend_from_slice(&1_u64.to_be_bytes());
        left.push(0xff);
        let mut right = Vec::new();
        right.extend_from_slice(Uuid::from_u128(2).as_bytes());
        right.extend_from_slice(&2_u64.to_be_bytes());
        std::fs::write(&left_path, left).unwrap();
        std::fs::write(&right_path, right).unwrap();
        let mut work = V4CompactionWork::default();

        let error = merge_v4_forward_artifacts(
            File::open(left_path).unwrap(),
            File::open(right_path).unwrap(),
            &index,
            2,
            &mut work,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("truncated"), "{error}");
        assert!(work.peak_configured_cache_window_bytes > 0);
        assert!(
            work.peak_configured_cache_window_bytes
                <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
        assert!(
            index
                .child_names()
                .unwrap()
                .into_iter()
                .all(|name| !name.to_string_lossy().starts_with(".v4-"))
        );
        assert!(work.cache_release.sync_operations > 0);
        #[cfg(target_os = "linux")]
        assert!(work.cache_release.release_operations >= 3);
    }

    fn write_v4_test_artifact(
        index_path: &Path,
        name: &str,
        kind: crate::V4OrdinalArtifactKind,
        generation: u64,
        bytes: &[u8],
    ) -> crate::V4OrdinalArtifact {
        fs::write(index_path.join(name), bytes).unwrap();
        crate::V4OrdinalArtifact {
            name: name.to_owned(),
            kind,
            generation,
            bytes: u64::try_from(bytes.len()).unwrap(),
            sha256: hex_sha256(bytes),
        }
    }

    fn assert_no_v4_temporary(index: &graphforge_filesystem::StableDirectory) {
        assert!(
            index
                .child_names()
                .unwrap()
                .into_iter()
                .all(|name| !name.to_string_lossy().starts_with(".v4-"))
        );
    }

    #[test]
    fn v4_forward_cancellation_finalizes_and_removes_unpublished_output() {
        let root = tempfile::tempdir().unwrap();
        let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
        let left = root.path().join("left");
        let right = root.path().join("right");
        let mut left_bytes = Vec::new();
        left_bytes.extend_from_slice(Uuid::from_u128(1).as_bytes());
        left_bytes.extend_from_slice(&1_u64.to_be_bytes());
        let mut right_bytes = Vec::new();
        right_bytes.extend_from_slice(Uuid::from_u128(2).as_bytes());
        right_bytes.extend_from_slice(&2_u64.to_be_bytes());
        fs::write(&left, left_bytes).unwrap();
        fs::write(&right, right_bytes).unwrap();
        inject_v4_compaction_post_write_failure("forward");
        inject_v4_output_cleanup_failure();
        let mut work = V4CompactionWork::default();

        let error = merge_v4_forward_artifacts(
            File::open(left).unwrap(),
            File::open(right).unwrap(),
            &index,
            2,
            &mut work,
        )
        .unwrap_err()
        .to_string();

        let primary = error
            .find("v4 compaction cancelled after forward output")
            .unwrap();
        let cleanup = error.find("v4 forward output cleanup also failed").unwrap();
        assert!(primary < cleanup, "{error}");
        assert!(
            error.contains("unpublished artifact cleanup finalization failed"),
            "{error}"
        );
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        assert_no_v4_temporary(&index);
        assert!(work.cache_release.sync_operations > 0);
        #[cfg(target_os = "linux")]
        assert!(work.cache_release.release_operations >= 3);

        inject_v4_input_release_failure();
        let mut release_work = V4CompactionWork::default();
        let error = merge_v4_forward_artifacts(
            File::open(root.path().join("left")).unwrap(),
            File::open(root.path().join("right")).unwrap(),
            &index,
            3,
            &mut release_work,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("injected v4 input release failure"),
            "{error}"
        );
        assert_no_v4_temporary(&index);
        assert!(release_work.cache_release.sync_operations > 0);
    }

    #[test]
    fn streamed_v4_publication_guard_covers_setup_and_post_rename_failures() {
        let points = [
            "initial_file_identity",
            "writer_construction",
            "window_validation",
            "fsync_evidence_overflow",
            "final_file_identity",
            "file_space_usage",
            "replace_child",
            "directory_sync",
            "post_publication_metric_overflow",
            "manifest_update",
        ];
        for point in points {
            let root = tempfile::tempdir().unwrap();
            let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
            if matches!(point, "initial_file_identity" | "writer_construction") {
                inject_v4_output_cleanup_failure();
            }
            inject_v4_publication_failure(point);
            let error = stage_v4_ordinal_artifacts([(Uuid::from_u128(1), 1)], 7, &index, || false)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("injected v4 publication failure at {point}")),
                "{error}"
            );
            if matches!(point, "initial_file_identity" | "writer_construction") {
                let primary = error.find("injected v4 publication failure").unwrap();
                let cleanup = error
                    .find("unpublished artifact cleanup finalization failed")
                    .unwrap();
                assert!(primary < cleanup, "{error}");
            }
            assert!(!error.contains(root.path().to_string_lossy().as_ref()));
            let names = index
                .child_names()
                .unwrap()
                .into_iter()
                .filter_map(|name| name.into_string().ok())
                .collect::<Vec<_>>();
            assert!(
                names
                    .iter()
                    .all(|name| !name.starts_with(".v4-") && !name.ends_with(".uuidx")),
                "{point}: {names:?}"
            );
        }
    }

    #[test]
    fn content_addressed_v4_install_reuses_identical_and_preserves_collisions() {
        let root = tempfile::tempdir().unwrap();
        let encoded = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
        let graph = encoded
            .create_child_directory(std::ffi::OsStr::new("graph"))
            .unwrap();
        let topology = graph
            .create_child_directory(std::ffi::OsStr::new("topology"))
            .unwrap();
        let index = topology
            .create_child_directory(std::ffi::OsStr::new("uuid-membership"))
            .unwrap();
        let mappings = [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 2_u64)];
        let (manifest, _) = stage_v4_ordinal_artifacts(mappings, 1, &index, || false).unwrap();
        let names = v4_manifest_artifact_names(&manifest);
        let originals = names
            .iter()
            .map(|name| {
                let file = index.open_child_file(std::ffi::OsStr::new(name)).unwrap();
                (
                    name.clone(),
                    graphforge_filesystem::file_identity(&file).unwrap(),
                    fs::read(
                        root.path()
                            .join("graph/topology/uuid-membership")
                            .join(name),
                    )
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();

        let reused = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false).unwrap();
        assert!(reused.publications.is_empty());
        inject_v4_authority_failure("after_artifacts");
        let error = publish_v4_construction_artifacts(&encoded, reused, 1, &hex_sha256(b"delta"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("injected v4 authority failure at after_artifacts"));
        for (name, identity, bytes) in &originals {
            let file = index.open_child_file(std::ffi::OsStr::new(name)).unwrap();
            assert_eq!(
                graphforge_filesystem::file_identity(&file).unwrap(),
                *identity
            );
            assert_eq!(
                fs::read(
                    root.path()
                        .join("graph/topology/uuid-membership")
                        .join(name)
                )
                .unwrap(),
                *bytes
            );
        }

        let (conflict_name, conflict_identity, _) = &originals[0];
        let conflict = b"occupied conflicting authority";
        fs::write(
            root.path()
                .join("graph/topology/uuid-membership")
                .join(conflict_name),
            conflict,
        )
        .unwrap();
        let error = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("authentication failed"), "{error}");
        let file = index
            .open_child_file(std::ffi::OsStr::new(conflict_name))
            .unwrap();
        assert_eq!(
            graphforge_filesystem::file_identity(&file).unwrap(),
            *conflict_identity
        );
        assert_eq!(
            fs::read(
                root.path()
                    .join("graph/topology/uuid-membership")
                    .join(conflict_name)
            )
            .unwrap(),
            conflict
        );
        assert_no_v4_temporary(&index);
    }

    #[test]
    fn v4_ordinal_cancellation_finalizes_and_removes_unpublished_output() {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        let first_uuid = *Uuid::from_u128(1).as_bytes();
        let second_uuid = *Uuid::from_u128(2).as_bytes();
        let first = write_v4_test_artifact(
            &index_path,
            "ordinal-one.uuidx",
            crate::V4OrdinalArtifactKind::OrdinalUuids,
            1,
            &first_uuid,
        );
        let second = write_v4_test_artifact(
            &index_path,
            "ordinal-two.uuidx",
            crate::V4OrdinalArtifactKind::OrdinalUuids,
            2,
            &second_uuid,
        );
        let mut manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: 2,
            forward_identities: Vec::new(),
            ordinal_ranges: vec![
                crate::V4OrdinalRange {
                    first_node_id: 1,
                    count: 1,
                    artifact: first,
                    blocks: Vec::new(),
                },
                crate::V4OrdinalRange {
                    first_node_id: 2,
                    count: 1,
                    artifact: second,
                    blocks: Vec::new(),
                },
            ],
            tombstones: Vec::new(),
        };
        let pinned = pinned_v4_update(root.path(), manifest.clone());
        let mut created = HashMap::new();
        let mut work = V4CompactionWork::default();
        inject_v4_compaction_post_write_failure("ordinal");

        let error = compact_v4_ordinal_interval(
            &pinned,
            &index,
            &index_path,
            &mut manifest,
            &mut created,
            1,
            2,
            &mut work,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("v4 compaction cancelled after ordinal output"));
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        assert_no_v4_temporary(&index);
        assert!(created.is_empty());
        assert!(work.cache_release.sync_operations > 0);
        #[cfg(target_os = "linux")]
        assert!(work.cache_release.release_operations >= 2);
    }

    #[test]
    fn v4_tombstone_cancellation_finalizes_and_removes_unpublished_output() {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        let first_bytes = 1_u64.to_be_bytes();
        let second_bytes = 2_u64.to_be_bytes();
        let first = write_v4_test_artifact(
            &index_path,
            "tombstone-one.uuidx",
            crate::V4OrdinalArtifactKind::NodeTombstones,
            1,
            &first_bytes,
        );
        let second = write_v4_test_artifact(
            &index_path,
            "tombstone-two.uuidx",
            crate::V4OrdinalArtifactKind::NodeTombstones,
            2,
            &second_bytes,
        );
        let mut manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: 2,
            forward_identities: Vec::new(),
            ordinal_ranges: Vec::new(),
            tombstones: vec![
                crate::V4OrdinalTombstones {
                    generation: 1,
                    artifact: first,
                    blocks: Vec::new(),
                },
                crate::V4OrdinalTombstones {
                    generation: 2,
                    artifact: second,
                    blocks: Vec::new(),
                },
            ],
        };
        let pinned = pinned_v4_update(root.path(), manifest.clone());
        let mut created = HashMap::new();
        let mut work = V4CompactionWork::default();
        inject_v4_compaction_post_write_failure("tombstone");

        let error = compact_v4_tombstone_interval(
            &pinned,
            &index,
            &index_path,
            &mut manifest,
            &mut created,
            1,
            2,
            &mut work,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("v4 compaction cancelled after tombstone output"));
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        assert_no_v4_temporary(&index);
        assert!(created.is_empty());
        assert!(work.cache_release.sync_operations > 0);
        #[cfg(target_os = "linux")]
        {
            assert!(work.cache_release.release_operations >= 2);
            assert!(
                work.cache_release.released_bytes >= 24,
                "{:?}",
                work.cache_release
            );
        }
    }

    #[test]
    fn v4_delta_one_two_four_history_is_bounded_and_binary_carried() {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        const ROWS_PER_GENERATION: u64 = 257;
        let base = (1..=ROWS_PER_GENERATION)
            .map(|id| (Uuid::from_u128(u128::from(id)), id))
            .collect::<Vec<_>>();
        let (mut manifest, _) = stage_v4_ordinal_artifacts(base, 1, &index, || false).unwrap();
        let mut observed = Vec::new();
        let mut next_node_id = ROWS_PER_GENERATION + 1;
        for generation in 2..=5_u64 {
            let pinned = pinned_v4_update(root.path(), manifest);
            let mut batch = crate::staging::RewriteBatch::new();
            let multiplier = 1_u64 << (generation - 2);
            let input_rows = ROWS_PER_GENERATION * multiplier;
            let first = next_node_id;
            let last = first + input_rows - 1;
            next_node_id = last + 1;
            let nodes = (first..=last)
                .map(|id| (Uuid::from_u128(u128::from(id)), id))
                .collect::<Vec<_>>();
            let planned = prepare_v4_ordinal_delta(
                root.path(),
                generation - 1,
                generation,
                &pinned,
                &mut batch,
                &nodes,
                &[],
                &"44".repeat(32),
            )
            .unwrap();
            assert_eq!(planned.metrics.input_identities, input_rows);
            assert_eq!(planned.metrics.prior_topology_rows_decoded, 0);
            assert_eq!(planned.metrics.per_record_seeks, 0);
            assert_eq!(
                planned.metrics.peak_buffer_bytes,
                crate::staging::STAGE_FILE_BLOCK_BYTES
            );
            assert!(planned.metrics.peak_temporary_bytes != 0);
            assert!(planned.metrics.fsync_operations >= 3);
            assert!(planned.metrics.created_artifacts >= 3);
            assert!(planned.metrics.write_blocks != 0);
            assert_eq!(
                planned.metrics.write_bytes,
                planned.metrics.physical_bytes_written
            );
            if planned.metrics.compactions == 0 {
                assert_eq!(planned.metrics.sequential_read_bytes, 0);
                assert_eq!(planned.metrics.sequential_read_calls, 0);
                assert_eq!(planned.metrics.sequential_read_blocks, 0);
            } else {
                assert!(planned.metrics.sequential_read_bytes != 0);
                assert!(planned.metrics.sequential_read_calls != 0);
                assert!(planned.metrics.sequential_read_blocks != 0);
            }
            let control_bytes = planned.auxiliary.bytes
                + u64::try_from(serde_json::to_vec(&planned.manifest).unwrap().len()).unwrap();
            let data_bytes = planned.metrics.physical_bytes_written - control_bytes;
            let new_payload_bytes = input_rows * 40;
            assert!(data_bytes >= new_payload_bytes * 2);
            assert!(
                data_bytes
                    <= (new_payload_bytes + planned.metrics.sequential_read_bytes)
                        .saturating_mul(2)
            );
            assert!(planned.metrics.write_blocks <= data_bytes.div_ceil(16) + 2);
            observed.push((
                planned.metrics.compactions,
                planned.metrics.physical_bytes_written,
                planned.manifest.forward_identities.len(),
            ));
            install_v4_plan(&batch);
            manifest = planned.manifest;
        }
        assert_eq!(
            observed.iter().map(|row| row.0).collect::<Vec<_>>(),
            [0, 1, 0, 2]
        );
        assert_eq!(manifest.forward_identities.len(), 2);
        assert_eq!(manifest.ordinal_ranges.len(), 2);
        assert_eq!(next_node_id - 1, ROWS_PER_GENERATION * 16);
        assert!(observed.iter().all(|row| row.1 != 0));
    }

    #[test]
    fn standalone_existing_v4_advances_with_topology_transaction() {
        let (dir, _, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let topology = dir.path().join("topology/nodes.parquet");
        let mut staged = crate::staging::RewriteBatch::new();
        staged.stage_file(&topology, &topology).unwrap();
        let mut snapshot = None;
        let committed = commit_uuid_topology_rewrite(
            dir.path(),
            staged,
            &UuidTopologyDelta {
                nodes: Vec::new(),
                edges: Vec::new(),
                deleted_nodes: Vec::new(),
                deleted_edges: Vec::new(),
            },
            &mut snapshot,
        )
        .unwrap();
        assert!(matches!(
            committed,
            CommittedUuidTopologyRewrite::Committed { generation: 8, .. }
        ));
        let manifest: crate::V4OrdinalIdentityManifest = serde_json::from_slice(
            &fs::read(dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.topology_generation, 8);
        assert!(
            standalone_v4_pinned_update(dir.path(), 8)
                .unwrap()
                .is_some()
        );
    }

    fn v4_plan_sibling_count(project: &Path) -> usize {
        let root = project.join(INDEX_DIR).join(V4_PLAN_ROOT);
        root.read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(V4_PLAN_PREFIX))
            })
            .count()
    }

    fn run_v4_rewrite_once(project: &Path) {
        let topology = project.join("topology/nodes.parquet");
        let mut staged = crate::staging::RewriteBatch::new();
        staged.stage_file(&topology, &topology).unwrap();
        let mut snapshot = None;
        commit_uuid_topology_rewrite(
            project,
            staged,
            &UuidTopologyDelta {
                nodes: Vec::new(),
                edges: Vec::new(),
                deleted_nodes: Vec::new(),
                deleted_edges: Vec::new(),
            },
            &mut snapshot,
        )
        .unwrap();
    }

    fn assert_exact_v4_reopen(project: &Path, generation: u64) {
        assert_exact_v4_reopen_values(
            project,
            generation,
            vec![
                Some(Uuid::from_u128(3)),
                Some(Uuid::from_u128(1)),
                Some(Uuid::from_u128(2)),
            ],
        );
    }

    fn assert_exact_v4_reopen_values(project: &Path, generation: u64, expected: Vec<Option<Uuid>>) {
        let index = project.join(INDEX_DIR);
        let manifest_bytes = fs::read(index.join(V4_ORDINAL_MANIFEST)).unwrap();
        let receipt: TopologyIndexReceipt =
            serde_json::from_slice(&fs::read(index.join(V4_ORDINAL_RECEIPT)).unwrap()).unwrap();
        assert_eq!(receipt.expected_generation, generation);
        assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
        let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
            topology_generation: generation,
            manifest_sha256: receipt.manifest_sha256,
        };
        let mut handle = match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
            project,
            &authority,
            crate::V4OrdinalIdentityLimits::default(),
        )
        .unwrap()
        {
            crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
            crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => {
                panic!("receipt-authenticated v4 authority unexpectedly requires rebuild")
            }
        };
        let pinned = handle.pinned_update_inputs().unwrap();
        assert_eq!(pinned.manifest.topology_generation, generation);
        assert_eq!(
            pinned.artifacts.len(),
            pinned.manifest.forward_identities.len()
                + pinned.manifest.ordinal_ranges.len()
                + pinned.manifest.tombstones.len()
        );
        assert_eq!(
            handle.lookup_node_uuids(&[1, 2, 3]).unwrap().values,
            expected
        );
    }

    #[test]
    fn v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch() {
        const CHILD_ROOT: &str = "GRAPHFORGE_V4_REWRITE_CHILD_ROOT";
        const RETRY: &str = "GRAPHFORGE_V4_REWRITE_RETRY";
        const TARGET: &str = "GRAPHFORGE_V4_REWRITE_TARGET";
        if let Ok(root) = std::env::var(CHILD_ROOT) {
            let root = Path::new(&root);
            crate::durable_rewrite::recover(root).unwrap();
            let target = std::env::var(TARGET).unwrap().parse::<u64>().unwrap();
            if crate::read_topology_generation(root).unwrap() < target {
                run_v4_rewrite_once(root);
            }
            if std::env::var(RETRY).is_err() {
                panic!("configured v4 rewrite failpoint did not terminate the process");
            }
            return;
        }

        for (failpoint, target) in [
            ("v4_append.after_delta_artifacts", 8),
            ("v4_append.after_receipt_stage", 8),
            ("v4_append.after_manifest_stage", 8),
            ("v4_compaction.after_outputs", 9),
            ("rewrite.before_intent", 8),
            ("rewrite.after_preparing_disarm", 8),
            ("rewrite.after_durable_intent", 8),
        ] {
            let (dir, _, _) = fixture();
            fs::write(
                dir.path().join("topology/generation.json"),
                b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
            )
            .unwrap();
            rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
            rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
            if target == 9 {
                run_v4_rewrite_once(dir.path());
            }

            let crashed = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(
                    "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                )
                .arg("--nocapture")
                .env(CHILD_ROOT, dir.path())
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .env(TARGET, target.to_string())
                .status()
                .unwrap();
            assert_eq!(
                crashed.code(),
                Some(crate::project_failpoint::exit_code()),
                "{failpoint}"
            );

            let retry = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(
                    "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                )
                .arg("--nocapture")
                .env(CHILD_ROOT, dir.path())
                .env(RETRY, "1")
                .env(TARGET, target.to_string())
                .status()
                .unwrap();
            assert!(retry.success(), "{failpoint}");
            assert_eq!(v4_plan_sibling_count(dir.path()), 0, "{failpoint}");
            assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), target);
            assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
            let v4: crate::V4OrdinalIdentityManifest = serde_json::from_slice(
                &fs::read(dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap(),
            )
            .unwrap();
            assert_eq!(v4.topology_generation, target, "{failpoint}");
            assert_eq!(
                receipt_manifest_digest(dir.path()),
                hex_sha256(&fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap())
            );
            assert_exact_v4_reopen(dir.path(), target);
        }

        for cleanup_failpoint in [
            "v4_plan_cleanup.before_unlink",
            "v4_plan_cleanup.after_unlink",
        ] {
            let (dir, _, _) = fixture();
            fs::write(
                dir.path().join("topology/generation.json"),
                b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
            )
            .unwrap();
            rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
            rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();

            let child = |failpoint: &str, retry: bool| {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path())
                    .env(
                        "GRAPHFORGE_PROJECT_FAILPOINTS",
                        "graphforge-internal-subprocess-v1",
                    )
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                    .env(TARGET, "8");
                if retry {
                    command.env(RETRY, "1");
                }
                command.status().unwrap()
            };
            assert_eq!(
                child("v4_append.after_delta_artifacts", false).code(),
                Some(crate::project_failpoint::exit_code())
            );
            assert_eq!(v4_plan_sibling_count(dir.path()), 1);
            assert_eq!(
                child(cleanup_failpoint, false).code(),
                Some(crate::project_failpoint::exit_code()),
                "{cleanup_failpoint}"
            );
            assert!(child("", true).success(), "{cleanup_failpoint}");
            assert_eq!(v4_plan_sibling_count(dir.path()), 0, "{cleanup_failpoint}");
            assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 8);
            assert_exact_v4_reopen(dir.path(), 8);
        }
    }

    #[test]
    fn shared_v4_builder_rejects_sparse_manifest_above_reader_bound() {
        let artifact = |name: String| crate::V4OrdinalArtifact {
            name,
            kind: crate::V4OrdinalArtifactKind::OrdinalUuids,
            generation: 1,
            bytes: 16,
            sha256: "00".repeat(32),
        };
        let ranges = (0..V4_MAX_RANGES)
            .map(|ordinal| crate::V4OrdinalRange {
                first_node_id: u64::try_from(ordinal).unwrap() * 2 + 1,
                count: 1,
                artifact: artifact(format!("ordinal-v4-1-{ordinal:016x}.uuidx")),
                blocks: vec![crate::V4OrdinalBlock {
                    offset: 0,
                    count: 1,
                    sha256: "11".repeat(32),
                }],
            })
            .collect::<Vec<_>>();
        let manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: 1,
            forward_identities: vec![crate::V4OrdinalArtifact {
                name: "forward-v4-1-0000000000000000.uuidx".to_owned(),
                kind: crate::V4OrdinalArtifactKind::ForwardIdentities,
                generation: 1,
                bytes: 24,
                sha256: "22".repeat(32),
            }],
            ordinal_ranges: ranges,
            tombstones: Vec::new(),
        };
        assert!(
            serde_json::to_vec(&manifest).unwrap().len() as u64
                > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES
        );
        assert!(admit_v4_construction_manifest(&manifest).is_err());
    }

    #[test]
    fn cleanup_authenticates_complete_unpublished_v4_facet() {
        let encoded_dir = tempfile::tempdir().unwrap();
        let encoded = graphforge_filesystem::StableDirectory::open(encoded_dir.path()).unwrap();
        let graph = encoded
            .create_child_directory(std::ffi::OsStr::new("graph"))
            .unwrap();
        let topology = graph
            .create_child_directory(std::ffi::OsStr::new("topology"))
            .unwrap();
        let index = topology
            .create_child_directory(std::ffi::OsStr::new("uuid-membership"))
            .unwrap();
        let mappings = [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 3_u64)];
        let bundle = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false).unwrap();
        publish_v4_construction_artifacts(&encoded, bundle, 1, &hex_sha256(b"delta")).unwrap();

        cleanup_private_construction_index(&encoded).unwrap();
        assert!(index.child_names().unwrap().is_empty());
    }

    #[test]
    fn construction_encoder_io_geometry_is_block_bounded() {
        for records in [32_768_u64, 65_536, 131_072] {
            let source_dir = tempfile::tempdir().unwrap();
            let encoded_dir = tempfile::tempdir().unwrap();
            let mut input = BufWriter::with_capacity(
                BULK_IO_BYTES,
                File::create(source_dir.path().join("identities.run")).unwrap(),
            );
            for value in 1..=records {
                input.write_all(&u128::from(value).to_be_bytes()).unwrap();
                input.write_all(&[0]).unwrap();
                input.write_all(&[0; 7]).unwrap();
                input.write_all(&value.to_be_bytes()).unwrap();
            }
            input.flush().unwrap();
            drop(input);
            let source = graphforge_filesystem::StableDirectory::open(source_dir.path()).unwrap();
            let encoded = graphforge_filesystem::StableDirectory::open(encoded_dir.path()).unwrap();
            let source_sha256 =
                hex_sha256(&fs::read(source_dir.path().join("identities.run")).unwrap());
            let result = encode_construction_index(
                &source,
                "identities.run",
                &source_sha256,
                &encoded,
                1,
                0,
                None,
                records,
                0,
                &mut || false,
            )
            .unwrap();
            let identity_blocks = (records * IDENTITY_RECORD_BYTES).div_ceil(BULK_IO_BYTES as u64);
            let surrogate_blocks =
                (records * NODE_LOOKUP_RECORD_BYTES).div_ceil(BULK_IO_BYTES as u64);
            assert!(
                result.read_operations <= 2 * identity_blocks + surrogate_blocks + 4,
                "{records}: {} reads",
                result.read_operations
            );
            assert!(
                result.write_operations <= identity_blocks + surrogate_blocks + 4,
                "{records}: {} writes",
                result.write_operations
            );
            assert!(result.peak_buffer_bytes <= 3 * BULK_IO_BYTES as u64);
        }
    }

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

    pub(crate) fn fixture() -> (tempfile::TempDir, Vec<Uuid>, Vec<Uuid>) {
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

    fn install_test_v4_facet(
        project: &Path,
        generation: u64,
        nodes: &[Uuid],
    ) -> (Vec<String>, crate::AuthenticatedV4OrdinalIdentityAuthority) {
        let root = project.join(INDEX_DIR);
        fs::write(root.join("ordinal-v4.lock"), []).unwrap();
        let mappings = nodes.iter().copied().zip(1_u64..).collect::<Vec<_>>();
        let mut forward_mappings = mappings.clone();
        forward_mappings.sort_unstable_by_key(|(uuid, _)| *uuid);
        let forward_bytes = forward_mappings
            .iter()
            .flat_map(|(uuid, id)| uuid.as_bytes().iter().copied().chain(id.to_be_bytes()))
            .collect::<Vec<_>>();
        let ordinal_bytes = mappings
            .iter()
            .flat_map(|(uuid, _)| uuid.as_bytes().iter().copied())
            .collect::<Vec<_>>();
        let forward_digest = hex_sha256(&forward_bytes);
        let ordinal_digest = hex_sha256(&ordinal_bytes);
        let tombstone_bytes = (nodes.len() as u64).to_be_bytes();
        let tombstone_digest = hex_sha256(&tombstone_bytes);
        let forward_name = format!("forward-v4-{generation}-{}.uuidx", &forward_digest[..16]);
        let ordinal_name = format!("ordinal-v4-{generation}-{}.uuidx", &ordinal_digest[..16]);
        let tombstone_name = format!(
            "tombstones-v4-{generation}-{}.uuidx",
            &tombstone_digest[..16]
        );
        fs::write(root.join(&forward_name), &forward_bytes).unwrap();
        fs::write(root.join(&ordinal_name), &ordinal_bytes).unwrap();
        fs::write(root.join(&tombstone_name), tombstone_bytes).unwrap();
        let manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: generation,
            forward_identities: vec![crate::V4OrdinalArtifact {
                name: forward_name.clone(),
                kind: crate::V4OrdinalArtifactKind::ForwardIdentities,
                generation,
                bytes: forward_bytes.len() as u64,
                sha256: forward_digest,
            }],
            ordinal_ranges: vec![crate::V4OrdinalRange {
                first_node_id: 1,
                count: nodes.len() as u64,
                artifact: crate::V4OrdinalArtifact {
                    name: ordinal_name.clone(),
                    kind: crate::V4OrdinalArtifactKind::OrdinalUuids,
                    generation,
                    bytes: ordinal_bytes.len() as u64,
                    sha256: ordinal_digest,
                },
                blocks: vec![crate::V4OrdinalBlock {
                    offset: 0,
                    count: nodes.len() as u64,
                    sha256: hex_sha256(&ordinal_bytes),
                }],
            }],
            tombstones: vec![crate::V4OrdinalTombstones {
                generation,
                artifact: crate::V4OrdinalArtifact {
                    name: tombstone_name.clone(),
                    kind: crate::V4OrdinalArtifactKind::NodeTombstones,
                    generation,
                    bytes: 8,
                    sha256: tombstone_digest.clone(),
                },
                blocks: vec![crate::V4OrdinalTombstoneBlock {
                    offset: 0,
                    count: 1,
                    first: nodes.len() as u64,
                    last: nodes.len() as u64,
                    sha256: tombstone_digest,
                }],
            }],
        };
        let body = serde_json::to_vec(&manifest).unwrap();
        fs::write(root.join(V4_ORDINAL_MANIFEST), &body).unwrap();
        let receipt = TopologyIndexReceipt {
            nonce: Uuid::new_v4().simple().to_string(),
            expected_generation: generation,
            topology_delta_sha256: hex_sha256(b"test-v4-facet"),
            manifest_sha256: hex_sha256(&body),
        };
        fs::write(
            root.join(V4_ORDINAL_RECEIPT),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        let authority = crate::AuthenticatedV4OrdinalIdentityAuthority {
            authority: crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                topology_generation: generation,
                manifest_sha256: hex_sha256(&body),
            },
        };
        (vec![forward_name, ordinal_name, tombstone_name], authority)
    }

    #[test]
    fn orphan_collection_authenticates_and_preserves_union_of_v3_and_v4_facets() {
        let (dir, nodes, edges) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let (selected, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
        let orphan = dir
            .path()
            .join(INDEX_DIR)
            .join("ordinal-v4-7-0000000000000000.uuidx");
        fs::write(&orphan, b"orphan").unwrap();

        let work = maintain_uuid_membership_orphans_with_ordinal_authority(
            dir.path(),
            16,
            Some(&authority),
        )
        .unwrap();
        assert_eq!(work.removed, 1);
        assert!(!orphan.exists());
        for name in selected {
            assert!(dir.path().join(INDEX_DIR).join(name).exists());
        }
        let mut v3 = UuidMembershipIndex::open(dir.path()).unwrap();
        assert_eq!(v3.count(UuidIndexKind::Node), nodes.len() as u64);
        assert_eq!(v3.count(UuidIndexKind::Edge), edges.len() as u64);
        assert_eq!(
            v3.probe(UuidIndexKind::Edge, &[edges[0]]).unwrap().0,
            vec![true]
        );

        let receipt_path = dir.path().join(INDEX_DIR).join(V4_ORDINAL_RECEIPT);
        let manifest_path = dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST);
        let mut manifest: crate::V4OrdinalIdentityManifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest.topology_generation = 8;
        let replacement = serde_json::to_vec(&manifest).unwrap();
        fs::write(&manifest_path, &replacement).unwrap();
        let mut receipt: TopologyIndexReceipt =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        receipt.expected_generation = 8;
        receipt.manifest_sha256 = hex_sha256(&replacement);
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert!(
            maintain_uuid_membership_orphans_with_ordinal_authority(
                dir.path(),
                16,
                Some(&authority)
            )
            .is_err()
        );
    }

    #[test]
    fn v4_orphan_cleanup_subprocess_crash_retry_preserves_authenticated_union() {
        const CHILD_ROOT: &str = "GRAPHFORGE_V4_ORPHAN_CHILD_ROOT";
        const RETRY: &str = "GRAPHFORGE_V4_ORPHAN_RETRY";
        if let Ok(root) = std::env::var(CHILD_ROOT) {
            let root = Path::new(&root);
            let manifest_bytes = fs::read(root.join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap();
            let receipt: TopologyIndexReceipt = serde_json::from_slice(
                &fs::read(root.join(INDEX_DIR).join(V4_ORDINAL_RECEIPT)).unwrap(),
            )
            .unwrap();
            assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
            let authority = crate::AuthenticatedV4OrdinalIdentityAuthority {
                authority: crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                    topology_generation: receipt.expected_generation,
                    manifest_sha256: receipt.manifest_sha256,
                },
            };
            maintain_uuid_membership_orphans_with_ordinal_authority(root, 16, Some(&authority))
                .unwrap();
            if std::env::var(RETRY).is_err() {
                panic!("configured v4 orphan cleanup failpoint did not terminate the process");
            }
            return;
        }

        for failpoint in ["v4_cleanup.before_unlink", "v4_cleanup.after_unlink"] {
            let (dir, nodes, _) = fixture();
            fs::write(
                dir.path().join("topology/generation.json"),
                b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
            )
            .unwrap();
            rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
            let (referenced, _) = install_test_v4_facet(dir.path(), 7, &nodes);
            let orphan = dir
                .path()
                .join(INDEX_DIR)
                .join("forward-v4-7-0000000000000000.uuidx");
            fs::write(&orphan, b"authenticated-orphan-candidate").unwrap();

            let child = |retry: bool| {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_orphan_cleanup_subprocess_crash_retry_preserves_authenticated_union",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path());
                if retry {
                    command.env(RETRY, "1");
                } else {
                    command
                        .env(
                            "GRAPHFORGE_PROJECT_FAILPOINTS",
                            "graphforge-internal-subprocess-v1",
                        )
                        .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint);
                }
                command.status().unwrap()
            };
            assert_eq!(
                child(false).code(),
                Some(crate::project_failpoint::exit_code()),
                "{failpoint}"
            );
            assert!(child(true).success(), "{failpoint}");
            assert!(!orphan.exists(), "{failpoint}");
            for name in &referenced {
                assert!(
                    dir.path().join(INDEX_DIR).join(name).is_file(),
                    "{failpoint}"
                );
            }
            assert_exact_v4_reopen_values(
                dir.path(),
                7,
                vec![Some(Uuid::from_u128(3)), Some(Uuid::from_u128(1)), None],
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn ordinal_orphan_admission_rejects_link_fifo_and_oversized_manifest() {
        use std::os::unix::fs::symlink;

        let (dir, nodes, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let (_, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
        let manifest = dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST);
        let original = fs::read(&manifest).unwrap();
        let replacement = dir.path().join(INDEX_DIR).join("replacement.json");
        fs::write(&replacement, &original).unwrap();

        fs::remove_file(&manifest).unwrap();
        symlink(&replacement, &manifest).unwrap();
        assert!(
            maintain_uuid_membership_orphans_with_ordinal_authority(
                dir.path(),
                16,
                Some(&authority)
            )
            .is_err()
        );
        fs::remove_file(&manifest).unwrap();

        assert!(
            std::process::Command::new("mkfifo")
                .arg(&manifest)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            maintain_uuid_membership_orphans_with_ordinal_authority(
                dir.path(),
                16,
                Some(&authority)
            )
            .is_err()
        );
        fs::remove_file(&manifest).unwrap();

        fs::write(&manifest, vec![b'x'; MAX_MANIFEST_BYTES as usize + 1]).unwrap();
        assert!(
            maintain_uuid_membership_orphans_with_ordinal_authority(
                dir.path(),
                16,
                Some(&authority)
            )
            .is_err()
        );
    }

    fn receipt_manifest_digest(project: &Path) -> String {
        let root = project.join(INDEX_DIR);
        let receipt: TopologyIndexReceipt =
            serde_json::from_slice(&fs::read(root.join(TOPOLOGY_RECEIPT)).unwrap()).unwrap();
        assert_eq!(
            receipt.manifest_sha256,
            hex_sha256(&fs::read(root.join(MANIFEST)).unwrap())
        );
        receipt.manifest_sha256
    }

    fn make_installed_manifest_stale(project: &Path) -> String {
        let path = project.join(INDEX_DIR).join(MANIFEST);
        let mut manifest: Manifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        manifest.live_node_count = manifest.live_node_count.saturating_add(17);
        let body = serde_json::to_vec(&manifest).unwrap();
        fs::write(path, &body).unwrap();
        hex_sha256(&body)
    }

    #[test]
    fn construction_snapshot_streams_live_uuid_authority_in_order() {
        let (dir, nodes, edges) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let mut streamed = Vec::new();
        let (snapshot, work) = open_uuid_construction_snapshot(dir.path(), 0, |identity| {
            streamed.push(identity);
            Ok(())
        })
        .unwrap();

        let mut expected = nodes
            .iter()
            .copied()
            .zip(1_u64..)
            .map(|(uuid, surrogate)| ConstructionUuidIdentity {
                uuid,
                kind: UuidIndexKind::Node,
                surrogate,
            })
            .chain(edges.iter().copied().map(|uuid| ConstructionUuidIdentity {
                uuid,
                kind: UuidIndexKind::Edge,
                surrogate: 0,
            }))
            .collect::<Vec<_>>();
        expected.sort_by_key(|identity| identity.uuid);
        assert_eq!(streamed, expected);
        assert_eq!(work.live_nodes, nodes.len() as u64);
        assert_eq!(work.live_edges, edges.len() as u64);
        assert_eq!(work.max_node_surrogate, nodes.len() as u64);
        assert!(work.authentication_bytes > 0);
        assert!(work.authentication_blocks > 0);
        snapshot.revalidate().unwrap();
    }

    #[test]
    fn forced_rebuild_receipt_binds_newly_staged_manifest() {
        let (dir, _, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let stale_digest = make_installed_manifest_stale(dir.path());
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let installed_digest = receipt_manifest_digest(dir.path());
        assert_ne!(installed_digest, stale_digest);
    }

    #[test]
    fn stale_v3_migration_receipt_survives_crash_roll_forward() {
        const CHILD_ROOT: &str = "GRAPHFORGE_UUID_MIGRATION_CHILD_ROOT";
        if let Ok(root) = std::env::var(CHILD_ROOT) {
            let _ =
                rebuild_uuid_membership_indexes(Path::new(&root), UuidIndexBuildLimits::default());
            panic!("child migration failpoint did not terminate the process");
        }

        let (dir, _, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let stale_digest = make_installed_manifest_stale(dir.path());
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("uuid_membership::tests::stale_v3_migration_receipt_survives_crash_roll_forward")
            .arg("--nocapture")
            .env(CHILD_ROOT, dir.path())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "rewrite.after_durable_intent",
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

        assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 0);
        assert_eq!(crate::read_search_generation(dir.path()).unwrap(), 0);
        let installed_digest = receipt_manifest_digest(dir.path());
        assert_ne!(installed_digest, stale_digest);
        assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
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
        assert_eq!(probe.per_record_seeks, 0);
        assert_eq!(lookup.per_record_seeks, 0);
        assert_eq!(lookup.surrogate_blocks_read, 1);
        assert_eq!(
            (probe.requested, probe.unique_requested, probe.found),
            (3, 2, 1)
        );
    }

    #[test]
    fn probe_work_is_fence_selected_block_merge_not_per_record_seeks() {
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
        assert_eq!(metrics.per_record_seeks, 0);
        assert_eq!(metrics.identity_blocks_read, 1);
        assert_eq!(metrics.file_seeks, metrics.identity_blocks_read);
        assert_eq!(metrics.identity_bytes_read, 8_192 * IDENTITY_RECORD_BYTES);
    }

    #[test]
    fn batch_lookup_restores_duplicates_and_applies_newest_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        let retained = (1_u64..=40_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();
        append_uuid_membership_delta(dir.path(), 1, &retained, &[]).unwrap();
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        append_uuid_membership_delta_with_tombstones(
            dir.path(),
            2,
            &[],
            &[],
            &[(retained[19_999].0, retained[19_999].1)],
            &[],
        )
        .unwrap();

        let present = retained[39_999].0;
        let deleted = retained[19_999].0;
        let missing = Uuid::from_u128(50_000);
        let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
        let (resolved, metrics) = index
            .lookup_node_surrogates(&[present, deleted, present, missing])
            .unwrap();
        assert_eq!(resolved, [Some(40_000), None, Some(40_000), None]);
        assert_eq!(
            (metrics.requested, metrics.unique_requested, metrics.found),
            (4, 3, 1)
        );
        assert_eq!(metrics.per_record_seeks, 0);
        assert!(metrics.identity_blocks_read <= 3);
        assert_eq!(metrics.surrogate_blocks_read, 1);
        assert_eq!(metrics.file_seeks, metrics.identity_blocks_read + 1);
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
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
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
    fn compact_retained_reference_authentication_is_batched_and_linear() {
        let source = tempfile::tempdir().unwrap();
        crate::generation::force_bump_topology_generation_for_test(source.path()).unwrap();
        append_uuid_membership_delta(
            source.path(),
            1,
            &[(Uuid::from_u128(1), 1), (Uuid::from_u128(2), 2)],
            &[Uuid::from_u128(100)],
        )
        .unwrap();
        crate::generation::force_bump_topology_generation_for_test(source.path()).unwrap();
        append_uuid_membership_delta(
            source.path(),
            2,
            &[(Uuid::from_u128(3), 3)],
            &[Uuid::from_u128(101)],
        )
        .unwrap();
        let (inventory, _) = crate::capture_graph_files(source.path()).unwrap();

        let container = tempfile::tempdir().unwrap();
        crate::open_or_initialize_project(container.path()).unwrap();
        let lease = crate::begin_graph_object_publication(container.path()).unwrap();
        let paths = inventory
            .files
            .iter()
            .map(|entry| PathBuf::from(&entry.relative_path))
            .collect::<Vec<_>>();
        crate::append_graph_files_v2(
            &lease,
            source.path(),
            &mut crate::GraphManifestState::empty(),
            &paths,
            &[],
        )
        .unwrap();
        drop(lease);

        let snapshot = AuthenticatedUuidIndexSnapshot::open_from_compact_inventory(
            container.path(),
            &inventory,
            2,
        )
        .unwrap();
        let retained = snapshot
            .manifest
            .runs
            .iter()
            .flat_map(|run| [&run.identities, &run.node_surrogates])
            .map(|record| snapshot.retained_reference(record).unwrap())
            .collect::<Vec<_>>();
        assert!(retained.len() > 2);
        let references = retained
            .iter()
            .map(|reference| ConstructionReferenceAuthentication {
                source_root: &reference.source_root,
                source_root_volume: reference.source_root_volume,
                source_root_file_id: &reference.source_root_file_id,
                source_path: &reference.source_path,
                source_volume: reference.source_volume,
                source_file_id: &reference.source_file_id,
                target_path: &reference.target_path,
                bytes: reference.bytes,
                sha256: &reference.sha256,
                parent_manifest_sha256: &reference.parent_manifest_sha256,
            })
            .collect::<Vec<_>>();
        let work = snapshot
            .authenticate_construction_references(&references)
            .unwrap();
        assert_eq!(
            work.global_revalidation_bytes,
            snapshot.snapshot_authentication_bytes() * 2
        );
        assert_eq!(
            work.referenced_payload_bytes,
            retained
                .iter()
                .map(|reference| reference.bytes)
                .sum::<u64>()
        );

        let victim_reference = retained
            .iter()
            .find(|reference| reference.bytes > 0)
            .expect("one retained UUID payload is non-empty");
        let victim = Path::new(&victim_reference.source_root).join(&victim_reference.source_path);
        let original = fs::read(&victim).unwrap();
        let mut permissions = fs::metadata(&victim).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&victim, permissions).unwrap();
        fs::write(&victim, vec![0_u8; original.len()]).unwrap();
        assert!(
            snapshot
                .authenticate_construction_references(&references)
                .is_err()
        );
        fs::write(&victim, &original).unwrap();
        let mut permissions = fs::metadata(&victim).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&victim, permissions).unwrap();

        let error = snapshot
            .authenticate_construction_references_with(&references, || {
                let mut permissions = fs::metadata(&victim).unwrap().permissions();
                permissions.set_readonly(false);
                fs::set_permissions(&victim, permissions).unwrap();
                fs::write(&victim, vec![0_u8; original.len()]).unwrap();
                let mut permissions = fs::metadata(&victim).unwrap().permissions();
                permissions.set_readonly(true);
                fs::set_permissions(&victim, permissions).unwrap();
            })
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("digest does not match its address"),
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
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
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
            crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
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
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        append_uuid_membership_delta(
            dir.path(),
            1,
            &[(Uuid::from_u128(1), 1)],
            &[Uuid::from_u128(2)],
        )
        .unwrap();
        let manifest_before = fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap();

        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
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

        let reverse = tempfile::tempdir().unwrap();
        crate::generation::force_bump_topology_generation_for_test(reverse.path()).unwrap();
        append_uuid_membership_delta(reverse.path(), 1, &[], &[Uuid::from_u128(9)]).unwrap();
        crate::generation::force_bump_topology_generation_for_test(reverse.path()).unwrap();
        assert!(
            append_uuid_membership_delta(reverse.path(), 2, &[(Uuid::from_u128(9), 9)], &[])
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );
    }

    #[test]
    fn orphan_maintenance_is_bounded_and_preserves_linked_and_live_runs() {
        let dir = tempfile::tempdir().unwrap();
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        append_uuid_membership_delta(
            dir.path(),
            1,
            &[(Uuid::from_u128(1), 1)],
            &[Uuid::from_u128(2)],
        )
        .unwrap();
        let root = dir.path().join(INDEX_DIR);
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        let live = root.join(&manifest.runs[0].identities.name);
        let orphan_one = root.join("identities-v3-orphan-0000000000000001.uuidx");
        let orphan_two = root.join("node-surrogates-v3-orphan-0000000000000002.uuidx");
        fs::copy(&live, &orphan_one).unwrap();
        fs::copy(&live, &orphan_two).unwrap();

        assert!(maintain_uuid_membership_orphans(dir.path(), 1).is_err());

        let first =
            maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 1, None).unwrap();
        assert_eq!(first.candidates, 2);
        assert_eq!(first.removed, 1);
        assert_eq!(first.deferred_limit, 1);
        assert!(live.is_file());
        let second =
            maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 64, None).unwrap();
        assert_eq!(second.removed, 1);
        assert!(live.is_file());
        assert!(!orphan_one.exists());
        assert!(!orphan_two.exists());
    }

    #[test]
    fn bulk_append_validation_uses_sequential_megabyte_blocks_and_zero_random_seeks() {
        let dir = tempfile::tempdir().unwrap();
        let retained = (1_u64..=40_000)
            .map(|value| (Uuid::from_u128(u128::from(value)), value))
            .collect::<Vec<_>>();
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        append_uuid_membership_delta(dir.path(), 1, &retained, &[]).unwrap();

        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
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
        first.commit_unsealed_for_test().unwrap();
        fs::create_dir_all(dir.path().join("topology")).unwrap();
        fs::write(
            crate::generation::generation_path(dir.path()),
            crate::generation::encode_generation_state(1, 1, 0).unwrap(),
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
        install.commit_unsealed_for_test().unwrap();
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

    #[test]
    fn v4_stream_builder_emits_maximal_authenticated_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("index");
        fs::create_dir(&root).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&root).unwrap();
        let records = (1_u128..=4_100)
            .map(|value| {
                let node_id = if value <= 4_096 {
                    value as u64
                } else {
                    value as u64 + 10
                };
                (Uuid::from_u128(value), node_id)
            })
            .collect::<Vec<_>>();
        let (manifest, metrics) =
            stage_v4_ordinal_artifacts(records.clone(), 7, &index, || false).unwrap();

        assert_eq!(metrics.input_records, 4_100);
        assert_eq!(metrics.ranges, 2);
        assert_eq!(metrics.cancellation_polls, 4_100);
        assert_eq!(metrics.peak_buffer_bytes, 3 * V4_ORDINAL_BLOCK_BYTES);
        assert_eq!(metrics.peak_temporary_bytes, 4_100 * (24 + 16));
        assert_eq!(metrics.fsync_operations, 4);
        assert_eq!(manifest.ordinal_ranges[0].first_node_id, 1);
        assert_eq!(manifest.ordinal_ranges[0].count, 4_096);
        assert_eq!(manifest.ordinal_ranges[0].blocks.len(), 1);
        assert_eq!(manifest.ordinal_ranges[0].blocks[0].count, 4_096);
        assert_eq!(manifest.ordinal_ranges[1].first_node_id, 4_107);
        assert_eq!(manifest.ordinal_ranges[1].count, 4);
        assert_eq!(manifest.tombstones.len(), 1);
        assert_eq!(manifest.tombstones[0].artifact.bytes, 0);
        assert_eq!(manifest.tombstones[0].blocks, Vec::new());

        let forward = &manifest.forward_identities[0];
        let forward_bytes = fs::read(root.join(&forward.name)).unwrap();
        assert_eq!(forward_bytes.len(), records.len() * 24);
        assert_eq!(hex_sha256(&forward_bytes), forward.sha256);
        for range in &manifest.ordinal_ranges {
            let bytes = fs::read(root.join(&range.artifact.name)).unwrap();
            assert_eq!(hex_sha256(&bytes), range.artifact.sha256);
            assert_eq!(bytes.len() as u64, range.count * 16);
        }
    }

    #[test]
    fn explicit_v4_rebuild_uses_topology_and_independent_projection_orders() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = [
            Uuid::from_u128(30),
            Uuid::from_u128(10),
            Uuid::from_u128(20),
        ];
        write_node_parquet_with_ids(
            &dir.path().join("topology/nodes.parquet"),
            &nodes,
            &[2, 7, 3],
        );
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();

        let evidence = rebuild_v4_ordinal_identity_with_evidence(
            dir.path(),
            UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap();
        assert_eq!(
            evidence.disposition,
            V4OrdinalRebuildDisposition::CanonicalTopology
        );
        assert_eq!(evidence.topology_generation, 7);
        assert_eq!(evidence.input_identities, 3);
        assert_eq!(evidence.ordinal_ranges, 2);
        assert!(evidence.artifact_bytes >= 3 * (24 + 16));
        assert!(evidence.write_blocks >= 3);
        assert!(evidence.peak_buffer_bytes <= 3 * V4_ORDINAL_BLOCK_BYTES as u64);
        assert_eq!(evidence.build.node_count, 3);
        let root = dir.path().join(INDEX_DIR);
        let manifest_bytes = fs::read(root.join(V4_ORDINAL_MANIFEST)).unwrap();
        let manifest: crate::V4OrdinalIdentityManifest =
            serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(manifest.topology_generation, 7);
        assert_eq!(manifest.ordinal_ranges.len(), 2);
        assert_eq!(manifest.ordinal_ranges[0].first_node_id, 2);
        assert_eq!(manifest.ordinal_ranges[0].count, 2);
        assert_eq!(manifest.ordinal_ranges[1].first_node_id, 7);
        let forward = fs::read(root.join(&manifest.forward_identities[0].name)).unwrap();
        let forward_uuids = forward
            .chunks_exact(24)
            .map(|record| Uuid::from_bytes(record[..16].try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            forward_uuids,
            vec![
                Uuid::from_u128(10),
                Uuid::from_u128(20),
                Uuid::from_u128(30)
            ]
        );
        let receipt: TopologyIndexReceipt =
            serde_json::from_slice(&fs::read(root.join(V4_ORDINAL_RECEIPT)).unwrap()).unwrap();
        assert_eq!(receipt.expected_generation, 7);
        assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
        assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
    }

    #[test]
    fn explicit_v4_rebuild_does_not_trust_corrupt_v3_reverse_state() {
        let (dir, _, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":3,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let root = dir.path().join(INDEX_DIR);
        let v3: Manifest = serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        fs::write(
            root.join(&v3.runs[0].node_surrogates.name),
            b"planted-corrupt-v3-reverse",
        )
        .unwrap();

        let metrics =
            rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        assert_eq!(metrics.node_count, 3);
        assert!(root.join(V4_ORDINAL_MANIFEST).is_file());
        assert!(root.join(V4_ORDINAL_RECEIPT).is_file());
    }

    #[test]
    fn v4_rebuild_subprocess_crash_retry_selects_one_complete_authority() {
        const CHILD_ROOT: &str = "GRAPHFORGE_V4_REBUILD_CHILD_ROOT";
        const RETRY: &str = "GRAPHFORGE_V4_REBUILD_RETRY";
        if let Ok(root) = std::env::var(CHILD_ROOT) {
            let root = Path::new(&root);
            crate::durable_rewrite::recover(root).unwrap();
            if !root.join(INDEX_DIR).join(V4_ORDINAL_MANIFEST).exists() {
                rebuild_v4_ordinal_identity_with_evidence(root, UuidIndexBuildLimits::default())
                    .unwrap();
            }
            if std::env::var(RETRY).is_err() {
                panic!("configured v4 rebuild failpoint did not terminate the process");
            }
            return;
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum PreRecoveryDisposition {
            PriorV3Only,
            IncompleteV4,
            CompleteV4,
        }

        for (failpoint, expected, expected_installed) in [
            (
                "rewrite.before_intent",
                PreRecoveryDisposition::PriorV3Only,
                0,
            ),
            (
                "rewrite.after_preparing_disarm",
                PreRecoveryDisposition::PriorV3Only,
                0,
            ),
            (
                "rewrite.after_durable_intent",
                PreRecoveryDisposition::PriorV3Only,
                0,
            ),
            (
                "rewrite.after_first_data_install",
                PreRecoveryDisposition::IncompleteV4,
                1,
            ),
            (
                "rewrite.after_middle_data_install",
                PreRecoveryDisposition::IncompleteV4,
                2,
            ),
            (
                "rewrite.after_last_data_install",
                PreRecoveryDisposition::IncompleteV4,
                usize::MAX,
            ),
            (
                "rewrite.before_generation_authority",
                PreRecoveryDisposition::IncompleteV4,
                usize::MAX,
            ),
            (
                "rewrite.after_generation_authority",
                PreRecoveryDisposition::CompleteV4,
                usize::MAX,
            ),
        ] {
            let (dir, _, _) = fixture();
            fs::write(
                dir.path().join("topology/generation.json"),
                b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
            )
            .unwrap();
            rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();

            let child = |retry: bool| {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_rebuild_subprocess_crash_retry_selects_one_complete_authority",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path());
                if retry {
                    command.env(RETRY, "1");
                } else {
                    command
                        .env(
                            "GRAPHFORGE_PROJECT_FAILPOINTS",
                            "graphforge-internal-subprocess-v1",
                        )
                        .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint);
                }
                command.status().unwrap()
            };
            assert_eq!(
                child(false).code(),
                Some(crate::project_failpoint::exit_code()),
                "{failpoint}"
            );
            // Inspect the crashed tree before recovery. The prior v3 authority
            // must remain valid at every boundary; v4 is either absent,
            // incomplete and therefore inadmissible, or already complete.
            UuidMembershipIndex::open_at_generation(dir.path(), 7).unwrap();
            let journal_path = dir.path().join(".graphforge-rewrite-v1.json");
            let journal = fs::read(&journal_path)
                .ok()
                .map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).unwrap());
            let entries = journal
                .as_ref()
                .and_then(|value| value.get("entries"))
                .and_then(serde_json::Value::as_array);
            let intended_v4 = entries
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry["class"] == "data"
                        && entry["destination"]
                            .as_str()
                            .is_some_and(|path| path.starts_with(INDEX_DIR))
                })
                .collect::<Vec<_>>();
            let installed_v4 = intended_v4
                .iter()
                .filter(|entry| {
                    let path = dir.path().join(entry["destination"].as_str().unwrap());
                    let expected = entry["temporary_file"].as_str().unwrap();
                    File::open(path)
                        .ok()
                        .and_then(|file| graphforge_filesystem::file_identity(&file).ok())
                        .is_some_and(|identity| {
                            identity
                                .file_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                                == expected
                        })
                })
                .count();
            if expected_installed == usize::MAX {
                assert_eq!(installed_v4, intended_v4.len(), "{failpoint}");
            } else {
                assert_eq!(
                    installed_v4, expected_installed,
                    "{failpoint}: journal={journal:?} intended={intended_v4:?}"
                );
            }
            let generation_selected = entries
                .into_iter()
                .flatten()
                .find(|entry| entry["class"] == "generation_authority")
                .is_some_and(|entry| {
                    let expected = entry["temporary_file"].as_str().unwrap();
                    File::open(dir.path().join("topology/generation.json"))
                        .ok()
                        .and_then(|file| graphforge_filesystem::file_identity(&file).ok())
                        .is_some_and(|identity| {
                            identity
                                .file_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                                == expected
                        })
                });
            let index = dir.path().join(INDEX_DIR);
            let coherent_v4 = match (
                fs::read(index.join(V4_ORDINAL_MANIFEST)).ok(),
                fs::read(index.join(V4_ORDINAL_RECEIPT)).ok(),
            ) {
                (Some(manifest), Some(receipt)) => {
                    let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt).unwrap();
                    assert_eq!(receipt.expected_generation, 7, "{failpoint}");
                    assert_eq!(
                        receipt.manifest_sha256,
                        hex_sha256(&manifest),
                        "{failpoint}"
                    );
                    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                        topology_generation: 7,
                        manifest_sha256: receipt.manifest_sha256,
                    };
                    matches!(
                        crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
                            dir.path(),
                            &authority,
                            crate::V4OrdinalIdentityLimits::default()
                        ),
                        Ok(crate::V4OrdinalIdentityOpen::Ready(_))
                    )
                }
                _ => false,
            };
            let observed = if generation_selected && coherent_v4 {
                PreRecoveryDisposition::CompleteV4
            } else if installed_v4 == 0 {
                PreRecoveryDisposition::PriorV3Only
            } else {
                PreRecoveryDisposition::IncompleteV4
            };
            assert_eq!(observed, expected, "{failpoint}");

            assert!(child(true).success(), "{failpoint}");
            assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
            assert_eq!(manifest_generation(dir.path()).unwrap(), Some(7));
            UuidMembershipIndex::open(dir.path()).unwrap();

            let manifest_bytes = fs::read(index.join(V4_ORDINAL_MANIFEST)).unwrap();
            let receipt_bytes = fs::read(index.join(V4_ORDINAL_RECEIPT)).unwrap();
            let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
            assert_eq!(receipt.expected_generation, 7);
            assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
            let generation = crate::durable_rewrite::GenerationPair {
                topology: 7,
                search: 0,
                property: 0,
            };
            assert_eq!(
                crate::durable_rewrite::reconcile_auxiliary(
                    dir.path(),
                    generation,
                    generation,
                    &crate::AuxiliaryReceipt {
                        kind: "uuid-membership/v4".to_owned(),
                        schema_version: crate::ORDINAL_IDENTITY_V4,
                        path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
                        digest: hex_sha256(&receipt_bytes),
                        bytes: u64::try_from(receipt_bytes.len()).unwrap(),
                    },
                )
                .unwrap(),
                crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed
            );
            let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                topology_generation: 7,
                manifest_sha256: receipt.manifest_sha256,
            };
            assert!(matches!(
                crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
                    dir.path(),
                    &authority,
                    crate::V4OrdinalIdentityLimits::default()
                ),
                Ok(crate::V4OrdinalIdentityOpen::Ready(_))
            ));
        }
    }

    #[test]
    fn v4_rebuild_scratch_accounting_is_exact_linear_and_overflow_bounded() {
        for identities in [1_u64, 2, 4] {
            let dir = tempfile::tempdir().unwrap();
            let uuids = (1..=identities)
                .map(|identity| Uuid::from_u128(u128::from(identity)))
                .collect::<Vec<_>>();
            let node_ids = (1..=identities).collect::<Vec<_>>();
            write_node_parquet_with_ids(
                &dir.path().join("topology/nodes.parquet"),
                &uuids,
                &node_ids,
            );
            fs::write(
                dir.path().join("topology/generation.json"),
                b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
            )
            .unwrap();
            let evidence = rebuild_v4_ordinal_identity_with_evidence(
                dir.path(),
                UuidIndexBuildLimits {
                    scan_batch_rows: 1,
                    run_records: 1,
                    merge_fan_in: 2,
                },
            )
            .unwrap();
            let index = dir.path().join(INDEX_DIR);
            let manifest_bytes = fs::metadata(index.join(V4_ORDINAL_MANIFEST)).unwrap().len();
            let receipt_bytes = fs::metadata(index.join(V4_ORDINAL_RECEIPT)).unwrap().len();
            let with_live_scratch = identities * 48 + evidence.artifact_bytes * 2 + manifest_bytes;
            let after_scratch_release = evidence.artifact_bytes + manifest_bytes + receipt_bytes;
            assert_eq!(
                evidence.peak_temporary_bytes,
                with_live_scratch.max(after_scratch_release)
            );
        }

        let mut projection_overflow = V4RebuildScratchAccounting {
            sorted_projection_bytes: u64::MAX,
            ..Default::default()
        };
        assert!(projection_overflow.register_artifacts(1).is_err());
        let mut artifact_overflow = V4RebuildScratchAccounting {
            retained_staged_bytes: u64::MAX,
            ..Default::default()
        };
        assert!(artifact_overflow.register_staged_artifact(1).is_err());
        let mut control_overflow = V4RebuildScratchAccounting {
            staged_control_bytes: u64::MAX,
            ..Default::default()
        };
        assert!(control_overflow.register_staged_control(1).is_err());
    }

    #[test]
    fn v4_stream_builder_rejects_noncanonical_and_cancelled_input() {
        let dir = tempfile::tempdir().unwrap();
        let root_a = dir.path().join("index-a");
        fs::create_dir(&root_a).unwrap();
        let index_a = graphforge_filesystem::StableDirectory::open(&root_a).unwrap();
        let error = stage_v4_ordinal_artifacts(
            [(Uuid::from_u128(2), 1), (Uuid::from_u128(1), 2)],
            1,
            &index_a,
            || false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not canonical"));

        let root_b = dir.path().join("index-b");
        fs::create_dir(&root_b).unwrap();
        let index_b = graphforge_filesystem::StableDirectory::open(&root_b).unwrap();
        let mut polls = 0;
        let error = stage_v4_ordinal_artifacts(
            [(Uuid::from_u128(1), 1), (Uuid::from_u128(2), 2)],
            1,
            &index_b,
            || {
                polls += 1;
                polls == 2
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn v4_stream_builder_packs_sparse_ids_without_sparse_max_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("index");
        fs::create_dir(&root).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&root).unwrap();
        let (manifest, metrics) = stage_v4_ordinal_artifacts(
            [
                (Uuid::from_u128(1), 1),
                (Uuid::from_u128(2), u64::MAX - 1),
                (Uuid::from_u128(3), u64::MAX),
            ],
            9,
            &index,
            || false,
        )
        .unwrap();
        assert_eq!(manifest.ordinal_ranges.len(), 2);
        assert_eq!(manifest.ordinal_ranges[0].artifact.bytes, 16);
        assert_eq!(manifest.ordinal_ranges[1].artifact.bytes, 32);
        assert_eq!(metrics.artifact_bytes, 3 * 24 + 3 * 16);
        assert_eq!(metrics.peak_temporary_bytes, metrics.artifact_bytes);
        assert_eq!(metrics.fsync_operations, 4);
    }
}
