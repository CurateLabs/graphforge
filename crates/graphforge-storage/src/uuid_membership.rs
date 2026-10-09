//! The ordinal node-identity facet under `topology/uuid-membership/`.
//!
//! The directory name is the published location of the node `node_id -> UUID`
//! authority (ordinal v4). The UUID membership index that once lived beside it
//! (`manifest.json`, `identities-v5-*`, `node-surrogates-v5-*`) is no longer
//! produced or read (#1902): live UUIDs are answered from the published
//! topology Parquet by [`crate::TopologyIdentityProbe`]. Projects that still
//! carry those files open normally and the files are ignored.

use graphforge_core::GfError;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Write as _;
use uuid::Uuid;

mod construction;
mod maintenance;
mod ordinal_artifacts;
mod ordinal_compaction;
mod rebuild;
mod topology_delta;

pub(crate) use construction::clear_private_ordinal_residue;
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
pub use rebuild::rebuild_v4_ordinal_identity;
pub use rebuild::rebuild_v4_ordinal_identity_with_evidence;
pub(crate) use topology_delta::commit_uuid_neutral_topology_rewrite;
pub(crate) use topology_delta::commit_uuid_topology_rewrite;
pub(crate) use topology_delta::prepare_v4_ordinal_delta;

// Private recovery intents evolve independently of the published UUID format.
const BULK_IO_BYTES: usize = 1 << 20;
// Persistent authenticated authority for UUID-to-surrogate resolution. Keeping
// it in the immutable topology generation is what lets writer reopen avoid
// decoding historical topology shards; `.graphforge-cache` is only for data
// that can be discarded and reconstructed without violating that contract.
const INDEX_DIR: &str = "topology/uuid-membership";
const V4_ORDINAL_MANIFEST: &str = "ordinal-v4-manifest.json";
const V4_ORDINAL_RECEIPT: &str = "ordinal-v4-receipt.json";

fn storage_err(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("UUID membership index: {error}"))
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConstructionIndexOutput {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
    pub xxh64: u64,
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

const DEFAULT_ORPHAN_GC_LIMIT: usize = 64;

const MAX_MANIFEST_BYTES: u64 = 1 << 20;

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TopologyIndexReceipt {
    nonce: String,
    expected_generation: u64,
    topology_delta_sha256: String,
    manifest_sha256: String,
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
        probe: crate::UuidProbeMetrics,
        v4_metrics: Option<Box<V4OrdinalAppendMetrics>>,
    },
}

impl CommittedUuidTopologyRewrite {
    /// The topology generation the rewrite committed, if it changed any.
    pub(crate) fn generation(&self) -> Option<u64> {
        match self {
            Self::NoTopologyChange => None,
            Self::Committed { generation, .. } => Some(*generation),
        }
    }
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

#[cfg(test)]
pub(crate) mod tests;
