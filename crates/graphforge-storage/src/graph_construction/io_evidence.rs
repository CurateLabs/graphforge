//! Io evidence for graph construction.

#[cfg(test)]
use super::recovery::SHAPE_CLEANUP_FAILURES;

use super::{
    ArtifactReceipt, AtomicU64, BLOCK_BYTES, BTreeMap, BTreeSet, BufReader, ChunkReader,
    Deserialize, DetailCodec, Digest, File, GfError, GraphConstructionEncoding,
    GraphConstructionEncodingEvidence, GraphConstructionSession, Length, Ordering, OsStr, Path,
    Read, Serialize, Sha256, StableDirectory, Write, file_identity, hex, read_fixed,
    replace_checkpoint_control, shape_publication_io_failure, storage,
};

impl GraphConstructionSession {
    /// Record storage-owned application I/O performed by the facade's
    /// post-publication hydration before the refreshed workspace becomes visible.
    #[doc(hidden)]
    pub fn record_hydration_evidence(
        &mut self,
        hydration: &crate::GraphFilesOpenEvidence,
    ) -> Result<(), GfError> {
        self.checkpoint.evidence.hydration_application_read_bytes = self
            .checkpoint
            .evidence
            .hydration_application_read_bytes
            .checked_add(hydration.application_read_bytes)
            .ok_or_else(|| storage("hydration read byte count overflows"))?;
        self.checkpoint
            .evidence
            .hydration_application_read_operations = self
            .checkpoint
            .evidence
            .hydration_application_read_operations
            .checked_add(hydration.application_read_calls)
            .ok_or_else(|| storage("hydration read operation count overflows"))?;
        self.checkpoint.evidence.hydration_application_write_bytes = self
            .checkpoint
            .evidence
            .hydration_application_write_bytes
            .checked_add(hydration.application_write_bytes)
            .ok_or_else(|| storage("hydration write byte count overflows"))?;
        self.checkpoint
            .evidence
            .hydration_application_write_operations = self
            .checkpoint
            .evidence
            .hydration_application_write_operations
            .checked_add(hydration.application_write_calls)
            .ok_or_else(|| storage("hydration write operation count overflows"))?;
        self.checkpoint.evidence.hydration_fsync_operations = self
            .checkpoint
            .evidence
            .hydration_fsync_operations
            .checked_add(hydration.fsync_calls)
            .ok_or_else(|| storage("hydration fsync count overflows"))?;
        self.checkpoint.evidence.hydration_files_copied = self
            .checkpoint
            .evidence
            .hydration_files_copied
            .checked_add(hydration.files_copied)
            .ok_or_else(|| storage("hydration copied-file count overflows"))?;
        self.checkpoint.evidence.hydration_file_fsync_operations = self
            .checkpoint
            .evidence
            .hydration_file_fsync_operations
            .checked_add(hydration.file_fsync_calls)
            .ok_or_else(|| storage("hydration file barrier count overflows"))?;
        self.checkpoint
            .evidence
            .hydration_directory_fsync_operations = self
            .checkpoint
            .evidence
            .hydration_directory_fsync_operations
            .checked_add(hydration.directory_fsync_calls)
            .ok_or_else(|| storage("hydration directory barrier count overflows"))?;
        replace_checkpoint_control(&self.root, &self.checkpoint)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Measured application I/O and bounded retained-window evidence.
pub struct GraphConstructionEvidence {
    /// All application-observed payload bytes read by the seal phase.
    #[serde(default)]
    pub seal_application_read_bytes: u64,
    /// All application-observed payload bytes read by canonical shaping, including
    /// authentication and merge consumption.
    #[serde(default)]
    pub shape_application_read_bytes: u64,
    /// All application-observed shaped payload bytes read by canonical encoding.
    #[serde(default)]
    pub encode_application_read_bytes: u64,
    /// Actual non-empty reads performed by canonical encoding.
    #[serde(default)]
    pub encode_application_read_operations: u64,
    /// Payload bytes submitted by canonical artifact writers.
    #[serde(default)]
    pub encode_application_write_bytes: u64,
    /// Actual canonical artifact write submissions.
    #[serde(default)]
    pub encode_application_write_operations: u64,
    /// Canonical output writer submissions.
    #[serde(default)]
    pub encode_output_write_operations: u64,
    /// UUID-membership writer submissions, including carry.
    #[serde(default)]
    pub encode_membership_write_operations: u64,
    /// Authenticated source-spool writer submissions.
    #[serde(default)]
    pub encode_source_spool_write_operations: u64,
    /// Ordinal payload writer submissions.
    #[serde(default)]
    pub encode_ordinal_artifact_write_operations: u64,
    /// Ordinal publication-control writer submissions.
    #[serde(default)]
    pub encode_ordinal_publication_write_operations: u64,
    /// Canonical encoding file and directory durability barriers.
    #[serde(default)]
    pub encode_fsync_operations: u64,
    /// Completed canonical objects in the encoded inventory.
    #[serde(default)]
    pub canonical_artifact_objects: u64,
    /// Canonical output file and directory barriers.
    #[serde(default)]
    pub encode_output_fsync_operations: u64,
    /// Authenticated source-spool durability barriers.
    #[serde(default)]
    pub encode_source_spool_fsync_operations: u64,
    /// UUID-membership durability barriers.
    #[serde(default)]
    pub encode_membership_fsync_operations: u64,
    /// Ordinal-index payload and publication durability barriers.
    #[serde(default)]
    pub encode_ordinal_fsync_operations: u64,
    /// Application-observed durable control bytes read immediately before publication.
    #[serde(default)]
    pub publication_application_read_bytes: u64,
    /// Actual non-empty durable-control reads immediately before publication.
    #[serde(default)]
    pub publication_application_read_operations: u64,
    /// Application-observed CAS payload, manifest-install and manifest-path read bytes.
    #[serde(default)]
    pub cas_application_read_bytes: u64,
    /// Actual non-empty CAS payload and manifest source/authentication reads.
    #[serde(default)]
    pub cas_application_read_operations: u64,
    /// Payload and manifest bytes submitted to CAS temporary-object writers.
    #[serde(default)]
    pub cas_application_write_bytes: u64,
    /// Actual CAS temporary-object write submissions.
    #[serde(default)]
    pub cas_application_write_operations: u64,
    /// CAS file and directory durability barriers.
    #[serde(default)]
    pub cas_fsync_operations: u64,
    /// Actual payload, manifest-install and manifest-path-read work components.
    #[serde(default)]
    pub cas_publication_io: crate::graph_object_store::GraphPublicationIo,
    /// Application-observed published payload bytes read while hydrating the workspace.
    #[serde(default)]
    pub hydration_application_read_bytes: u64,
    /// Actual non-empty reads performed during hydration and verification.
    #[serde(default)]
    pub hydration_application_read_operations: u64,
    /// Payload bytes submitted to hydrated workspace writers.
    #[serde(default)]
    pub hydration_application_write_bytes: u64,
    /// Actual hydrated workspace write submissions.
    #[serde(default)]
    pub hydration_application_write_operations: u64,
    /// Hydration file and directory durability barriers.
    #[serde(default)]
    pub hydration_fsync_operations: u64,
    /// Canonical files copied into the private hydrated workspace.
    #[serde(default)]
    pub hydration_files_copied: u64,
    /// Hydration durability barriers completed on materialized files.
    #[serde(default)]
    pub hydration_file_fsync_operations: u64,
    /// Hydration durability barriers completed on containing directories.
    #[serde(default)]
    pub hydration_directory_fsync_operations: u64,
    /// Artifact bytes authenticated during recovery or successor-bound reclamation.
    #[serde(default)]
    pub recovery_application_read_bytes: u64,
    /// Bounded artifact reads used by recovery or successor-bound reclamation.
    #[serde(default)]
    pub recovery_application_read_operations: u64,
    /// Directory and checkpoint synchronization barriers for recovery/reclamation.
    #[serde(default)]
    pub recovery_checkpoint_fsync_operations: u64,
    /// New canonical graph payload bytes emitted by encoding.
    #[serde(default)]
    pub canonical_output_bytes: u64,
    /// Staged artifact bytes plus structurally retained parent payload bytes.
    #[serde(default)]
    pub staged_and_retained_disk_bytes: u64,
    /// Receipt-derived retained construction artifacts by semantic category.
    /// Persisted in the checkpoint so resume never scans the session tree.
    #[serde(default)]
    pub storage_current: BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>,
    /// Independently accumulated receipt and category-scoped identity totals.
    #[serde(default)]
    pub storage_receipt_category_authorities:
        BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>,
    /// Per-category peak allocated bytes observed as receipt-backed staging
    /// artifacts accumulated. These are transient construction bytes, not
    /// committed-generation allocation.
    #[serde(default)]
    pub storage_transient_peak_allocated_bytes: BTreeMap<crate::ArtifactCategory, u64>,
    /// Independently accumulated category-scoped receipt high-water marks.
    #[serde(default)]
    pub storage_receipt_transient_peak_authorities: BTreeMap<crate::ArtifactCategory, u64>,
    /// High-water mark of the union of all simultaneously retained,
    /// receipt-authenticated construction artifacts. Unlike the per-category
    /// diagnostics above, this is a total and categories are never treated as
    /// mutually exclusive.
    #[serde(default)]
    pub storage_transient_peak_total_allocated_bytes: u64,
    /// Exact currently retained construction allocation keyed by authenticated
    /// native `(volume, file-id)` identity. This is persisted with the
    /// checkpoint so lifecycle qualification can union it with other owners
    /// without double counting aliases.
    #[serde(default)]
    pub storage_active_identity_allocated_bytes: BTreeMap<String, u64>,
    /// Writer-owned identity deltas in exact operation order. Unlike the final
    /// active map, this preserves staging/merge/encoding coexistence for files
    /// removed before construction returns.
    #[serde(default)]
    pub storage_allocation_transitions: Vec<crate::StorageAllocationTransition>,
    /// Rows accepted.
    pub input_rows: u64,
    /// Non-replay chunks accepted.
    pub input_batches: u64,
    /// Immutable Parquet shards.
    pub parquet_shards: u64,
    /// Immutable payload artifacts accepted from authenticated chunk receipts.
    #[serde(default)]
    pub immutable_artifacts: u64,
    /// Bytes submitted through measured immutable-artifact writers.
    /// Control-record traffic is deliberately reported by the outer I/O probe.
    pub write_bytes: u64,
    /// Actual submissions by measured immutable-artifact writers.
    pub write_operations: u64,
    /// File and directory durability barriers completed for accepted artifacts,
    /// including synchronized cache-window rollovers before publication.
    pub fsync_operations: u64,
    /// Successful file-level page-cache release boundaries.
    #[serde(default)]
    pub cache_release_operations: u64,
    /// Page-cache release boundaries unsupported by the target platform.
    #[serde(default)]
    pub cache_release_unsupported_operations: u64,
    /// Clean file bytes covered by successful page-cache release requests.
    #[serde(default)]
    pub cache_released_bytes: u64,
    /// Largest synchronized file-cache window between release boundaries.
    #[serde(default)]
    pub peak_cache_release_window_bytes: u64,
    /// Bytes read for independent authentication.
    pub authentication_read_bytes: u64,
    /// Actual bounded authentication reads.
    pub authentication_read_operations: u64,
    /// Parent runtime-catalog bytes read through its retained descriptor.
    pub parent_catalog_read_bytes: u64,
    /// Parent runtime-catalog read operations observed by the bounded reader.
    pub parent_catalog_read_operations: u64,
    /// Retained UUID-index bytes loaded by bounded construction probes.
    pub retained_probe_read_bytes: u64,
    /// One-MiB retained UUID-index cache fills performed by construction probes.
    pub retained_probe_block_loads: u64,
    /// Shaped-output bytes read while constructing its authenticated inventory.
    pub shaped_output_authentication_bytes: u64,
    /// Bounded reads used to construct the shaped-output inventory.
    pub shaped_output_authentication_operations: u64,
    /// Bytes read while validating exact idempotent replays.
    pub replay_validation_read_bytes: u64,
    /// Read operations while validating exact idempotent replays.
    pub replay_validation_read_operations: u64,
    /// Bytes read while revalidating sealed inputs for canonical shaping.
    pub shape_input_validation_read_bytes: u64,
    /// Read operations while revalidating sealed inputs for canonical shaping.
    pub shape_input_validation_read_operations: u64,
    /// Fixed-width run records.
    pub run_records: u64,
    /// Largest retained Arrow row window.
    pub peak_batch_rows: u64,
    /// Largest retained Arrow byte window.
    pub peak_batch_bytes: u64,
    /// Largest fixed-width sort window.
    pub peak_run_records: u64,
    /// Prior topology rows decoded during staging. Invariant: zero.
    pub prior_topology_rows_decoded: u64,
    /// CURRENT transitions during staging. Invariant: zero.
    pub current_transitions: u64,
    /// Exact idempotent replays.
    pub replayed_chunks: u64,
    /// Temporary and final fixed-width records read by canonical shaping.
    pub merge_read_records: u64,
    /// Actual non-empty fixed-run read submissions completed by shaping.
    #[serde(default)]
    pub merge_read_operations: u64,
    /// Temporary and final fixed-width records written by canonical shaping.
    pub merge_written_records: u64,
    /// Actual non-empty fixed-run write submissions completed by shaping.
    #[serde(default)]
    pub merge_write_operations: u64,
    /// External merge groups completed (including intermediate levels).
    pub merge_groups: u64,
    /// Highest number of simultaneously open merge inputs.
    pub peak_merge_inputs: u64,
    /// Exact fixed-width payload bytes read during shaping.
    pub merge_read_bytes: u64,
    /// Exact fixed-width payload bytes written during shaping.
    pub merge_written_bytes: u64,
    /// Logical lower bound on bounded reader refills implied by payload bytes
    /// and the fixed one-MiB window; this is not an observed syscall count.
    pub merge_read_blocks: u64,
    /// Logical lower bound on bounded writer flushes implied by payload bytes
    /// and the fixed one-MiB window; this is not an observed syscall count.
    pub merge_write_blocks: u64,
    /// Number of completed merge levels across shaped domains.
    pub merge_passes: u64,
    /// Largest measured temporary merge footprint.
    pub peak_merge_temporary_bytes: u64,
    /// Currently retained filesystem allocation owned by authenticated
    /// shape/merge artifacts.
    #[serde(default)]
    pub current_merge_temporary_allocated_bytes: u64,
    /// Largest explicitly retained application buffer set during append. This
    /// includes input Arrow buffers, extracted fixed runs, sorted Arrow output,
    /// and a conservative full-batch Parquet encoding window; allocator/RSS is
    /// intentionally measured by the outer process probe.
    pub peak_accounted_live_bytes: u64,
    /// Largest number of merge-source names retained by the online scheduler.
    pub peak_merge_name_slots: u64,
    /// Effective range partitions used by shaping.
    #[serde(default)]
    pub shape_partitions: u64,
    /// Recorded range-partition count requested by the session.
    #[serde(default)]
    pub shape_partition_count: u64,
    /// Identity records observed by the splitter sampling pass.
    #[serde(default)]
    pub splitter_sampled_source_records: u64,
    /// Identity keys retained as splitter sample points.
    #[serde(default)]
    pub splitter_sample_records: u64,
    /// Rows routed into the largest identity partition.
    #[serde(default)]
    pub max_partition_identity_rows: u64,
    /// Total identity rows routed by range partitioning.
    #[serde(default)]
    pub partitioned_identity_rows: u64,
    /// Sorted partition outputs concatenated into shaped artifacts.
    #[serde(default)]
    pub partition_outputs: u64,
    /// Largest number of records materialized for one sorted partition.
    #[serde(default)]
    pub peak_partition_records: u64,
    /// Records emitted through range-partitioned shaped outputs.
    #[serde(default)]
    pub partition_rows: u64,
    /// Largest number of endpoint-window names retained by its online merge.
    pub peak_resolved_endpoint_name_slots: u64,
    /// Largest complete runtime-catalog entry count retained during shaping.
    pub peak_catalog_entries: u64,
    /// Largest complete runtime-catalog identifier payload retained during shaping.
    pub peak_catalog_identifier_bytes: u64,
    /// Largest decoded catalog Arrow batch retained during one streaming pass.
    pub peak_catalog_decoded_batch_bytes: u64,
    /// File and directory durability barriers completed during shaping.
    pub merge_fsync_operations: u64,
    /// Bytes returned by instrumented Parquet range/sequential reads in shaping.
    pub parquet_read_bytes: u64,
    /// Instrumented Parquet read calls in shaping.
    pub parquet_read_operations: u64,
    /// Bytes submitted by instrumented Parquet writers in shaping.
    pub parquet_write_bytes: u64,
    /// Instrumented Parquet writer submissions in shaping.
    pub parquet_write_operations: u64,
}

/// Sanitized current and peak commitments to independent category ledgers.
pub type StorageCategoryAuthorityCommitments = (
    BTreeMap<crate::ArtifactCategory, String>,
    BTreeMap<crate::ArtifactCategory, String>,
);

impl GraphConstructionEvidence {
    /// Identity-free category authorities derived from accepted construction
    /// receipts and the active native-identity union.
    pub fn storage_category_authorities(
        &self,
    ) -> Result<BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>, GfError> {
        if self.storage_current.len() != crate::ArtifactCategory::ALL.len()
            || self.storage_receipt_category_authorities.len() != crate::ArtifactCategory::ALL.len()
            || crate::ArtifactCategory::ALL.iter().any(|category| {
                !self.storage_current.contains_key(category)
                    || !self
                        .storage_receipt_category_authorities
                        .contains_key(category)
            })
            || self.storage_current != self.storage_receipt_category_authorities
        {
            return Err(storage(
                "construction storage category authority is incomplete",
            ));
        }
        let category_allocated = self
            .storage_receipt_category_authorities
            .values()
            .try_fold(0_u64, |total, category| {
                total
                    .checked_add(category.allocated_bytes)
                    .ok_or_else(|| storage("construction category authority overflows"))
            })?;
        let identity_allocated = self
            .storage_active_identity_allocated_bytes
            .values()
            .try_fold(0_u64, |total, allocated| {
                total
                    .checked_add(*allocated)
                    .ok_or_else(|| storage("construction identity authority overflows"))
            })?;
        if category_allocated != identity_allocated {
            return Err(storage(
                "construction category authority differs from identity union",
            ));
        }
        Ok(self.storage_receipt_category_authorities.clone())
    }

    /// Identity-free per-category high-water authorities derived from accepted
    /// construction receipt transitions.
    pub fn storage_transient_peak_authorities(
        &self,
    ) -> Result<BTreeMap<crate::ArtifactCategory, u64>, GfError> {
        if self.storage_transient_peak_allocated_bytes.len() != crate::ArtifactCategory::ALL.len()
            || self.storage_receipt_transient_peak_authorities.len()
                != crate::ArtifactCategory::ALL.len()
            || crate::ArtifactCategory::ALL.iter().any(|category| {
                !self
                    .storage_transient_peak_allocated_bytes
                    .contains_key(category)
                    || !self
                        .storage_receipt_transient_peak_authorities
                        .contains_key(category)
            })
            || self.storage_transient_peak_allocated_bytes
                != self.storage_receipt_transient_peak_authorities
        {
            return Err(storage(
                "construction peak category authority is incomplete",
            ));
        }
        for category in crate::ArtifactCategory::ALL {
            if self.storage_transient_peak_allocated_bytes[&category]
                < self.storage_current[&category].allocated_bytes
            {
                return Err(storage(
                    "construction peak category authority is below current allocation",
                ));
            }
        }
        Ok(self.storage_receipt_transient_peak_authorities.clone())
    }

    /// Sanitized commitments to independent current and peak category authority.
    pub fn storage_category_authority_commitments(
        &self,
        context: &crate::ArtifactCategoryAuthorityContext,
    ) -> Result<StorageCategoryAuthorityCommitments, GfError> {
        let current = self.storage_category_authorities()?;
        let peaks = self.storage_transient_peak_authorities()?;
        Ok((
            current
                .iter()
                .map(|(category, totals)| {
                    (
                        *category,
                        crate::artifact_category_authority_commitment(context, *category, totals),
                    )
                })
                .collect(),
            peaks
                .iter()
                .map(|(category, allocated)| {
                    (
                        *category,
                        crate::artifact_category_peak_authority_commitment(
                            context, *category, *allocated,
                        ),
                    )
                })
                .collect(),
        ))
    }

    /// Build safe context from hidden construction receipt and identity ledgers.
    pub fn storage_category_authority_context(
        &self,
        contract: &str,
        rung: u64,
        generation_sha256: &str,
        owner: &str,
        live_nodes: u64,
        live_edges: u64,
    ) -> Result<crate::ArtifactCategoryAuthorityContext, GfError> {
        self.storage_category_authorities()?;
        if contract.is_empty()
            || generation_sha256.is_empty()
            || owner.is_empty()
            || rung == 0
            || live_nodes == 0
            || live_edges == 0
        {
            return Err(storage(
                "construction category authority context is invalid",
            ));
        }
        let empty_identities = BTreeMap::new();
        let mut category_identity_authorities = crate::ArtifactCategory::ALL
            .into_iter()
            .map(|category| {
                (
                    category,
                    crate::storage_attribution::identity_map_authority_sha256(&empty_identities),
                )
            })
            .collect::<BTreeMap<_, _>>();
        category_identity_authorities.insert(
            crate::ArtifactCategory::ConstructionStaging,
            crate::storage_attribution::identity_map_authority_sha256(
                &self.storage_active_identity_allocated_bytes,
            ),
        );
        Ok(crate::ArtifactCategoryAuthorityContext {
            contract: contract.to_owned(),
            version: 1,
            rung,
            generation_sha256: generation_sha256.to_owned(),
            owner: owner.to_owned(),
            receipt_authority_sha256: crate::storage_attribution::category_map_authority_sha256(
                b"graphforge-construction-receipt-authority-v1\0",
                &self.storage_receipt_category_authorities,
            ),
            native_identity_authority_sha256:
                crate::storage_attribution::identity_map_authority_sha256(
                    &self.storage_active_identity_allocated_bytes,
                ),
            native_category_identity_authority_sha256: category_identity_authorities,
            live_nodes,
            live_edges,
        })
    }

    /// Reconciled application-observed payload/control bytes read across construction.
    pub fn total_application_read_bytes(&self) -> Result<u64, GfError> {
        checked_evidence_sum(
            "total application read bytes",
            0,
            &[
                self.seal_application_read_bytes,
                self.shape_application_read_bytes,
                self.encode_application_read_bytes,
                self.publication_application_read_bytes,
                self.cas_application_read_bytes,
                self.hydration_application_read_bytes,
                self.recovery_application_read_bytes,
            ],
        )
    }
}

pub(super) struct HashingWriter {
    pub(super) inner: graphforge_filesystem::DurableFileCacheWriter,
    pub(super) digest: Sha256,
    pub(super) bytes: u64,
    pub(super) operations: u64,
}

#[derive(Clone, Default)]
pub(crate) struct IoCounter {
    pub(super) bytes: std::sync::Arc<AtomicU64>,
    pub(super) operations: std::sync::Arc<AtomicU64>,
}

impl IoCounter {
    pub(crate) fn account(&self, bytes: usize) {
        if bytes != 0 {
            self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            self.operations.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn add_to(&self, evidence: &mut GraphConstructionEvidence) -> Result<(), GfError> {
        evidence.parquet_read_bytes = evidence
            .parquet_read_bytes
            .checked_add(self.bytes.load(Ordering::Relaxed))
            .ok_or_else(|| storage("Parquet read byte count overflows"))?;
        evidence.parquet_read_operations = evidence
            .parquet_read_operations
            .checked_add(self.operations.load(Ordering::Relaxed))
            .ok_or_else(|| storage("Parquet read operation count overflows"))?;
        Ok(())
    }

    pub(crate) fn values(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.operations.load(Ordering::Relaxed),
        )
    }
}

pub(crate) struct CountingRead<R> {
    pub(super) inner: R,
    pub(super) counter: IoCounter,
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.counter.account(read);
        Ok(read)
    }
}

pub(crate) struct CountingChunkReader<R = File> {
    pub(crate) file: R,
    pub(crate) counter: IoCounter,
    cache_release: graphforge_filesystem::FileCacheReleaseTracker,
    cache_window_bytes: std::num::NonZeroU64,
}

impl<R> CountingChunkReader<R> {
    pub(crate) fn new(file: R, counter: IoCounter) -> Self {
        Self::with_cache_window(
            file,
            counter,
            std::num::NonZeroU64::new(graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES)
                .expect("default cache window is non-zero"),
        )
    }

    pub(crate) fn with_cache_window(
        file: R,
        counter: IoCounter,
        cache_window_bytes: std::num::NonZeroU64,
    ) -> Self {
        Self {
            file,
            counter,
            cache_release: graphforge_filesystem::FileCacheReleaseTracker::default(),
            cache_window_bytes,
        }
    }

    pub(crate) fn cache_release_tracker(&self) -> graphforge_filesystem::FileCacheReleaseTracker {
        self.cache_release.clone()
    }
}

pub(crate) trait ConstructionFileHandle: Send + Sync {
    fn descriptor(&self) -> &File;
    fn length(&self) -> u64;
}

impl ConstructionFileHandle for File {
    fn descriptor(&self) -> &File {
        self
    }

    fn length(&self) -> u64 {
        self.metadata().map_or(0, |metadata| metadata.len())
    }
}

impl ConstructionFileHandle for crate::graph_object_store::AuthenticatedGraphObject {
    fn descriptor(&self) -> &File {
        self.as_ref()
    }

    fn length(&self) -> u64 {
        self.authenticated_length()
    }
}

impl<R: ConstructionFileHandle> Length for CountingChunkReader<R> {
    fn len(&self) -> u64 {
        self.file.length()
    }
}

impl<R: ConstructionFileHandle> ChunkReader for CountingChunkReader<R> {
    type T = CountingRead<BufReader<graphforge_filesystem::FileCacheReleasingReader>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        use std::io::{Seek, SeekFrom};

        let file = self.file.descriptor().try_clone()?;
        let mut file = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
            file,
            self.cache_window_bytes,
            self.cache_release.clone(),
        )?;
        file.seek(SeekFrom::Start(start))?;
        Ok(CountingRead {
            inner: BufReader::with_capacity(BLOCK_BYTES, file),
            counter: self.counter.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        use std::io::{Seek, SeekFrom};

        let file = self.file.descriptor().try_clone()?;
        let mut file = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
            file,
            self.cache_window_bytes,
            self.cache_release.clone(),
        )?;
        file.seek(SeekFrom::Start(start))?;
        let mut value = vec![0_u8; length];
        file.read_exact(&mut value)?;
        file.finish()?;
        self.counter.account(length);
        Ok(bytes::Bytes::from(value))
    }
}

impl HashingWriter {
    pub(super) fn new(inner: File) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(4).map_err(storage)?;
        Self::with_cache_window(inner, cache_window)
    }

    pub(super) fn with_cache_window(
        inner: File,
        cache_window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        Ok(Self {
            inner: graphforge_filesystem::DurableFileCacheWriter::with_window_bytes_checked(
                inner,
                cache_window,
                || shape_publication_io_failure("writer_construction"),
            )
            .map_err(storage)?,
            digest: Sha256::new(),
            bytes: 0,
            operations: 0,
        })
    }
}

impl Write for HashingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("construction writer byte count overflows"))?;
        self.operations = self.operations.checked_add(1).ok_or_else(|| {
            std::io::Error::other("construction writer operation count overflows")
        })?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(super) fn account_cache_release(
    released: graphforge_filesystem::FileCacheReleaseEvidence,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    evidence.cache_release_operations = evidence
        .cache_release_operations
        .checked_add(released.release_operations)
        .ok_or_else(|| storage("cache release operations overflows"))?;
    evidence.cache_release_unsupported_operations = evidence
        .cache_release_unsupported_operations
        .checked_add(released.unsupported_operations)
        .ok_or_else(|| storage("cache release unsupported operations overflows"))?;
    evidence.cache_released_bytes = evidence
        .cache_released_bytes
        .checked_add(released.released_bytes)
        .ok_or_else(|| storage("cache released bytes overflows"))?;
    evidence.peak_cache_release_window_bytes = evidence
        .peak_cache_release_window_bytes
        .max(released.peak_window_bytes);
    Ok(())
}

pub(super) fn account_encoding_cache_release(
    released: &GraphConstructionEncodingEvidence,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    evidence.cache_release_operations = evidence
        .cache_release_operations
        .checked_add(released.cache_release_operations)
        .ok_or_else(|| storage("cache release operations overflows"))?;
    evidence.cache_release_unsupported_operations = evidence
        .cache_release_unsupported_operations
        .checked_add(released.cache_release_unsupported_operations)
        .ok_or_else(|| storage("cache release unsupported operations overflows"))?;
    evidence.cache_released_bytes = evidence
        .cache_released_bytes
        .checked_add(released.cache_released_bytes)
        .ok_or_else(|| storage("cache released bytes overflows"))?;
    evidence.peak_cache_release_window_bytes = evidence
        .peak_cache_release_window_bytes
        .max(released.peak_cache_release_window_bytes);
    Ok(())
}

pub(super) fn copy_post_shape_io(
    target: &mut GraphConstructionEvidence,
    source: &GraphConstructionEvidence,
) {
    target.recovery_application_read_bytes = source.recovery_application_read_bytes;
    target.recovery_application_read_operations = source.recovery_application_read_operations;
    target.recovery_checkpoint_fsync_operations = source.recovery_checkpoint_fsync_operations;
    target.storage_current = source.storage_current.clone();
    target.storage_receipt_category_authorities =
        source.storage_receipt_category_authorities.clone();
    target.storage_transient_peak_allocated_bytes =
        source.storage_transient_peak_allocated_bytes.clone();
    target.storage_receipt_transient_peak_authorities =
        source.storage_receipt_transient_peak_authorities.clone();
    target.storage_transient_peak_total_allocated_bytes =
        source.storage_transient_peak_total_allocated_bytes;
    target.storage_active_identity_allocated_bytes =
        source.storage_active_identity_allocated_bytes.clone();
    target
        .storage_allocation_transitions
        .clone_from(&source.storage_allocation_transitions);
    target.current_merge_temporary_allocated_bytes = source.current_merge_temporary_allocated_bytes;
    target.peak_merge_temporary_bytes = source.peak_merge_temporary_bytes;
    target.cache_release_operations = source.cache_release_operations;
    target.cache_release_unsupported_operations = source.cache_release_unsupported_operations;
    target.cache_released_bytes = source.cache_released_bytes;
    target.peak_cache_release_window_bytes = source.peak_cache_release_window_bytes;
    target.encode_application_read_bytes = source.encode_application_read_bytes;
    target.encode_application_read_operations = source.encode_application_read_operations;
    target.encode_application_write_bytes = source.encode_application_write_bytes;
    target.encode_application_write_operations = source.encode_application_write_operations;
    target.encode_output_write_operations = source.encode_output_write_operations;
    target.encode_membership_write_operations = source.encode_membership_write_operations;
    target.encode_source_spool_write_operations = source.encode_source_spool_write_operations;
    target.encode_ordinal_artifact_write_operations =
        source.encode_ordinal_artifact_write_operations;
    target.encode_ordinal_publication_write_operations =
        source.encode_ordinal_publication_write_operations;
    target.encode_fsync_operations = source.encode_fsync_operations;
    target.canonical_artifact_objects = source.canonical_artifact_objects;
    target.encode_output_fsync_operations = source.encode_output_fsync_operations;
    target.encode_source_spool_fsync_operations = source.encode_source_spool_fsync_operations;
    target.encode_membership_fsync_operations = source.encode_membership_fsync_operations;
    target.encode_ordinal_fsync_operations = source.encode_ordinal_fsync_operations;
    target.publication_application_read_bytes = source.publication_application_read_bytes;
    target.publication_application_read_operations = source.publication_application_read_operations;
    target.cas_application_read_bytes = source.cas_application_read_bytes;
    target.cas_application_read_operations = source.cas_application_read_operations;
    target.cas_application_write_bytes = source.cas_application_write_bytes;
    target.cas_application_write_operations = source.cas_application_write_operations;
    target.cas_fsync_operations = source.cas_fsync_operations;
    target.cas_publication_io = source.cas_publication_io.clone();
    target.hydration_application_read_bytes = source.hydration_application_read_bytes;
    target.hydration_application_read_operations = source.hydration_application_read_operations;
    target.hydration_application_write_bytes = source.hydration_application_write_bytes;
    target.hydration_application_write_operations = source.hydration_application_write_operations;
    target.hydration_fsync_operations = source.hydration_fsync_operations;
    target.hydration_files_copied = source.hydration_files_copied;
    target.hydration_file_fsync_operations = source.hydration_file_fsync_operations;
    target.hydration_directory_fsync_operations = source.hydration_directory_fsync_operations;
    target.canonical_output_bytes = source.canonical_output_bytes;
    target.staged_and_retained_disk_bytes = source.staged_and_retained_disk_bytes;
}

pub(super) fn record_active_identity_install(
    evidence: &mut GraphConstructionEvidence,
    identity: String,
    allocated_bytes: u64,
    duplicate_message: &'static str,
) -> Result<(), GfError> {
    if evidence
        .storage_active_identity_allocated_bytes
        .insert(identity.clone(), allocated_bytes)
        .is_some()
    {
        return Err(storage(duplicate_message));
    }
    evidence
        .storage_allocation_transitions
        .push(crate::StorageAllocationTransition {
            installed: BTreeMap::from([(identity, allocated_bytes)]),
            removed: BTreeSet::new(),
        });
    Ok(())
}

pub(super) fn record_active_identity_remove(
    evidence: &mut GraphConstructionEvidence,
    identity: &str,
) -> Result<u64, GfError> {
    let removed = evidence
        .storage_active_identity_allocated_bytes
        .remove(identity)
        .ok_or_else(|| storage("active construction identity is absent"))?;
    evidence
        .storage_allocation_transitions
        .push(crate::StorageAllocationTransition {
            installed: BTreeMap::new(),
            removed: BTreeSet::from([identity.to_owned()]),
        });
    Ok(removed)
}

fn checked_category_install(
    current: &crate::ArtifactStorageTotals,
    logical_bytes: u64,
    allocated_bytes: u64,
) -> Result<crate::ArtifactStorageTotals, GfError> {
    Ok(crate::ArtifactStorageTotals {
        logical_references: current
            .logical_references
            .checked_add(1)
            .ok_or_else(|| storage("category logical-reference authority overflows"))?,
        logical_bytes: current
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| storage("category logical-byte authority overflows"))?,
        physical_objects: current
            .physical_objects
            .checked_add(1)
            .ok_or_else(|| storage("category physical-object authority overflows"))?,
        physical_logical_bytes: current
            .physical_logical_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| storage("category physical-logical authority overflows"))?,
        allocated_bytes: current
            .allocated_bytes
            .checked_add(allocated_bytes)
            .ok_or_else(|| storage("category allocated-byte authority overflows"))?,
    })
}

pub(super) fn record_category_install(
    evidence: &mut GraphConstructionEvidence,
    logical_bytes: u64,
    allocated_bytes: u64,
) -> Result<(), GfError> {
    let category = crate::ArtifactCategory::ConstructionStaging;
    let reported = checked_category_install(
        &evidence.storage_current[&category],
        logical_bytes,
        allocated_bytes,
    )?;
    let authority = checked_category_install(
        &evidence.storage_receipt_category_authorities[&category],
        logical_bytes,
        allocated_bytes,
    )?;
    evidence.storage_current.insert(category, reported.clone());
    evidence
        .storage_receipt_category_authorities
        .insert(category, authority.clone());
    evidence
        .storage_transient_peak_allocated_bytes
        .entry(category)
        .and_modify(|peak| *peak = (*peak).max(reported.allocated_bytes));
    evidence
        .storage_receipt_transient_peak_authorities
        .entry(category)
        .and_modify(|peak| *peak = (*peak).max(authority.allocated_bytes));
    let active_total = evidence
        .storage_active_identity_allocated_bytes
        .values()
        .try_fold(0_u64, |total, value| total.checked_add(*value))
        .ok_or_else(|| storage("active construction allocation overflows"))?;
    evidence.storage_transient_peak_total_allocated_bytes = evidence
        .storage_transient_peak_total_allocated_bytes
        .max(active_total);
    Ok(())
}

pub(super) fn checked_category_remove(
    current: &crate::ArtifactStorageTotals,
    logical_bytes: u64,
    allocated_bytes: u64,
) -> Result<crate::ArtifactStorageTotals, GfError> {
    Ok(crate::ArtifactStorageTotals {
        logical_references: current
            .logical_references
            .checked_sub(1)
            .ok_or_else(|| storage("category logical-reference authority underflows"))?,
        logical_bytes: current
            .logical_bytes
            .checked_sub(logical_bytes)
            .ok_or_else(|| storage("category logical-byte authority underflows"))?,
        physical_objects: current
            .physical_objects
            .checked_sub(1)
            .ok_or_else(|| storage("category physical-object authority underflows"))?,
        physical_logical_bytes: current
            .physical_logical_bytes
            .checked_sub(logical_bytes)
            .ok_or_else(|| storage("category physical-logical authority underflows"))?,
        allocated_bytes: current
            .allocated_bytes
            .checked_sub(allocated_bytes)
            .ok_or_else(|| storage("category allocated-byte authority underflows"))?,
    })
}

pub(super) fn record_shape_artifact_install(
    evidence: &mut GraphConstructionEvidence,
    receipt: &ArtifactReceipt,
) -> Result<(), GfError> {
    let identity_key = format!(
        "{:016x}:{}",
        receipt.identity.volume_serial, receipt.identity.file_id
    );
    record_active_identity_install(
        evidence,
        identity_key,
        receipt.allocated_bytes,
        "shape artifact identity installed twice",
    )?;
    record_category_install(evidence, receipt.bytes, receipt.allocated_bytes)?;
    evidence.current_merge_temporary_allocated_bytes = evidence
        .current_merge_temporary_allocated_bytes
        .checked_add(receipt.allocated_bytes)
        .ok_or_else(|| storage("current merge allocation overflows"))?;
    evidence.peak_merge_temporary_bytes = evidence
        .peak_merge_temporary_bytes
        .max(evidence.current_merge_temporary_allocated_bytes);
    Ok(())
}

pub(super) fn record_encoding_io_evidence(
    evidence: &mut GraphConstructionEvidence,
    encoded: &GraphConstructionEncoding,
) -> Result<(), GfError> {
    let measured = &encoded.invocation.evidence;
    record_encoding_components(evidence, measured)?;
    evidence.encode_application_read_bytes = checked_evidence_sum(
        "encoding read bytes",
        evidence.encode_application_read_bytes,
        &[measured.input_read_bytes, measured.membership_read_bytes],
    )?;
    evidence.encode_application_read_operations = checked_evidence_sum(
        "encoding read operations",
        evidence.encode_application_read_operations,
        &[
            measured.input_read_operations,
            measured.membership_read_operations,
            measured.source_spool_read_operations,
        ],
    )?;
    evidence.encode_application_write_bytes = checked_evidence_sum(
        "encoding write bytes",
        evidence.encode_application_write_bytes,
        &[
            measured.output_write_bytes,
            measured.membership_total_write_bytes,
            measured.source_spool_write_bytes,
            measured.ordinal_artifact_write_bytes,
            measured.ordinal_publication_write_bytes,
        ],
    )?;
    evidence.encode_application_write_operations = checked_evidence_sum(
        "encoding write operations",
        evidence.encode_application_write_operations,
        &[
            measured.output_write_operations,
            measured.membership_write_operations,
            measured.source_spool_write_operations,
            measured.ordinal_artifact_write_operations,
            measured.ordinal_publication_write_operations,
        ],
    )?;
    evidence.encode_fsync_operations = checked_evidence_sum(
        "encoding fsync operations",
        evidence.encode_fsync_operations,
        &[
            measured.fsync_operations,
            measured.membership_fsync_operations,
            measured.source_spool_fsync_operations,
            measured.ordinal_fsync_operations,
        ],
    )?;
    evidence.canonical_artifact_objects = u64::try_from(encoded.artifacts.len())
        .map_err(|_| storage("canonical artifact inventory exceeds u64"))?;
    evidence.canonical_output_bytes =
        encoded
            .artifacts
            .iter()
            .try_fold(0_u64, |total, artifact| {
                total
                    .checked_add(artifact.bytes)
                    .ok_or_else(|| storage("canonical output inventory overflows"))
            })?;
    let retained_bytes = encoded
        .retained_artifacts
        .iter()
        .try_fold(0_u64, |total, artifact| {
            total
                .checked_add(artifact.bytes)
                .ok_or_else(|| storage("retained artifact inventory overflows"))
        })?;
    evidence.staged_and_retained_disk_bytes = evidence
        .write_bytes
        .checked_add(retained_bytes)
        .ok_or_else(|| storage("staged and retained inventory overflows"))?;
    Ok(())
}

fn record_encoding_components(
    evidence: &mut GraphConstructionEvidence,
    measured: &GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    for (counter, value, name) in [
        (
            &mut evidence.encode_output_write_operations,
            measured.output_write_operations,
            "encode_output_write_operations",
        ),
        (
            &mut evidence.encode_membership_write_operations,
            measured.membership_write_operations,
            "encode_membership_write_operations",
        ),
        (
            &mut evidence.encode_source_spool_write_operations,
            measured.source_spool_write_operations,
            "encode_source_spool_write_operations",
        ),
        (
            &mut evidence.encode_ordinal_artifact_write_operations,
            measured.ordinal_artifact_write_operations,
            "encode_ordinal_artifact_write_operations",
        ),
        (
            &mut evidence.encode_ordinal_publication_write_operations,
            measured.ordinal_publication_write_operations,
            "encode_ordinal_publication_write_operations",
        ),
        (
            &mut evidence.encode_output_fsync_operations,
            measured.fsync_operations,
            "encode_output_fsync_operations",
        ),
        (
            &mut evidence.encode_source_spool_fsync_operations,
            measured.source_spool_fsync_operations,
            "encode_source_spool_fsync_operations",
        ),
        (
            &mut evidence.encode_membership_fsync_operations,
            measured.membership_fsync_operations,
            "encode_membership_fsync_operations",
        ),
        (
            &mut evidence.encode_ordinal_fsync_operations,
            measured.ordinal_fsync_operations,
            "encode_ordinal_fsync_operations",
        ),
    ] {
        *counter = checked_evidence_sum(name, *counter, &[value])?;
    }
    Ok(())
}

pub(super) fn checked_evidence_sum(
    name: &str,
    initial: u64,
    values: &[u64],
) -> Result<u64, GfError> {
    values.iter().try_fold(initial, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| storage(format!("{name} overflow")))
    })
}

pub(super) fn record_encoded_active_artifacts(
    session_root: &StableDirectory,
    encoding: &GraphConstructionEncoding,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let encoded_root = session_root
        .open_child_directory(OsStr::new(&encoding.root))
        .map_err(storage)?
        .open_child_directory(OsStr::new("graph"))
        .map_err(storage)?;
    for artifact in &encoding.artifacts {
        let components = Path::new(&artifact.path)
            .components()
            .map(|component| match component {
                std::path::Component::Normal(value) => Ok(value.to_owned()),
                _ => Err(storage("encoded artifact path is not normalized")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (name, directories) = components
            .split_last()
            .ok_or_else(|| storage("encoded artifact path is empty"))?;
        let mut directory = encoded_root.try_clone().map_err(storage)?;
        for child in directories {
            directory = directory.open_child_directory(child).map_err(storage)?;
        }
        let file = directory.open_child_file(name).map_err(storage)?;
        let identity = file_identity(&file).map_err(storage)?;
        let usage = graphforge_filesystem::file_space_usage(&file).map_err(storage)?;
        if usage.logical_bytes != artifact.bytes {
            return Err(storage("encoded artifact allocation authority changed"));
        }
        let identity_key = format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id));
        if let Some(existing) = evidence
            .storage_active_identity_allocated_bytes
            .get(&identity_key)
        {
            if *existing != usage.allocated_bytes {
                return Err(storage("encoded artifact identity allocation changed"));
            }
            continue;
        }
        record_active_identity_install(
            evidence,
            identity_key,
            usage.allocated_bytes,
            "encoded artifact identity installed twice",
        )?;
        record_category_install(evidence, usage.logical_bytes, usage.allocated_bytes)?;
    }
    Ok(())
}

pub(super) fn account_merge_read<const N: usize>(
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    account_merge_read_bytes(evidence, N as u64)
}

pub(super) fn account_merge_read_bytes(
    evidence: &mut GraphConstructionEvidence,
    _bytes: u64,
) -> Result<(), GfError> {
    evidence.merge_read_records = evidence
        .merge_read_records
        .checked_add(1)
        .ok_or_else(|| storage("merge read record count overflows"))?;
    Ok(())
}

pub(super) fn account_merge_write<const N: usize>(
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    account_merge_write_bytes(evidence, N as u64)
}

pub(super) fn account_merge_write_bytes(
    evidence: &mut GraphConstructionEvidence,
    bytes: u64,
) -> Result<(), GfError> {
    evidence.merge_written_records = evidence
        .merge_written_records
        .checked_add(1)
        .ok_or_else(|| storage("merge written record count overflows"))?;
    evidence.merge_written_bytes = evidence
        .merge_written_bytes
        .checked_add(bytes)
        .ok_or_else(|| storage("merge written byte count overflows"))?;
    Ok(())
}

pub(super) fn account_sequential_read(
    bytes: u64,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if bytes != 0 {
        evidence.merge_read_blocks = evidence
            .merge_read_blocks
            .checked_add(bytes.div_ceil(BLOCK_BYTES as u64))
            .ok_or_else(|| storage("merge read block count overflows"))?;
    }
    Ok(())
}

pub(super) fn account_sequential_write(
    bytes: u64,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if bytes != 0 {
        evidence.merge_write_blocks = evidence
            .merge_write_blocks
            .checked_add(bytes.div_ceil(BLOCK_BYTES as u64))
            .ok_or_else(|| storage("merge write block count overflows"))?;
    }
    Ok(())
}

pub(super) fn account_fixed_read_operations(
    counter: &IoCounter,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let (bytes, operations) = counter.values();
    if (bytes == 0) != (operations == 0) {
        return Err(storage("fixed-run read bytes and submissions disagree"));
    }
    evidence.merge_read_bytes = evidence
        .merge_read_bytes
        .checked_add(bytes)
        .ok_or_else(|| storage("merge read byte count overflows"))?;
    evidence.merge_read_operations = evidence
        .merge_read_operations
        .checked_add(operations)
        .ok_or_else(|| storage("merge read operation count overflows"))?;
    Ok(())
}

pub(super) fn open_counted_fixed_reader(
    root: &StableDirectory,
    name: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<
    (
        BufReader<CountingRead<graphforge_filesystem::FileCacheReleasingReader>>,
        IoCounter,
    ),
    GfError,
> {
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    account_sequential_read(file.metadata().map_err(storage)?.len(), evidence)?;
    let counter = IoCounter::default();
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(4).map_err(storage)?;
    Ok((
        BufReader::with_capacity(
            BLOCK_BYTES,
            CountingRead {
                inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                    file,
                    cache_window,
                    graphforge_filesystem::FileCacheReleaseTracker::default(),
                )
                .map_err(storage)?,
                counter: counter.clone(),
            },
        ),
        counter,
    ))
}

pub(super) fn release_counted_reader_cache(
    reader: &mut BufReader<CountingRead<graphforge_filesystem::FileCacheReleasingReader>>,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let released = reader.get_mut().inner.finish().map_err(storage)?;
    account_cache_release(released, evidence)?;
    #[cfg(test)]
    SHAPE_CLEANUP_FAILURES.with(|failures| {
        if failures.borrow().input_release {
            failures.borrow_mut().input_release = false;
            return Err(storage("injected shape input release failure"));
        }
        Ok(())
    })?;
    Ok(())
}

pub(super) fn combine_cache_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    source: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage(format!(
            "{primary}; {source} cache release also failed: {cleanup}"
        ))),
    }
}

pub(super) fn combine_secondary_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    context: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage(format!(
            "{primary}; {context} also failed: {cleanup}"
        ))),
    }
}

pub(super) fn account_fixed_write_operations(
    receipt: &ArtifactReceipt,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if (receipt.bytes == 0) != (receipt.write_operations == 0) {
        return Err(storage("fixed-run write bytes and submissions disagree"));
    }
    evidence.merge_write_operations = evidence
        .merge_write_operations
        .checked_add(receipt.write_operations)
        .ok_or_else(|| storage("merge write operation count overflows"))?;
    Ok(())
}

pub(super) fn read_run_record<const N: usize>(
    reader: &mut impl Read,
    codec: Option<DetailCodec>,
) -> Result<Option<[u8; N]>, GfError> {
    match codec {
        Some(codec) => codec.read(reader).map_err(storage),
        None => read_fixed(reader),
    }
}

pub(super) fn account_probe_work(
    work: &crate::uuid_membership::UuidProbeMetrics,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    evidence.retained_probe_read_bytes = checked_evidence_sum(
        "retained probe read bytes",
        evidence.retained_probe_read_bytes,
        &[work.identity_bytes_read, work.surrogate_bytes_read],
    )?;
    evidence.retained_probe_block_loads = checked_evidence_sum(
        "retained probe block loads",
        evidence.retained_probe_block_loads,
        &[work.identity_blocks_read, work.surrogate_blocks_read],
    )?;
    Ok(())
}

pub(super) fn merge_cache_release_evidence(
    target: &mut graphforge_filesystem::FileCacheReleaseEvidence,
    source: graphforge_filesystem::FileCacheReleaseEvidence,
) -> Result<(), GfError> {
    target.release_operations = target
        .release_operations
        .checked_add(source.release_operations)
        .ok_or_else(|| storage("release operations overflows"))?;
    target.unsupported_operations = target
        .unsupported_operations
        .checked_add(source.unsupported_operations)
        .ok_or_else(|| storage("unsupported operations overflows"))?;
    target.released_bytes = target
        .released_bytes
        .checked_add(source.released_bytes)
        .ok_or_else(|| storage("released bytes overflows"))?;
    target.peak_window_bytes = target.peak_window_bytes.max(source.peak_window_bytes);
    Ok(())
}

#[cfg(test)]
mod tests;
