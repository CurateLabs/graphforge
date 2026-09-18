//! Range-partitioned shaping: the replacement for the external merge tree.
//!
//! Shaping used to build its global UUID order with a fan-in-32 `BinaryHeap`
//! merge over one run per staged chunk, after copying every staged run once so
//! the merge had an owned input. Both the copies and the tree are gone.
//!
//! Every record is routed once into the partition that owns its key
//! (see [`super::partition`]), each partition is sorted independently, and the
//! partitions are concatenated in index order. Because the partition function
//! is monotone, that concatenation *is* the global order: there is nothing left
//! to merge. Two passes over the data replace `1 + ceil(log_fanin(chunks))`.
//!
//! Fixed-width partitions are loaded and sorted by a bounded worker pool and
//! consumed by the calling thread in partition index order
//! (see [`super::partition_load`]). Workers return their own results and local
//! counters; the shared evidence, the allocation ledger and publication stay
//! with the coordinator, so the worker count is a scheduling decision that
//! changes neither the evidence nor a byte of output. Row partitions still run
//! sequentially.

use super::partition::{PartitionBalance, PartitionPlan};
use super::partition_load::{
    PARTITION_LOAD_WORKERS, PartitionLoadCounters, abandon_if_stopped, consume_in_partition_order,
};
use super::{
    ArtifactReceipt, BLOCK_BYTES, CountingChunkReader, GraphConstructionEvidence, HashingWriter,
    IoCounter, account_cache_release, account_fixed_write_operations, account_merge_read_bytes,
    account_merge_write_bytes, account_sequential_write, artifact_temp,
    cleanup_failed_shape_output, cleanup_shape_publication, combine_cache_cleanup,
    combine_secondary_cleanup, construction_failpoint, hex, injected_input_release_failure,
    open_fixed_reader, persist_shape_receipt, read_run_record, record_shape_artifact_install,
    reject_cancelled, run_record_bytes, sha256, shape_publication_failure,
    shape_publication_io_failure, unlink_shape_artifact, unlink_writer_capability, uuid_column,
    uuid_value,
};
use crate::construction_detail_codec::DetailCodec;
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use arrow::array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    UInt32Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{DataType, SchemaRef};
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, file_identity};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::Digest;
use std::ffi::OsStr;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;

/// Per-spill write buffer for row (Arrow) partition spills. Every partition
/// holds one open spill during the routing pass, so this is multiplied by the
/// partition count: it is deliberately far below `BLOCK_BYTES`, which is sized
/// for the handful of streams a merge group used to open.
const SPILL_BLOCK_BYTES: usize = 64 * 1024;

/// Per-spill write buffer for the one fixed-width family that is routed a
/// record at a time, [`PartitionFamily::Resolved`] (#1443).
///
/// The other four fixed-width families arrive sorted by the key they are
/// routed by, so [`FixedRangePartitioner::route_slice`] receives whole
/// contiguous same-partition runs (kilobytes each at the ladder's chunk
/// shape) and needs no buffer behind it. Resolved endpoints are re-keyed by
/// edge UUID away from their node-UUID input order, so consecutive records
/// land in unrelated partitions and every one of them reached the
/// descriptor as its own `write(2)`: 33.6M calls for 840 MB at S20, against
/// 93k for the whole shaping pass before #1440 removed the buffers, and the
/// measured 23% ingest throughput regression that removal cost. This buffer
/// restores the batching for exactly that family.
///
/// Sized so the bound is invisible in resident memory even at the partition
/// ceiling: it is allocated lazily per open spill, only the Resolved family
/// holds spills open while it is live (the staged families are sealed and
/// finished before resolution starts), and 256 partitions cost 2 MiB at this
/// size. 8 KiB already cuts the S20 call count to ~100k, so raising it buys
/// nothing measurable (see the #1443 curve).
const RESOLVED_SPILL_BUFFER_BYTES: usize = 8 * 1024;

/// Canonical fixed-width partition families. The family name is part of the
/// durable artifact grammar, so it is a closed set rather than a free string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartitionFamily {
    Identities,
    NodeDetails,
    EdgeDetails,
    Endpoints,
    Resolved,
}

impl PartitionFamily {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Identities => "identities",
            Self::NodeDetails => "node-details",
            Self::EdgeDetails => "edge-details",
            Self::Endpoints => "endpoints",
            Self::Resolved => "resolved",
        }
    }

    /// Bound on the per-partition write buffer a spill of this family holds
    /// while open; zero writes every routed slice straight to the descriptor.
    /// See [`RESOLVED_SPILL_BUFFER_BYTES`] for why only one family has one.
    pub(super) const fn spill_buffer_bytes(self) -> usize {
        match self {
            Self::Resolved => RESOLVED_SPILL_BUFFER_BYTES,
            Self::Identities | Self::NodeDetails | Self::EdgeDetails | Self::Endpoints => 0,
        }
    }

    /// Every fixed-width family name, for the durable-name grammar.
    pub(super) const ALL: [Self; 5] = [
        Self::Identities,
        Self::NodeDetails,
        Self::EdgeDetails,
        Self::Endpoints,
        Self::Resolved,
    ];
}

/// Durable name of one fixed-width partition spill.
pub(super) fn fixed_spill_name(family: PartitionFamily, partition: usize) -> String {
    format!("part-{}-p{partition:05}.run", family.as_str())
}

/// Durable name of one row partition spill.
pub(super) fn row_spill_name(namespace: &str, partition: usize) -> String {
    format!("part-rows-{namespace}-p{partition:05}.arrow")
}

/// Whether `name` is a per-partition spill in the durable shaping grammar.
pub(super) fn is_partition_artifact_name(name: &str) -> bool {
    let is_partition_index =
        |tail: &str| tail.len() == 5 && tail.bytes().all(|byte| byte.is_ascii_digit());
    for family in PartitionFamily::ALL {
        if let Some(tail) = name
            .strip_prefix("part-")
            .and_then(|body| body.strip_prefix(family.as_str()))
            .and_then(|body| body.strip_prefix("-p"))
            .and_then(|body| body.strip_suffix(".run"))
        {
            return is_partition_index(tail);
        }
    }
    if let Some(body) = name
        .strip_prefix("part-rows-")
        .and_then(|body| body.strip_suffix(".arrow"))
    {
        return body.split_once("-p").is_some_and(|(namespace, tail)| {
            namespace.len() == 16
                && super::is_canonical_lower_hex(namespace, 16)
                && is_partition_index(tail)
        });
    }
    false
}

/// The sort key column of a normalized construction row batch.
///
/// The staged schema puts the identity UUID first; its name is domain-defined,
/// so it is taken from the schema rather than assumed.
fn key_column(batch: &RecordBatch) -> Result<&arrow::array::FixedSizeBinaryArray, GfError> {
    let schema = batch.schema();
    uuid_column(batch, schema.field(0).name())
}

/// One open, unsealed partition spill.
struct SpillWriter {
    name: String,
    temporary: std::ffi::OsString,
    identity: FileIdentity,
    writer: BufWriter<HashingWriter>,
}

impl SpillWriter {
    /// Constructed only from an already-sealed [`RowSpill`]'s buffered
    /// writer (`RowRangePartitioner::seal`): row spills still buffer their
    /// writes, because the Arrow IPC `StreamWriter` they hold does not offer
    /// the record-boundary information [`FixedRangePartitioner::route_slice`]
    /// needs to batch by contiguous partition run the way fixed-width
    /// families do (#1439 follow-up). `create`/`write`/`abandon` accordingly
    /// have no callers left after that change and are not reintroduced here.
    fn seal(
        mut self,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<ArtifactReceipt, GfError> {
        self.writer.flush().map_err(super::storage)?;
        self.writer
            .get_mut()
            .inner
            .sync_all_and_release()
            .map_err(super::storage)?;
        let cache_release = self.writer.get_ref().inner.evidence();
        account_cache_release(cache_release, evidence)?;
        account_sequential_write(self.writer.get_ref().bytes, evidence)?;
        let receipt = ArtifactReceipt {
            name: self.name.clone(),
            bytes: self.writer.get_ref().bytes,
            allocated_bytes: graphforge_filesystem::file_space_usage(
                self.writer.get_ref().inner.file(),
            )
            .map_err(super::storage)?
            .allocated_bytes,
            sha256: hex(&self.writer.get_ref().digest.clone().finalize()),
            xxh64: crate::corruption_checksum::hex(self.writer.get_ref().checksum.finish()),
            identity: self.identity.into(),
            write_operations: self.writer.get_ref().operations,
            fsync_operations: cache_release
                .sync_operations
                .checked_add(1)
                .ok_or_else(|| super::storage("artifact synchronization count overflows"))?,
        };
        drop(self.writer);
        root.install_child(
            self.temporary.as_os_str(),
            self.identity,
            OsStr::new(&self.name),
        )
        .map_err(super::storage)?;
        root.sync().map_err(super::storage)?;
        construction_failpoint("shape.partition_spill.after_install");
        persist_shape_receipt(root, &receipt)?;
        record_shape_artifact_install(evidence, &receipt)?;
        account_fixed_write_operations(&receipt, evidence)?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
        Ok(receipt)
    }
}

/// One open, unsealed fixed-width partition spill (#1439 follow-up, #1443).
///
/// Unlike [`SpillWriter`], this holds no fixed-size per-partition block.
/// Every partition open at once during routing used to cost
/// `SPILL_BLOCK_BYTES` (64 KiB) of resident buffer regardless of how much of
/// it had actually been written; concurrent spill buffers across partitions
/// and families were the dominant measured term in both the RSS and fsync
/// growth #1439 originally investigated. The staged runs routed through this
/// are UUID-sorted and the partition function is monotone, so the caller
/// accumulates one contiguous same-partition run of wire bytes and writes it
/// in a single call -- see [`FixedRangePartitioner::route_slice`] -- and
/// those families set `bound` to zero.
///
/// The one family whose input order does not match its routing key,
/// [`PartitionFamily::Resolved`], sets a small `bound` instead: slices
/// shorter than it are coalesced in `buffer` (grown lazily, never past the
/// bound) and reach the descriptor in bound-sized writes; a slice at least
/// as long as the bound bypasses the buffer. This is the batching the 64 KiB
/// block used to provide, at a fraction of its residency.
struct FixedSpillWriter {
    name: String,
    temporary: std::ffi::OsString,
    identity: FileIdentity,
    writer: HashingWriter,
    buffer: Vec<u8>,
    bound: usize,
}

impl FixedSpillWriter {
    fn create(
        root: &StableDirectory,
        name: String,
        window: std::num::NonZeroU64,
        bound: usize,
    ) -> Result<Self, GfError> {
        let temporary = artifact_temp(&name);
        let file = root
            .create_replaceable_child_file(temporary.as_os_str())
            .map_err(super::storage)?;
        let identity = file_identity(&file).map_err(super::storage)?;
        let writer = HashingWriter::with_cache_window(file, window)?;
        Ok(Self {
            name,
            temporary,
            identity,
            writer,
            buffer: Vec::new(),
            bound,
        })
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), GfError> {
        if self.bound == 0 {
            return self.writer.write_all(bytes).map_err(super::storage);
        }
        if self.buffer.len() + bytes.len() > self.bound {
            self.flush_buffer()?;
        }
        if bytes.len() >= self.bound {
            self.writer.write_all(bytes).map_err(super::storage)
        } else {
            self.buffer.extend_from_slice(bytes);
            Ok(())
        }
    }

    fn flush_buffer(&mut self) -> Result<(), GfError> {
        if !self.buffer.is_empty() {
            self.writer
                .write_all(&self.buffer)
                .map_err(super::storage)?;
            self.buffer.clear();
        }
        Ok(())
    }

    /// Drop an unsealed spill and remove its temporary. See
    /// [`SpillWriter::abandon`]: same contract, same reason.
    fn abandon(self, root: &StableDirectory) {
        let Self {
            temporary,
            identity,
            writer,
            ..
        } = self;
        drop(writer);
        let _ = root.unlink_child_if_identity(temporary.as_os_str(), identity);
        let _ = root.sync();
    }

    fn seal(
        mut self,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<ArtifactReceipt, GfError> {
        self.flush_buffer()?;
        self.writer.flush().map_err(super::storage)?;
        self.writer
            .inner
            .sync_all_and_release()
            .map_err(super::storage)?;
        let cache_release = self.writer.inner.evidence();
        account_cache_release(cache_release, evidence)?;
        account_sequential_write(self.writer.bytes, evidence)?;
        let receipt = ArtifactReceipt {
            name: self.name.clone(),
            bytes: self.writer.bytes,
            allocated_bytes: graphforge_filesystem::file_space_usage(self.writer.inner.file())
                .map_err(super::storage)?
                .allocated_bytes,
            sha256: hex(&self.writer.digest.clone().finalize()),
            xxh64: crate::corruption_checksum::hex(self.writer.checksum.finish()),
            identity: self.identity.into(),
            write_operations: self.writer.operations,
            fsync_operations: cache_release
                .sync_operations
                .checked_add(1)
                .ok_or_else(|| super::storage("artifact synchronization count overflows"))?,
        };
        drop(self.writer);
        root.install_child(
            self.temporary.as_os_str(),
            self.identity,
            OsStr::new(&self.name),
        )
        .map_err(super::storage)?;
        root.sync().map_err(super::storage)?;
        construction_failpoint("shape.partition_spill.after_install");
        persist_shape_receipt(root, &receipt)?;
        record_shape_artifact_install(evidence, &receipt)?;
        account_fixed_write_operations(&receipt, evidence)?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
        Ok(receipt)
    }
}

/// Routes fixed-width records into per-partition spills, then emits one sorted
/// artifact by sorting each partition and concatenating them in index order.
pub(super) struct FixedRangePartitioner<'a, const N: usize> {
    root: &'a StableDirectory,
    family: PartitionFamily,
    codec: Option<DetailCodec>,
    reject_duplicates: bool,
    window: std::num::NonZeroU64,
    spills: Vec<Option<FixedSpillWriter>>,
    sealed: Vec<Option<ArtifactReceipt>>,
    balance: PartitionBalance,
    records: u64,
    /// Concurrent partition loads while finishing; see
    /// [`super::partition_load::consume_in_partition_order`] for the bound.
    load_workers: NonZeroUsize,
}

impl<const N: usize> Drop for FixedRangePartitioner<'_, N> {
    fn drop(&mut self) {
        for slot in &mut self.spills {
            if let Some(spill) = slot.take() {
                spill.abandon(self.root);
            }
        }
    }
}

impl<'a, const N: usize> FixedRangePartitioner<'a, N> {
    pub(super) fn new(
        root: &'a StableDirectory,
        family: PartitionFamily,
        partitions: usize,
        codec: Option<DetailCodec>,
        reject_duplicates: bool,
    ) -> Result<Self, GfError> {
        let streams = partitions
            .checked_add(1)
            .ok_or_else(|| super::storage("partition stream count overflow"))?;
        let window = graphforge_filesystem::cache_release_window_for_streams(streams)
            .map_err(super::storage)?;
        Ok(Self {
            root,
            family,
            codec,
            reject_duplicates,
            window,
            spills: (0..partitions).map(|_| None).collect(),
            sealed: (0..partitions).map(|_| None).collect(),
            balance: PartitionBalance::new(partitions),
            records: 0,
            load_workers: PARTITION_LOAD_WORKERS,
        })
    }

    /// Force the finish-time worker count. Scheduling only: the tests hold the
    /// evidence and output bytes equal across every value.
    #[cfg(test)]
    pub(super) fn with_load_workers(mut self, workers: NonZeroUsize) -> Self {
        self.load_workers = workers;
        self
    }

    /// Route one record into the partition owning `key`.
    ///
    /// Used only where the caller cannot establish a contiguous
    /// same-partition run to batch (the post-resolution "Resolved" family is
    /// re-keyed away from its input's sort order, so consecutive records are
    /// not generally co-partitioned). Prefer [`Self::route_slice`] wherever
    /// the input is sorted by the same key it is routed by.
    pub(super) fn route(
        &mut self,
        plan: &PartitionPlan,
        key: &[u8; 16],
        record: &[u8; N],
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let partition = plan.partition_of(key);
        let wire = run_record_bytes(record, self.codec)?;
        self.route_slice(partition, wire, 1, evidence)
    }

    /// Route one contiguous same-partition slice of wire bytes in a single
    /// write (#1439 follow-up).
    ///
    /// `records` is the number of records the slice holds, charged to the
    /// balance in one step. Callers whose input is sorted by the same key
    /// they route by (identities, node/edge details, staged endpoints) can
    /// accumulate a run of consecutive same-partition records and flush it
    /// here instead of writing once per record through a held-open buffer.
    pub(super) fn route_slice(
        &mut self,
        partition: usize,
        wire: &[u8],
        records: u64,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        if records == 0 {
            debug_assert!(wire.is_empty());
            return Ok(());
        }
        let slot = self
            .spills
            .get_mut(partition)
            .ok_or_else(|| super::storage("routed partition is out of range"))?;
        if slot.is_none() {
            *slot = Some(FixedSpillWriter::create(
                self.root,
                fixed_spill_name(self.family, partition),
                self.window,
                self.family.spill_buffer_bytes(),
            )?);
        }
        slot.as_mut()
            .ok_or_else(|| super::storage("partition spill is absent"))?
            .write(wire)?;
        account_merge_read_bytes(evidence, wire.len() as u64)?;
        account_merge_write_bytes(evidence, wire.len() as u64)?;
        self.balance.record_many(partition, records)?;
        self.records = self
            .records
            .checked_add(records)
            .ok_or_else(|| super::storage("partition record count overflows"))?;
        Ok(())
    }

    /// Measured per-partition row counts.
    pub(super) fn balance(&self) -> &PartitionBalance {
        &self.balance
    }

    /// Flush, fsync and install every open spill.
    pub(super) fn seal(&mut self, evidence: &mut GraphConstructionEvidence) -> Result<(), GfError> {
        for partition in 0..self.spills.len() {
            if let Some(spill) = self.spills[partition].take() {
                self.sealed[partition] = Some(spill.seal(self.root, evidence)?);
            }
        }
        Ok(())
    }

    /// Sort each partition and concatenate them into one globally ordered run.
    ///
    /// Returns `None` when no record was routed.
    #[allow(clippy::too_many_lines)] // One publication lifecycle; the cleanup arms are the invariant.
    pub(super) fn finish_optional(
        mut self,
        output: &str,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<Option<String>, GfError> {
        let root = self.root;
        self.seal(evidence)?;
        if self.records == 0 {
            return Ok(None);
        }
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(self.family.as_str(), self.records);
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic = super::diagnostics::Group::start(evidence);
        #[cfg(any(test, feature = "test-support"))]
        let partitions = self.sealed.iter().filter(|slot| slot.is_some()).count();
        let temporary = artifact_temp(output);
        let mut publication = root
            .create_unpublished_replaceable_child(temporary.as_os_str())
            .map_err(super::storage)?;
        let identity = match publication
            .verify_identity_with(|| shape_publication_io_failure("initial_file_identity"))
            .map_err(super::storage)
        {
            Ok(identity) => identity,
            Err(primary) => {
                let cleanup = cleanup_shape_publication(&mut publication);
                return combine_secondary_cleanup(Err(primary), cleanup, "partition setup cleanup");
            }
        };
        let file = publication.take_file().map_err(super::storage)?;
        let hashing = match HashingWriter::with_cache_window(file, self.window) {
            Ok(hashing) => hashing,
            Err(primary) => {
                let cleanup = cleanup_shape_publication(&mut publication);
                return combine_secondary_cleanup(Err(primary), cleanup, "partition setup cleanup");
            }
        };
        let configured =
            graphforge_filesystem::validate_cache_release_operation_windows(&[hashing
                .inner
                .window_bytes()])
            .map_err(super::storage)
            .and_then(|_| shape_publication_failure("window_validation"));
        let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
        if let Err(primary) = configured {
            let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
            return combine_secondary_cleanup(Err(primary), cleanup, "partition output cleanup");
        }
        let concatenated = (|| -> Result<u64, GfError> {
            let mut previous: Option<[u8; N]> = None;
            let mut written = 0_u64;
            // Distinct keys per partition, counted off the sorted order below
            // (identical keys are contiguous and never span partitions, since
            // the partition function is a function of the key). This, not the
            // row count, is what the splitters promise to balance: a range
            // partition cannot split one key, so a node-keyed family such as
            // staged endpoints is row-skewed exactly as much as its degree
            // distribution is, and a power-law graph has many hubs, not one
            // (#1439). A bad splitter set concentrates *distinct* keys, which
            // this count does not hide. For a unique-key family it equals the
            // row balance recorded at routing time.
            let mut keys = PartitionBalance::new(self.sealed.len());
            // Every sealed partition, in canonical index order. Workers load
            // and sort them on their own; this thread consumes them in this
            // order, so the output is the same for any worker count.
            let jobs = self
                .sealed
                .iter()
                .enumerate()
                .filter_map(|(partition, slot)| {
                    slot.as_ref().map(|receipt| {
                        (
                            partition,
                            receipt.name.clone(),
                            self.balance.rows().get(partition).copied(),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let codec = self.codec;
            let load = |job: usize, stop: &AtomicBool| {
                let (_, name, expected) = &jobs[job];
                load_fixed_partition::<N>(root, name, *expected, codec, stop)
            };
            let consume =
                |job: usize, (records, counters): (Vec<[u8; N]>, PartitionLoadCounters)| {
                    let partition = jobs[job].0;
                    reject_cancelled(cancelled)?;
                    // The ordered critical section: fold the worker's local
                    // counters into the shared evidence, then write the partition.
                    counters.merge_into(evidence)?;
                    injected_input_release_failure()?;
                    for record in &records {
                        // Partition order is key order, so the concatenation is the
                        // global order. Prove it rather than assume it: this is the
                        // invariant the external merge's heap used to provide.
                        if let Some(prior) = previous.as_ref() {
                            if prior[..16] > record[..16] {
                                return Err(super::storage(
                                    "range partition concatenation is not globally ordered",
                                ));
                            }
                            if self.reject_duplicates && prior[..16] == record[..16] {
                                return Err(super::storage(
                                    "duplicate identity across construction runs",
                                ));
                            }
                            if prior[..16] != record[..16] {
                                keys.record(partition)?;
                            }
                        } else {
                            keys.record(partition)?;
                        }
                        let wire = run_record_bytes(record, self.codec)?;
                        writer.write_all(wire).map_err(super::storage)?;
                        account_merge_write_bytes(evidence, wire.len() as u64)?;
                        previous = Some(*record);
                        written = written.checked_add(1).ok_or_else(|| {
                            super::storage("partition output record count overflows")
                        })?;
                        if written.is_multiple_of(4096) {
                            reject_cancelled(cancelled)?;
                        }
                    }
                    Ok(())
                };
            consume_in_partition_order(jobs.len(), self.load_workers, load, consume)?;
            // Load-bearing, not a nicety (#1439): a collapsed one-partition
            // run is perfectly deterministic and passes every byte-equality
            // test, so this is what tells a working range partition apart
            // from a catastrophically skewed one for every fixed-width
            // family, not only identities.
            keys.assert_balanced(&format!("{} distinct keys", self.family.as_str()))?;
            Ok(written)
        })();
        let written = match concatenated {
            Ok(written) => written,
            Err(primary) => {
                let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
                return combine_secondary_cleanup(
                    Err(primary),
                    cleanup,
                    "partition output cleanup",
                );
            }
        };
        let finalized = writer.flush().map_err(super::storage).and_then(|()| {
            writer
                .get_mut()
                .inner
                .sync_all_and_release()
                .map_err(super::storage)
        });
        if let Err(primary) = finalized {
            let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
            return combine_secondary_cleanup(Err(primary), cleanup, "partition output cleanup");
        }
        let cache_release = writer.get_ref().inner.evidence();
        let prepared = (|| -> Result<(ArtifactReceipt, GraphConstructionEvidence), GfError> {
            shape_publication_failure("fsync_evidence_overflow")?;
            let mut committed = evidence.clone();
            account_cache_release(cache_release, &mut committed)?;
            account_sequential_write(writer.get_ref().bytes, &mut committed)?;
            shape_publication_failure("final_file_identity")?;
            if file_identity(writer.get_ref().inner.file()).map_err(super::storage)?
                != publication.identity().map_err(super::storage)?
            {
                return Err(super::storage(
                    "partition output authority changed before publication",
                ));
            }
            shape_publication_failure("file_space_usage")?;
            let allocated_bytes =
                graphforge_filesystem::file_space_usage(writer.get_ref().inner.file())
                    .map_err(super::storage)?
                    .allocated_bytes;
            let receipt = ArtifactReceipt {
                name: output.to_owned(),
                bytes: writer.get_ref().bytes,
                allocated_bytes,
                sha256: hex(&writer.get_ref().digest.clone().finalize()),
                xxh64: crate::corruption_checksum::hex(writer.get_ref().checksum.finish()),
                identity: identity.into(),
                write_operations: writer.get_ref().operations,
                fsync_operations: cache_release
                    .sync_operations
                    .checked_add(1)
                    .ok_or_else(|| super::storage("artifact synchronization count overflows"))?,
            };
            record_shape_artifact_install(&mut committed, &receipt)?;
            account_fixed_write_operations(&receipt, &mut committed)?;
            committed.merge_fsync_operations = committed
                .merge_fsync_operations
                .checked_add(receipt.fsync_operations)
                .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
            committed.partition_rows = committed
                .partition_rows
                .checked_add(written)
                .ok_or_else(|| super::storage("partition row count overflows"))?;
            committed.partition_outputs = committed
                .partition_outputs
                .checked_add(1)
                .ok_or_else(|| super::storage("partition output count overflows"))?;
            Ok((receipt, committed))
        })();
        let (receipt, committed) = match prepared {
            Ok(prepared) => prepared,
            Err(primary) => {
                let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
                return combine_secondary_cleanup(
                    Err(primary),
                    cleanup,
                    "partition setup publication cleanup",
                );
            }
        };
        drop(writer);
        let published = (|| -> Result<(), GfError> {
            shape_publication_failure("install_child")?;
            let allocation_file = if root.allocation().is_some() {
                let file = root
                    .open_child_file(temporary.as_os_str())
                    .map_err(super::storage)?;
                root.observe_file(temporary.as_os_str(), &file)
                    .map_err(super::storage)?;
                Some(file)
            } else {
                None
            };
            publication
                .install_child(OsStr::new(output))
                .map_err(super::storage)?;
            if let Some(file) = &allocation_file {
                root.record_replacement(temporary.as_os_str(), OsStr::new(output), file)
                    .map_err(super::storage)?;
            }
            drop(allocation_file);
            publication.sync_parent().map_err(super::storage)?;
            shape_publication_failure("directory_sync")?;
            shape_publication_failure("post_publication_metric_overflow")?;
            shape_publication_failure("manifest_update")?;
            persist_shape_receipt(root, &receipt)
        })();
        if let Err(primary) = published {
            let receipt_cleanup =
                unlink_writer_capability(root, output, Some(&receipt)).map_err(super::storage);
            let primary = combine_secondary_cleanup::<()>(
                Err(primary),
                receipt_cleanup,
                "partition receipt cleanup",
            )
            .expect_err("primary partition publication failure is retained");
            let cleanup = cleanup_shape_publication(&mut publication);
            return combine_secondary_cleanup(
                Err(primary),
                cleanup,
                "partition publication cleanup",
            );
        }
        publication.commit().map_err(super::storage)?;
        *evidence = committed;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(self.family.as_str(), 1, partitions, evidence, true);
        }
        construction_failpoint("shape.partition_output.after_install");
        for partition in 0..self.sealed.len() {
            if let Some(receipt) = self.sealed[partition].take() {
                unlink_shape_artifact(root, &receipt.name, evidence)?;
            }
        }
        Ok(Some(output.to_owned()))
    }
}

/// Read one fixed-width partition spill into memory and sort it, on a worker.
///
/// This is the materialization the design's rule R4 requires: the sorted
/// partition exists in full before any of it is written, so row-group and
/// record boundaries are a pure function of row count, never of arrival.
///
/// The shared evidence is never touched here. Everything the load observes
/// comes back as [`PartitionLoadCounters`] for the coordinator to merge, in
/// partition order, once it consumes the result.
///
/// `expected_records`, when given, pre-sizes the returned `Vec` from the
/// row count the routing pass already recorded in the balance (#1439),
/// removing the growth-by-doubling headroom `Vec::new()` would otherwise
/// carry at the moment a large partition is fully materialized.
fn load_fixed_partition<const N: usize>(
    root: &StableDirectory,
    name: &str,
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    stop: &AtomicBool,
) -> Result<(Vec<[u8; N]>, PartitionLoadCounters), GfError> {
    let (mut reader, counter, spill_bytes) = open_fixed_reader(root, name)?;
    let mut counters = PartitionLoadCounters {
        spill_bytes,
        ..PartitionLoadCounters::default()
    };
    let loaded = (|| -> Result<Vec<[u8; N]>, GfError> {
        let mut records = match expected_records.and_then(|count| usize::try_from(count).ok()) {
            Some(count) => Vec::with_capacity(count),
            None => Vec::new(),
        };
        while let Some(record) = read_run_record::<N>(&mut reader, codec)? {
            records.push(record);
            abandon_if_stopped(records.len(), stop)?;
        }
        Ok(records)
    })();
    let released = reader
        .get_mut()
        .inner
        .finish()
        .map_err(super::storage)
        .map(|released| counters.cache_release = released);
    let mut records = combine_cache_cleanup(loaded, released, "partition spill")?;
    (counters.read_bytes, counters.read_operations) = counter.values();
    counters.records = records.len() as u64;
    // The sort key is the whole record, whose leading 16 bytes are the
    // UUID. Records are globally unique on that prefix, so this is a total
    // order and no stability assumption is needed.
    records.sort_unstable();
    Ok((records, counters))
}

/// Routes normalized Arrow rows into per-partition spills, then emits one
/// sorted Parquet artifact with pinned writer properties.
pub(super) struct RowRangePartitioner<'a> {
    root: &'a StableDirectory,
    #[cfg(any(test, feature = "test-support"))]
    diagnostic_family: String,
    namespace: String,
    schema: Option<SchemaRef>,
    window: std::num::NonZeroU64,
    spills: Vec<Option<RowSpill>>,
    sealed: Vec<Option<ArtifactReceipt>>,
    balance: PartitionBalance,
    rows: u64,
}

struct RowSpill {
    writer: arrow::ipc::writer::StreamWriter<BufWriter<HashingWriter>>,
    name: String,
    temporary: std::ffi::OsString,
    identity: FileIdentity,
}

impl Drop for RowRangePartitioner<'_> {
    fn drop(&mut self) {
        for slot in &mut self.spills {
            if let Some(spill) = slot.take() {
                let RowSpill {
                    writer,
                    name,
                    temporary,
                    identity,
                } = spill;
                drop(writer);
                let _ = name;
                let _ = self
                    .root
                    .unlink_child_if_identity(temporary.as_os_str(), identity);
                let _ = self.root.sync();
            }
        }
    }
}

impl<'a> RowRangePartitioner<'a> {
    pub(super) fn new(
        root: &'a StableDirectory,
        authority: &str,
        partitions: usize,
    ) -> Result<Self, GfError> {
        let streams = partitions
            .checked_add(1)
            .ok_or_else(|| super::storage("row partition stream count overflow"))?;
        let window = graphforge_filesystem::cache_release_window_for_streams(streams)
            .map_err(super::storage)?;
        Ok(Self {
            root,
            #[cfg(any(test, feature = "test-support"))]
            diagnostic_family: super::diagnostics::row_family(authority),
            namespace: sha256(authority.as_bytes())[..16].to_owned(),
            schema: None,
            window,
            spills: (0..partitions).map(|_| None).collect(),
            sealed: (0..partitions).map(|_| None).collect(),
            balance: PartitionBalance::new(partitions),
            rows: 0,
        })
    }

    /// Route every row of one staged chunk Parquet into its partition.
    ///
    /// Staged chunks are already UUID-sorted and the partition function is
    /// monotone, so each partition's rows form one contiguous slice of each
    /// decoded batch: routing is a slice, never a shuffle.
    pub(super) fn push(
        &mut self,
        plan: &PartitionPlan,
        input: &str,
        batch_rows: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let file = self
            .root
            .open_child_file(OsStr::new(input))
            .map_err(super::storage)?;
        let counter = IoCounter::default();
        let chunk_reader =
            CountingChunkReader::with_cache_window(file, counter.clone(), self.window);
        let cache_release = chunk_reader.cache_release_tracker();
        let routed = (|| -> Result<(), GfError> {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(chunk_reader).map_err(super::storage)?;
            if self
                .schema
                .as_ref()
                .is_some_and(|known| known.as_ref() != builder.schema().as_ref())
            {
                return Err(super::storage("row partition schemas differ"));
            }
            let schema = builder.schema().clone();
            self.schema.get_or_insert_with(|| schema.clone());
            let reader = builder
                .with_batch_size(batch_rows.clamp(1, 4096))
                .build()
                .map_err(super::storage)?;
            for batch in reader {
                let batch = batch.map_err(super::storage)?;
                self.route_batch(plan, &schema, &batch, evidence)?;
                reject_cancelled(cancelled)?;
            }
            Ok(())
        })();
        let released = cache_release.check_error().map_err(super::storage);
        account_cache_release(cache_release.evidence(), evidence)?;
        counter.add_to(evidence)?;
        combine_cache_cleanup(routed, released, "row partition source")
    }

    fn route_batch(
        &mut self,
        plan: &PartitionPlan,
        schema: &SchemaRef,
        batch: &RecordBatch,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let uuids = key_column(batch)?;
        let mut start = 0;
        while start < batch.num_rows() {
            let partition = plan.partition_of(&uuid_value(uuids, start)?);
            let mut end = start + 1;
            while end < batch.num_rows() && plan.partition_of(&uuid_value(uuids, end)?) == partition
            {
                end += 1;
            }
            self.route_slice(schema, batch, start, end - start, partition, evidence)?;
            start = end;
        }
        Ok(())
    }

    fn route_slice(
        &mut self,
        schema: &SchemaRef,
        batch: &RecordBatch,
        offset: usize,
        length: usize,
        partition: usize,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let slot = self
            .spills
            .get_mut(partition)
            .ok_or_else(|| super::storage("routed row partition is out of range"))?;
        if slot.is_none() {
            let name = row_spill_name(&self.namespace, partition);
            let temporary = artifact_temp(&name);
            let file = self
                .root
                .create_replaceable_child_file(temporary.as_os_str())
                .map_err(super::storage)?;
            let identity = file_identity(&file).map_err(super::storage)?;
            let hashing = HashingWriter::with_cache_window(file, self.window)?;
            let buffered = BufWriter::with_capacity(SPILL_BLOCK_BYTES, hashing);
            *slot = Some(RowSpill {
                writer: arrow::ipc::writer::StreamWriter::try_new(buffered, schema)
                    .map_err(super::storage)?,
                name,
                temporary,
                identity,
            });
        }
        let spill = slot
            .as_mut()
            .ok_or_else(|| super::storage("row partition spill is absent"))?;
        spill
            .writer
            .write(&batch.slice(offset, length))
            .map_err(super::storage)?;
        for _ in 0..length {
            self.balance.record(partition)?;
        }
        self.rows = self
            .rows
            .checked_add(length as u64)
            .ok_or_else(|| super::storage("row partition count overflows"))?;
        evidence.merge_read_records = evidence
            .merge_read_records
            .checked_add(length as u64)
            .ok_or_else(|| super::storage("merge read records overflows"))?;
        Ok(())
    }

    /// Seal every open row spill.
    fn seal(&mut self, evidence: &mut GraphConstructionEvidence) -> Result<(), GfError> {
        for partition in 0..self.spills.len() {
            let Some(spill) = self.spills[partition].take() else {
                continue;
            };
            let RowSpill {
                writer,
                name,
                temporary,
                identity,
            } = spill;
            let buffered = writer.into_inner().map_err(super::storage)?;
            self.sealed[partition] = Some(
                SpillWriter {
                    name,
                    temporary,
                    identity,
                    writer: buffered,
                }
                .seal(self.root, evidence)?,
            );
        }
        Ok(())
    }

    /// Sort each partition and write one globally ordered Parquet artifact.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One publication lifecycle.
    pub(super) fn finish(
        mut self,
        output: &str,
        output_rows: usize,
        output_bytes: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<String, GfError> {
        if output_rows == 0 || output_bytes == 0 {
            return Err(super::storage("invalid row partition group"));
        }
        let root = self.root;
        self.seal(evidence)?;
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(&self.diagnostic_family, self.rows);
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic = super::diagnostics::Group::start(evidence);
        #[cfg(any(test, feature = "test-support"))]
        let partitions = self.sealed.iter().filter(|slot| slot.is_some()).count();
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| super::storage("row partition lacks schema"))?;
        let temporary = artifact_temp(output);
        let file = root
            .create_replaceable_child_file(temporary.as_os_str())
            .map_err(super::storage)?;
        let identity = file_identity(&file).map_err(super::storage)?;
        let hashing = HashingWriter::with_cache_window(file, self.window)?;
        let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
        // Rule R5: every permanent Parquet on the ingest path takes the pinned
        // writer properties. `merge_row_group` used to pass `None` here, which
        // left shaped rows on arrow-rs defaults and made every downstream digest
        // hostage to an arrow-rs bump.
        let mut writer = ArrowWriter::try_new(
            buffered,
            schema.clone(),
            Some(crate::permanent_parquet::writer_properties().build()),
        )
        .map_err(super::storage)?;
        let mut previous: Option<[u8; 16]> = None;
        let written = (|| -> Result<u64, GfError> {
            let mut written = 0_u64;
            for partition in 0..self.sealed.len() {
                let Some(name) = self.sealed[partition]
                    .as_ref()
                    .map(|receipt| receipt.name.clone())
                else {
                    continue;
                };
                reject_cancelled(cancelled)?;
                let sorted = self.load_partition(&name, &schema, evidence)?;
                let uuids = key_column(&sorted)?;
                for row in 0..sorted.num_rows() {
                    let uuid = uuid_value(uuids, row)?;
                    if previous.is_some_and(|prior| prior >= uuid) {
                        return Err(super::storage(
                            "duplicate or unordered UUID in row partition",
                        ));
                    }
                    previous = Some(uuid);
                }
                written = written
                    .checked_add(u64::try_from(sorted.num_rows()).map_err(super::storage)?)
                    .ok_or_else(|| super::storage("row partition output count overflows"))?;
                write_bounded_row_groups(
                    &mut writer,
                    &sorted,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )?;
            }
            // Every row is keyed by its own unique UUID (the loop above
            // already refuses a duplicate), so no hub scenario exists here:
            // a skewed row partitioning is always a splitter defect (#1439).
            self.balance
                .assert_balanced(&format!("row partition {}", self.namespace))?;
            Ok(written)
        })();
        let written = match written {
            Ok(written) => written,
            Err(primary) => {
                drop(writer);
                // Leave no owned temporary behind on a failed or cancelled pass.
                let _ = root.unlink_child_if_identity(temporary.as_os_str(), identity);
                let _ = root.sync();
                return Err(primary);
            }
        };
        writer.finish().map_err(super::storage)?;
        writer.sync().map_err(super::storage)?;
        let hashing = writer.inner_mut().get_mut();
        hashing
            .inner
            .sync_all_and_release()
            .map_err(super::storage)?;
        let cache_release = hashing.inner.evidence();
        account_cache_release(cache_release, evidence)?;
        account_sequential_write(hashing.bytes, evidence)?;
        let receipt = ArtifactReceipt {
            name: output.to_owned(),
            bytes: hashing.bytes,
            allocated_bytes: graphforge_filesystem::file_space_usage(hashing.inner.file())
                .map_err(super::storage)?
                .allocated_bytes,
            sha256: hex(&hashing.digest.clone().finalize()),
            xxh64: crate::corruption_checksum::hex(hashing.checksum.finish()),
            identity: identity.into(),
            write_operations: hashing.operations,
            fsync_operations: cache_release
                .sync_operations
                .checked_add(3)
                .ok_or_else(|| super::storage("artifact synchronization count overflows"))?,
        };
        drop(writer);
        root.install_child(temporary.as_os_str(), identity, OsStr::new(output))
            .map_err(super::storage)?;
        root.sync().map_err(super::storage)?;
        construction_failpoint("shape.row_partition.after_install");
        persist_shape_receipt(root, &receipt)?;
        record_shape_artifact_install(evidence, &receipt)?;
        evidence.parquet_write_bytes = evidence
            .parquet_write_bytes
            .checked_add(receipt.bytes)
            .ok_or_else(|| super::storage("Parquet write byte count overflows"))?;
        evidence.parquet_write_operations = evidence
            .parquet_write_operations
            .checked_add(receipt.write_operations)
            .ok_or_else(|| super::storage("Parquet write operation count overflows"))?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
        evidence.partition_rows = evidence
            .partition_rows
            .checked_add(written)
            .ok_or_else(|| super::storage("partition row count overflows"))?;
        evidence.partition_outputs = evidence
            .partition_outputs
            .checked_add(1)
            .ok_or_else(|| super::storage("partition output count overflows"))?;
        for partition in 0..self.sealed.len() {
            if let Some(receipt) = self.sealed[partition].take() {
                unlink_shape_artifact(root, &receipt.name, evidence)?;
            }
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(&self.diagnostic_family, 1, partitions, evidence, true);
        }
        Ok(output.to_owned())
    }

    /// Read one row partition spill into memory and sort it by UUID.
    fn load_partition(
        &self,
        name: &str,
        schema: &SchemaRef,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<RecordBatch, GfError> {
        let file = self
            .root
            .open_child_file(OsStr::new(name))
            .map_err(super::storage)?;
        let counter = IoCounter::default();
        let reader = super::CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                file,
                self.window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(super::storage)?,
            counter: counter.clone(),
        };
        let mut reader = std::io::BufReader::with_capacity(BLOCK_BYTES, reader);
        let loaded = (|| -> Result<RecordBatch, GfError> {
            let stream = arrow::ipc::reader::StreamReader::try_new(&mut reader, None)
                .map_err(super::storage)?;
            let mut batches = Vec::new();
            for batch in stream {
                batches.push(batch.map_err(super::storage)?);
            }
            let combined = concat_batches(schema, &batches).map_err(super::storage)?;
            drop(batches);
            let uuids = key_column(&combined)?;
            let rows = u32::try_from(combined.num_rows()).map_err(super::storage)?;
            let mut order = (0..rows).collect::<Vec<_>>();
            let mut keys = Vec::with_capacity(combined.num_rows());
            for row in 0..combined.num_rows() {
                keys.push(uuid_value(uuids, row)?);
            }
            // UUIDs are unique within a schema group, so sorting on the key
            // alone is a total order and the permutation is unambiguous.
            order.sort_unstable_by_key(|index| keys[*index as usize]);
            take_record_batch(&combined, &UInt32Array::from(order)).map_err(super::storage)
        })();
        let released = reader.get_mut().inner.finish().map_err(super::storage);
        let released = match released {
            Ok(evidence_release) => account_cache_release(evidence_release, evidence),
            Err(error) => Err(error),
        };
        let sorted = combine_cache_cleanup(loaded, released, "row partition spill")?;
        counter.add_to(evidence)?;
        evidence.peak_partition_records = evidence
            .peak_partition_records
            .max(sorted.num_rows() as u64);
        Ok(sorted)
    }
}

/// Bytes one row of a column contributes to the row-group byte window.
///
/// This is exactly `column.slice(row, 1).to_data().get_slice_memory_size()`
/// (arrow-data's per-buffer accounting for a one-row slice: fixed-width
/// buffers contribute their width, a variable-width buffer contributes the
/// value's bytes plus one offset, a present null buffer contributes one byte)
/// computed without materialising a one-row `ArrayData` per row per column.
/// That materialisation was four heap allocations per row per column: 52M of
/// the 104M allocations `validate` made at S18, measured with heaptrack. Any
/// type outside the enumerated fast paths still takes the exact arrow
/// computation, so the boundaries, and therefore the shaped bytes, are
/// unchanged for every schema.
enum RowBytes<'a> {
    Constant(usize),
    Utf8(&'a StringArray, usize),
    LargeUtf8(&'a LargeStringArray, usize),
    Binary(&'a BinaryArray, usize),
    LargeBinary(&'a LargeBinaryArray, usize),
    Exact(&'a ArrayRef),
}

/// A column's row sizer plus its validity buffer, if any.
struct RowSizer<'a> {
    bytes: RowBytes<'a>,
    nulls: Option<&'a NullBuffer>,
}

impl<'a> RowSizer<'a> {
    fn of(column: &'a ArrayRef) -> Self {
        Self {
            bytes: RowBytes::of(column),
            nulls: column.nulls(),
        }
    }

    fn at(&self, row: usize) -> Result<usize, GfError> {
        // `ArrayData` drops a validity buffer whose null count is zero, so a
        // one-row slice carries one validity byte exactly when that row is
        // null (arrow-data `ArrayDataBuilder::build`).
        let null = usize::from(
            !self.bytes.counts_validity() && self.nulls.is_some_and(|nulls| nulls.is_null(row)),
        );
        self.bytes
            .at(row)?
            .checked_add(null)
            .ok_or_else(|| super::storage("partition row byte total overflows"))
    }
}

impl<'a> RowBytes<'a> {
    fn of(column: &'a ArrayRef) -> Self {
        let any = column.as_any();
        match column.data_type() {
            DataType::FixedSizeBinary(width) => match usize::try_from(*width) {
                Ok(width) => Self::Constant(width),
                Err(_) => Self::Exact(column),
            },
            DataType::Boolean => Self::Constant(1),
            DataType::Utf8 => match any.downcast_ref::<StringArray>() {
                Some(array) => Self::Utf8(array, 4),
                None => Self::Exact(column),
            },
            DataType::LargeUtf8 => match any.downcast_ref::<LargeStringArray>() {
                Some(array) => Self::LargeUtf8(array, 8),
                None => Self::Exact(column),
            },
            DataType::Binary => match any.downcast_ref::<BinaryArray>() {
                Some(array) => Self::Binary(array, 4),
                None => Self::Exact(column),
            },
            DataType::LargeBinary => match any.downcast_ref::<LargeBinaryArray>() {
                Some(array) => Self::LargeBinary(array, 8),
                None => Self::Exact(column),
            },
            primitive @ (DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)) => match primitive.primitive_width() {
                Some(width) => Self::Constant(width),
                None => Self::Exact(column),
            },
            _ => Self::Exact(column),
        }
    }

    fn at(&self, row: usize) -> Result<usize, GfError> {
        let value = |length: usize, base: usize| {
            length
                .checked_add(base)
                .ok_or_else(|| super::storage("partition row byte total overflows"))
        };
        match self {
            Self::Constant(bytes) => Ok(*bytes),
            Self::Utf8(array, base) => value(
                usize::try_from(array.value_length(row)).map_err(super::storage)?,
                *base,
            ),
            Self::LargeUtf8(array, base) => value(
                usize::try_from(array.value_length(row)).map_err(super::storage)?,
                *base,
            ),
            Self::Binary(array, base) => value(
                usize::try_from(array.value_length(row)).map_err(super::storage)?,
                *base,
            ),
            Self::LargeBinary(array, base) => value(
                usize::try_from(array.value_length(row)).map_err(super::storage)?,
                *base,
            ),
            Self::Exact(column) => column
                .slice(row, 1)
                .to_data()
                .get_slice_memory_size()
                .map_err(super::storage),
        }
    }

    /// Whether [`Self::at`] already includes the row's validity byte.
    const fn counts_validity(&self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

/// Write one sorted partition as row groups bounded by row count and bytes.
///
/// Boundaries are a pure function of the sorted row sequence, never of arrival
/// order or buffer pressure, which is what keeps the produced bytes identical
/// across runs.
fn write_bounded_row_groups<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    sorted: &RecordBatch,
    output_rows: usize,
    output_bytes: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let sizers = sorted
        .columns()
        .iter()
        .map(RowSizer::of)
        .collect::<Vec<_>>();
    let mut start = 0;
    while start < sorted.num_rows() {
        let mut length = 0;
        let mut bytes = 0_usize;
        while start + length < sorted.num_rows() && length < output_rows {
            let row_bytes = sizers.iter().try_fold(0_usize, |total, sizer| {
                sizer
                    .at(start + length)?
                    .checked_add(total)
                    .ok_or_else(|| super::storage("partition row byte total overflows"))
            })?;
            if row_bytes > output_bytes {
                return Err(super::storage(
                    "one normalized row exceeds partition byte window",
                ));
            }
            let next = bytes
                .checked_add(row_bytes)
                .ok_or_else(|| super::storage("partition selected bytes overflow"))?;
            if length > 0 && next > output_bytes {
                break;
            }
            bytes = next;
            length += 1;
        }
        let group = sorted.slice(start, length);
        writer.write(&group).map_err(super::storage)?;
        evidence.merge_written_records = evidence
            .merge_written_records
            .checked_add(length as u64)
            .ok_or_else(|| super::storage("merge written records overflows"))?;
        start += length;
        reject_cancelled(cancelled)?;
    }
    Ok(())
}

#[cfg(test)]
mod row_bytes_tests {
    use super::{RowBytes, RowSizer};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array,
        LargeStringArray, ListArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};
    use std::sync::Arc;

    fn exact(column: &ArrayRef, row: usize) -> usize {
        column
            .slice(row, 1)
            .to_data()
            .get_slice_memory_size()
            .unwrap()
    }

    /// Every fast path must agree with arrow's own one-row accounting, with
    /// and without a null buffer, or the row-group boundaries would move.
    #[test]
    fn row_bytes_match_arrow_slice_accounting_for_every_fast_path() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[1_u8; 16], [2; 16], [3; 16]].into_iter())
                    .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some([1_u8; 16]), None, Some([3; 16])].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            Arc::new(StringArray::from(vec![
                "EDGE",
                "",
                "a much longer route name",
            ])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("yyy")])),
            Arc::new(LargeStringArray::from(vec![Some("x"), None, Some("yyy")])),
            Arc::new(BinaryArray::from(vec![
                Some(&b"ab"[..]),
                None,
                Some(&b"abcd"[..]),
            ])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(true)])),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(2)]),
                None,
                Some(vec![Some(3)]),
            ])),
        ];
        for column in &columns {
            assert!(matches!(
                column.data_type(),
                DataType::FixedSizeBinary(_)
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::Int64
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::List(_)
            ));
            let sizer = RowSizer::of(column);
            for row in 0..column.len() {
                assert_eq!(
                    sizer.at(row).unwrap(),
                    exact(column, row),
                    "{:?} row {row}",
                    column.data_type()
                );
            }
        }
        let list = columns.last().unwrap();
        assert!(matches!(RowBytes::of(list), RowBytes::Exact(_)));
        // A non-null row of a nullable column costs no validity byte; a null
        // row costs one. Both are covered above; pin them explicitly.
        let nullable: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        assert_eq!(RowSizer::of(&nullable).at(0).unwrap(), 8);
        assert_eq!(RowSizer::of(&nullable).at(1).unwrap(), 9);
        let _ = Field::new("unused", DataType::Null, true);
    }
}
