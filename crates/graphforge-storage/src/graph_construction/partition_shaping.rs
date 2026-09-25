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
use super::partition_records::PartitionRecords;
use super::{
    ArtifactReceipt, BLOCK_BYTES, CountingChunkReader, GraphConstructionEvidence, HashingWriter,
    IoCounter, ReadWork, SealDirectoryBatch, account_cache_release, account_fixed_write_operations,
    account_merge_read_bytes, account_merge_write_bytes, account_sequential_write, artifact_temp,
    cleanup_failed_shape_output, cleanup_shape_publication, combine_cache_cleanup,
    combine_secondary_cleanup, construction_failpoint, hex, injected_input_release_failure,
    merge_cache_release_evidence, open_fixed_reader, persist_shape_receipt,
    persist_shape_receipt_in_batch, read_run_record, record_shape_artifact_install,
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
use std::cell::RefCell;
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

/// Durable name of one fixed-width partition spill **segment**.
///
/// Spills are sealed at group boundaries so the staged input of fully routed
/// chunks can retire while shaping continues (#1418). The sealing boundary is
/// part of the name: every segment of one partition is uniquely named, the
/// boundary ordering is self-describing, and a segment whose boundary is not
/// covered by a `shape-progress` control is identifiable as unclaimed.
pub(super) fn fixed_spill_name(family: PartitionFamily, boundary: u64, partition: usize) -> String {
    format!(
        "part-{}-g{boundary:020}-p{partition:05}.run",
        family.as_str()
    )
}

/// Durable name of one row partition spill segment.
pub(super) fn row_spill_name(namespace: &str, boundary: u64, partition: usize) -> String {
    format!("part-rows-{namespace}-g{boundary:020}-p{partition:05}.arrow")
}

/// One parsed partition-spill segment name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SegmentName {
    /// Fixed-width family, or `None` for a row (`part-rows-…`) segment.
    pub(super) family: Option<PartitionFamily>,
    /// Row namespace digest; empty for a fixed-width segment.
    pub(super) namespace: String,
    /// Sealing boundary recorded in the name.
    pub(super) boundary: u64,
    /// Partition index.
    pub(super) partition: usize,
}

impl SegmentName {
    /// The stable partitioner tag this segment belongs to: the fixed family
    /// name, or `rows:<namespace>` for a row group.
    pub(super) fn tag(&self) -> String {
        match &self.family {
            Some(family) => family.as_str().to_owned(),
            None => format!("rows:{}", self.namespace),
        }
    }
}

/// Parse one partition-spill segment name, or `None` when the name is not in
/// the segment grammar.
pub(super) fn parse_segment_name(name: &str) -> Option<SegmentName> {
    let is_boundary =
        |value: &str| value.len() == 20 && value.bytes().all(|byte| byte.is_ascii_digit());
    let is_partition_index =
        |tail: &str| tail.len() == 5 && tail.bytes().all(|byte| byte.is_ascii_digit());
    for family in PartitionFamily::ALL {
        let parsed = name
            .strip_prefix("part-")
            .and_then(|body| body.strip_prefix(family.as_str()))
            .and_then(|body| body.strip_prefix("-g"))
            .and_then(|body| body.split_once("-p"))
            .and_then(|(boundary, tail)| {
                tail.strip_suffix(".run")
                    .map(|partition| (boundary, partition))
            });
        let Some((boundary, partition)) = parsed else {
            continue;
        };
        if !is_boundary(boundary) || !is_partition_index(partition) {
            return None;
        }
        return Some(SegmentName {
            family: Some(family),
            namespace: String::new(),
            boundary: boundary.parse().ok()?,
            partition: partition.parse().ok()?,
        });
    }
    let parsed = name
        .strip_prefix("part-rows-")
        .and_then(|body| body.strip_suffix(".arrow"))
        .and_then(|body| body.split_once("-g"))
        .and_then(|(namespace, tail)| {
            tail.split_once("-p")
                .map(|(boundary, partition)| (namespace, boundary, partition))
        })?;
    if parsed.0.len() != 16
        || !super::is_canonical_lower_hex(parsed.0, 16)
        || !is_boundary(parsed.1)
        || !is_partition_index(parsed.2)
    {
        return None;
    }
    Some(SegmentName {
        family: None,
        namespace: parsed.0.to_owned(),
        boundary: parsed.1.parse().ok()?,
        partition: parsed.2.parse().ok()?,
    })
}

/// Whether `name` is a per-partition spill segment in the durable shaping
/// grammar.
pub(crate) fn is_partition_artifact_name(name: &str) -> bool {
    parse_segment_name(name).is_some()
}

/// The sort key column of a normalized construction row batch.
///
/// The staged schema puts the identity UUID first; its name is domain-defined,
/// so it is taken from the schema rather than assumed.
fn key_column(batch: &RecordBatch) -> Result<&arrow::array::FixedSizeBinaryArray, GfError> {
    let schema = batch.schema();
    uuid_column(batch, schema.field(0).name())
}

/// Boundary placeholder a spill's **temporary** is created under.
///
/// The sealing boundary does not exist yet when a spill starts writing, but
/// the temporary's target must be a canonical artifact name so crash cleanup
/// recognises it. Boundary zero is never a real boundary (real controls start
/// at one), so an installed name can never collide with a temporary target.
const UNSEALED_BOUNDARY: u64 = 0;

/// One open, unsealed partition spill.
struct SpillWriter {
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
        name: &str,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEvidence,
        batch: &mut SealDirectoryBatch,
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
            name: name.to_owned(),
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
        root.install_child(self.temporary.as_os_str(), self.identity, OsStr::new(&name))
            .map_err(super::storage)?;
        // The artifact's name becomes durable with the batch (#1452); the
        // per-spill directory sync it replaces is what serialized every
        // family and partition on one inode.
        batch.mark();
        construction_failpoint("shape.partition_spill.after_install");
        persist_shape_receipt_in_batch(root, &receipt, batch)?;
        record_shape_artifact_install(evidence, &receipt)?;
        account_fixed_write_operations(&receipt, evidence)?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
        Ok(receipt)
    }
}

/// Most lanes one boundary's spill seals lease (#1448). A seal is a data
/// fsync, a rename and a small control install; lanes overlap the fsyncs.
const SEAL_LANES: usize = 8;

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
    temporary: std::ffi::OsString,
    identity: FileIdentity,
    writer: HashingWriter,
    buffer: Vec<u8>,
    bound: usize,
}

impl FixedSpillWriter {
    fn create(
        root: &StableDirectory,
        family: PartitionFamily,
        partition: usize,
        window: std::num::NonZeroU64,
        bound: usize,
    ) -> Result<Self, GfError> {
        let name = fixed_spill_name(family, UNSEALED_BOUNDARY, partition);
        let temporary = artifact_temp(&name);
        let file = root
            .create_replaceable_child_file(temporary.as_os_str())
            .map_err(super::storage)?;
        let identity = file_identity(&file).map_err(super::storage)?;
        let writer = HashingWriter::with_cache_window(file, window)?;
        Ok(Self {
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
        self,
        name: &str,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEvidence,
        batch: &mut SealDirectoryBatch,
    ) -> Result<ArtifactReceipt, GfError> {
        self.seal_files(name, root, batch)?.account(evidence)
    }

    /// The filesystem half of [`Self::seal`]: make the spill durable, install
    /// it under `name` and persist its writer capability into `batch`. The
    /// evidence is charged by [`SealedSpill::account`], so lanes may seal
    /// spills concurrently and the caller charge them in partition order.
    fn seal_files(
        mut self,
        name: &str,
        root: &StableDirectory,
        batch: &mut SealDirectoryBatch,
    ) -> Result<SealedSpill, GfError> {
        self.flush_buffer()?;
        self.writer.flush().map_err(super::storage)?;
        self.writer
            .inner
            .sync_all_and_release()
            .map_err(super::storage)?;
        let cache_release = self.writer.inner.evidence();
        let receipt = ArtifactReceipt {
            name: name.to_owned(),
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
        root.install_child(self.temporary.as_os_str(), self.identity, OsStr::new(&name))
            .map_err(super::storage)?;
        // The artifact's name becomes durable with the batch (#1452); the
        // per-spill directory sync it replaces is what serialized every
        // family and partition on one inode.
        batch.mark();
        construction_failpoint("shape.partition_spill.after_install");
        persist_shape_receipt_in_batch(root, &receipt, batch)?;
        Ok(SealedSpill {
            receipt,
            cache_release,
        })
    }
}

/// A spill sealed on disk whose evidence is not yet charged (#1448).
struct SealedSpill {
    receipt: ArtifactReceipt,
    cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

impl SealedSpill {
    /// Charge the seal to `evidence`, exactly as a sequential seal does.
    fn account(self, evidence: &mut GraphConstructionEvidence) -> Result<ArtifactReceipt, GfError> {
        let Self {
            receipt,
            cache_release,
        } = self;
        account_cache_release(cache_release, evidence)?;
        account_sequential_write(receipt.bytes, evidence)?;
        record_shape_artifact_install(evidence, &receipt)?;
        account_fixed_write_operations(&receipt, evidence)?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| super::storage("merge fsync operations overflows"))?;
        Ok(receipt)
    }
}

/// Routes fixed-width records into per-partition spill segments, then emits
/// one sorted artifact by sorting each partition and concatenating them in
/// index order.
///
/// Each partition holds one segment set, appended per sealing boundary
/// (#1418): the staged input of fully routed chunks retires at the boundary,
/// so a resumed shape rebuilds the partitioner from the surviving sealed
/// segments instead of re-reading retired inputs.
pub(super) struct FixedRangePartitioner<'a, const N: usize> {
    root: &'a StableDirectory,
    family: PartitionFamily,
    codec: Option<DetailCodec>,
    reject_duplicates: bool,
    window: std::num::NonZeroU64,
    spills: Vec<Option<FixedSpillWriter>>,
    sealed: Vec<Vec<ArtifactReceipt>>,
    balance: PartitionBalance,
    records: u64,
    /// Concurrent partition loads while finishing; see
    /// [`super::partition_load::consume_in_partition_order`] for the bound.
    load_workers: NonZeroUsize,
    /// Instance construction CPU admission; when set, the finish-time worker
    /// count is leased from it (#1586).
    cpu_admission: Option<std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    max_partition_bytes: u64,
    /// Recorded external partition bound (#1585); zero keeps the refusal.
    max_external_partition_bytes: u64,
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
            sealed: (0..partitions).map(|_| Vec::new()).collect(),
            balance: PartitionBalance::new(partitions),
            records: 0,
            load_workers: PARTITION_LOAD_WORKERS,
            cpu_admission: None,
            max_partition_bytes: super::partition::default_materialization_bytes(),
            max_external_partition_bytes: 0,
        })
    }

    pub(super) fn with_materialization_limit(mut self, bytes: u64) -> Self {
        self.max_partition_bytes = bytes;
        self
    }

    /// Process a partition over `max_partition_bytes` externally, up to
    /// `bytes` (#1585). Zero keeps the refusal.
    pub(super) fn with_external_partitions(mut self, bytes: u64) -> Self {
        self.max_external_partition_bytes = bytes;
        self
    }

    /// Lease the finish-time worker count from the instance admission (#1586).
    pub(super) fn with_cpu_admission(
        mut self,
        admission: Option<std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    ) -> Self {
        self.cpu_admission = admission;
        self
    }

    /// Force the finish-time worker count. Scheduling only: the tests hold the
    /// evidence and output bytes equal across every value.
    #[cfg(test)]
    pub(super) fn with_load_workers(mut self, workers: NonZeroUsize) -> Self {
        self.load_workers = workers;
        self
    }

    /// Load and sort every job's partition off this thread and hand each to
    /// `consume` here, in job order.
    ///
    /// `cancelled` is the caller's cancellation callback, forwarded so the
    /// coordinator can poll it while it waits for a partition (#1581); the
    /// candidate schedulers poll it the same way.
    fn schedule_loads<C>(
        &self,
        jobs: &[(usize, Vec<String>, Option<u64>)],
        cancelled: &mut dyn FnMut() -> bool,
        consume: C,
    ) -> Result<(), GfError>
    where
        C: FnMut(usize, (LoadedPartition<N>, PartitionLoadCounters)) -> Result<(), GfError>,
    {
        let (root, codec, max_partition_bytes) = (self.root, self.codec, self.max_partition_bytes);
        let max_external_partition_bytes = self.max_external_partition_bytes;
        let load_job = move |root: &StableDirectory,
                             (_, names, expected): &(usize, Vec<String>, Option<u64>),
                             stop: &AtomicBool| {
            // #1585: a codec-free partition over its resident budget is sorted
            // into on-disk runs when the recorded external bound admits it.
            if max_external_partition_bytes != 0
                && codec.is_none()
                && !fits_resident::<N>(root, names, *expected, max_partition_bytes)?
            {
                return super::external_partition::sort_into_runs::<N>(
                    root,
                    names,
                    *expected,
                    max_partition_bytes,
                    max_external_partition_bytes,
                    stop,
                )
                .map(|(runs, counters)| (LoadedPartition::Runs(Box::new(runs)), counters));
            }
            load_fixed_partition::<N>(root, names, *expected, codec, max_partition_bytes, stop)
                .map(|(records, counters)| (LoadedPartition::Resident(records), counters))
        };
        let workers = self.load_workers;
        // #1586: lease the workers from the instance admission. Scheduling
        // only; a partial grant runs with fewer workers and the same bytes.
        let lease = match &self.cpu_admission {
            Some(admission) => Some(admission.acquire(workers, &mut *cancelled)?),
            None => None,
        };
        let workers = lease
            .as_ref()
            .map_or(workers, super::cpu_admission::ConstructionCpuLease::lanes);
        let load = |job: usize, stop: &AtomicBool| load_job(root, &jobs[job], stop);
        consume_in_partition_order(jobs.len(), workers, load, cancelled, consume)
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
                self.family,
                partition,
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

    /// How many partition spills are currently open and unsealed.
    pub(super) fn open_spill_count(&self) -> usize {
        self.spills.iter().filter(|slot| slot.is_some()).count()
    }

    /// Adopt the sealed segments and cumulative routing balance restored from
    /// an interrupted shape (#1418).
    ///
    /// `segments` holds one slot per partition (shorter is allowed when later
    /// partitions never received records); `balance_rows` must cover every
    /// partition and is the exact cumulative routing count restored from the
    /// progress chain.
    pub(super) fn restore(
        &mut self,
        segments: &[Vec<ArtifactReceipt>],
        balance_rows: &[u64],
    ) -> Result<(), GfError> {
        if segments.len() > self.sealed.len() || balance_rows.len() != self.sealed.len() {
            return Err(super::storage(
                "restored partition inventory differs from the plan",
            ));
        }
        for (slot, segments) in self.sealed.iter_mut().zip(segments) {
            slot.extend(segments.iter().cloned());
        }
        let mut total = 0_u64;
        for (partition, rows) in balance_rows.iter().enumerate() {
            self.balance.record_many(partition, *rows)?;
            total = total
                .checked_add(*rows)
                .ok_or_else(|| super::storage("partition record count overflows"))?;
        }
        self.records = total;
        Ok(())
    }

    /// Seal every open spill into this boundary's segment set, sharing one
    /// directory-durability batch across every partitioner of the group
    /// (#1418, #1452).
    pub(super) fn seal_at_boundary(
        &mut self,
        boundary: u64,
        evidence: &mut GraphConstructionEvidence,
        batch: &mut SealDirectoryBatch,
    ) -> Result<(), GfError> {
        let open = self
            .spills
            .iter_mut()
            .enumerate()
            .filter_map(|(partition, slot)| slot.take().map(|spill| (partition, spill)))
            .collect::<Vec<_>>();
        let want = NonZeroUsize::new(SEAL_LANES.min(open.len())).filter(|lanes| lanes.get() > 1);
        let lease = want.and_then(|want| {
            self.cpu_admission
                .as_ref()
                .and_then(|admission| admission.try_acquire(want))
                .filter(|lease| lease.lanes().get() > 1)
        });
        let Some(lease) = lease else {
            let mut first_error = None;
            for (partition, spill) in open {
                let name = fixed_spill_name(self.family, boundary, partition);
                match spill.seal(&name, self.root, evidence, batch) {
                    Ok(receipt) => self.sealed[partition].push(receipt),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            return first_error.map_or(Ok(()), Err);
        };
        // #1448: seal on lanes, each into its own directory batch; charge the
        // evidence in partition order afterwards, so it does not depend on
        // the schedule. Every spill is attempted even after one fails.
        let family = self.family;
        let root = self.root;
        let jobs = open
            .into_iter()
            .map(|job| std::sync::Mutex::new(Some(job)))
            .collect::<Vec<_>>();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let sealed = std::sync::Mutex::new(Vec::with_capacity(jobs.len()));
        let lane_batches = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for _ in 0..lease.lanes().get() {
                scope.spawn(|| {
                    let mut lane_batch = SealDirectoryBatch::new(root);
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        let Some(job) = jobs.get(index) else {
                            break;
                        };
                        let Some((partition, spill)) =
                            job.lock().ok().and_then(|mut job| job.take())
                        else {
                            continue;
                        };
                        let name = fixed_spill_name(family, boundary, partition);
                        let result = spill.seal_files(&name, root, &mut lane_batch);
                        if let Ok(mut sealed) = sealed.lock() {
                            sealed.push((partition, result));
                        }
                    }
                    if let Ok(mut batches) = lane_batches.lock() {
                        batches.push(lane_batch);
                    }
                });
            }
        });
        drop(lease);
        for mut lane_batch in lane_batches
            .into_inner()
            .map_err(|_| super::storage("seal lane batches poisoned"))?
        {
            batch.absorb(&mut lane_batch);
        }
        let mut sealed = sealed
            .into_inner()
            .map_err(|_| super::storage("seal lane results poisoned"))?;
        sealed.sort_by_key(|(partition, _)| *partition);
        let mut first_error = None;
        for (partition, result) in sealed {
            match result.and_then(|spill| spill.account(evidence)) {
                Ok(receipt) => self.sealed[partition].push(receipt),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Seal every open spill with a private durability batch.
    ///
    /// The spills seal into one directory-durability batch (#1452): every
    /// artifact and receipt rename lands in the partition directory during
    /// the loop, and one flush at the end makes the whole batch durable
    /// together. A crash before the flush is the incomplete-shape state the
    /// recovery contract already cleans and re-runs.
    pub(super) fn seal(
        &mut self,
        boundary: u64,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let mut batch = SealDirectoryBatch::new(self.root);
        self.seal_at_boundary(boundary, evidence, &mut batch)?;
        construction_failpoint("shape.partition_spill.before_flush");
        batch.flush(evidence)
    }

    /// Sort each partition and concatenate them into one globally ordered run.
    ///
    /// `boundary` names the final segment set when open spills remain (a
    /// shape whose routing ended without crossing a sealing boundary).
    ///
    /// Returns `None` when no record was routed.
    ///
    /// With `retain_segments`, sealed segments are **not** unlinked here: they
    /// remain the family's resume authority until a finish stage hands it to
    /// the installed output (#1418, #1562).
    pub(super) fn finish_optional(
        self,
        output: &str,
        boundary: u64,
        retain_segments: bool,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<Option<String>, GfError> {
        self.finish_with_segments(output, boundary, retain_segments, cancelled, evidence)
            .map(|(output, _)| output)
    }

    /// [`Self::finish_optional`] that retains every sealed segment and returns
    /// their receipts, for the caller to retire once a finish stage records
    /// the installed output as their successor (#1562).
    pub(super) fn finish_retaining(
        self,
        output: &str,
        boundary: u64,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(Option<String>, Vec<ArtifactReceipt>), GfError> {
        self.finish_with_segments(output, boundary, true, cancelled, evidence)
    }

    /// Every sealed segment receipt, in partition then boundary order.
    pub(super) fn sealed_segments(&self) -> Vec<ArtifactReceipt> {
        self.sealed.iter().flatten().cloned().collect()
    }

    #[allow(clippy::too_many_lines)] // One publication lifecycle; the cleanup arms are the invariant.
    fn finish_with_segments(
        mut self,
        output: &str,
        boundary: u64,
        retain_segments: bool,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(Option<String>, Vec<ArtifactReceipt>), GfError> {
        let root = self.root;
        self.seal(boundary, evidence)?;
        if self.records == 0 {
            return Ok((None, self.sealed_segments()));
        }
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(self.family.as_str(), self.records);
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic = super::diagnostics::Group::start(evidence);
        #[cfg(any(test, feature = "test-support"))]
        let partitions = self.sealed.iter().filter(|slot| !slot.is_empty()).count();
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
        // The callback is polled from three places on this thread — the
        // consume boundary, the record loop and the load coordinator's wait
        // (#1581) — and never two at once, so it lives behind a `RefCell`.
        let cancelled = RefCell::new(cancelled);
        let concatenated = (|| -> Result<u64, GfError> {
            let mut previous: Option<[u8; 16]> = None;
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
            let mut jobs = Vec::new();
            for (partition, segments) in self.sealed.iter().enumerate() {
                if segments.is_empty() {
                    continue;
                }
                let names = segments
                    .iter()
                    .map(|receipt| receipt.name.clone())
                    .collect::<Vec<_>>();
                jobs.push((
                    partition,
                    names,
                    self.balance.rows().get(partition).copied(),
                ));
            }
            let consume =
                |job: usize, (records, counters): (LoadedPartition<N>, PartitionLoadCounters)| {
                    let partition = jobs[job].0;
                    reject_cancelled(&mut **cancelled.borrow_mut())?;
                    // The ordered critical section: fold the worker's local
                    // counters into the shared evidence, then write the partition.
                    counters.merge_into(evidence)?;
                    injected_input_release_failure()?;
                    records.for_each_record(|record| {
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
                        let wire = record;
                        writer.write_all(wire).map_err(super::storage)?;
                        account_merge_write_bytes(evidence, wire.len() as u64)?;
                        previous = Some(record[..16].try_into().expect("UUID prefix"));
                        written = written.checked_add(1).ok_or_else(|| {
                            super::storage("partition output record count overflows")
                        })?;
                        if written.is_multiple_of(4096) {
                            reject_cancelled(&mut **cancelled.borrow_mut())?;
                        }
                        Ok(())
                    })
                };
            // The coordinator polls the same callback while it waits for a
            // partition, so a cancel is not held hostage by the head load.
            let mut cancelled_while_waiting =
                || reject_cancelled(&mut **cancelled.borrow_mut()).is_err();
            self.schedule_loads(&jobs, &mut cancelled_while_waiting, consume)?;
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
        // When no staged input was retired behind a progress boundary, the
        // segments have no resume role and are freed here, exactly as the
        // per-family finish always did (#1418). Otherwise supersession owns
        // their retirement, so an interrupted finish can still resume.
        if !retain_segments {
            for partition in 0..self.sealed.len() {
                for receipt in self.sealed[partition].drain(..) {
                    unlink_shape_artifact(root, &receipt.name, evidence)?;
                }
            }
        }
        Ok((Some(output.to_owned()), self.sealed_segments()))
    }
}

/// A loaded fixed-width partition, handed from a load worker to the
/// coordinator. An over-budget partition is sorted into on-disk runs and
/// merged instead of resident (#1585).
pub(super) enum LoadedPartition<const N: usize> {
    Resident(PartitionRecords<N>),
    /// An over-budget partition sorted into on-disk runs (#1585).
    Runs(Box<super::external_partition::ExternalPartition<N>>),
}

impl<const N: usize> LoadedPartition<N> {
    /// Hand every record to `consume` in sorted order.
    fn for_each_record(
        self,
        consume: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        match self {
            Self::Resident(records) => records.iter().try_for_each(consume),
            Self::Runs(runs) => runs.for_each_record(consume),
        }
    }
}

/// Combined on-disk length of a partition's sealed segments.
fn segment_bytes(root: &StableDirectory, names: &[String]) -> Result<u64, GfError> {
    names.iter().try_fold(0_u64, |total, name| {
        let length = root
            .open_child_file(OsStr::new(name))
            .and_then(|file| file.metadata())
            .map_err(super::storage)?
            .len();
        total
            .checked_add(length)
            .ok_or_else(|| super::storage("partition spill byte count overflows"))
    })
}

/// Whether a codec-free partition's resident materialization fits
/// `max_partition_bytes`: the admission `load_fixed_partition` applies, taken
/// before anything is allocated (#1585).
fn fits_resident<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: Option<u64>,
    max_partition_bytes: u64,
) -> Result<bool, GfError> {
    // The routed count decides without I/O. Only a load without one reads the
    // segment lengths; either load re-checks the count against the records.
    let count = if let Some(count) = expected_records {
        count
    } else {
        segment_bytes(root, names)? / N as u64
    };
    Ok(
        super::partition::admit_materialization(count.checked_mul(N as u64), max_partition_bytes)
            .is_ok(),
    )
}

/// Read one fixed-width partition's sealed segments into memory and sort them,
/// on a worker.
///
/// This is the materialization the design's rule R4 requires: the sorted
/// partition exists in full before any of it is written, so row-group and
/// record boundaries are a pure function of row count, never of arrival.
///
/// The shared evidence is never touched here. Everything the load observes
/// comes back as [`PartitionLoadCounters`] for the coordinator to merge, in
/// partition order, once it consumes the result.
///
/// Pre-size from the routing row count and the combined segment length.
/// Details retain only compact wire bytes and one offset per record; other
/// families keep their fixed-width arrays. Sorting does not change either wire
/// representation. Segments are read in boundary order, which is routing
/// order; the sort below is the only order authority.
pub(super) fn load_fixed_partition<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    max_partition_bytes: u64,
    stop: &AtomicBool,
) -> Result<(PartitionRecords<N>, PartitionLoadCounters), GfError> {
    if names.is_empty() {
        return Err(super::storage("partition has no sealed segments"));
    }
    let mut spill_bytes = 0_u64;
    for name in names {
        let file = root
            .open_child_file(OsStr::new(name))
            .map_err(super::storage)?;
        spill_bytes = spill_bytes
            .checked_add(file.metadata().map_err(super::storage)?.len())
            .ok_or_else(|| super::storage("partition spill byte count overflows"))?;
    }
    let mut counters = PartitionLoadCounters {
        spill_bytes,
        ..PartitionLoadCounters::default()
    };
    let loaded = (|| -> Result<PartitionRecords<N>, GfError> {
        if let Some(codec) = codec {
            codec.validate_size(N, 0, 0).map_err(super::storage)?;
        }
        let count = expected_records.unwrap_or(
            spill_bytes
                / if codec.is_some() {
                    (N - 255) as u64
                } else {
                    N as u64
                },
        );
        let retained = if codec.is_some() {
            count
                .checked_mul(std::mem::size_of::<usize>() as u64)
                .and_then(|offsets| spill_bytes.checked_add(offsets))
        } else {
            count.checked_mul(N as u64)
        };
        super::partition::admit_materialization(retained, max_partition_bytes)?;
        let mut records = PartitionRecords::new(codec, Some(count), spill_bytes)?;
        let mut admitted_wire_bytes = 0_u64;
        for name in names {
            let (mut reader, counter, _segment_bytes) = open_fixed_reader(root, name)?;
            let segment_loaded = (|| -> Result<(), GfError> {
                while let Some(record) = read_run_record::<N>(&mut reader, codec)? {
                    if records.len() as u64 >= count {
                        return Err(super::storage("partition exceeds admitted record count"));
                    }
                    let wire_bytes = if codec.is_some() {
                        N - 255 + usize::from(record[N - 256])
                    } else {
                        N
                    };
                    admitted_wire_bytes = admitted_wire_bytes
                        .checked_add(wire_bytes as u64)
                        .ok_or_else(|| super::storage("partition wire byte count overflows"))?;
                    if admitted_wire_bytes > spill_bytes {
                        return Err(super::storage("partition exceeds admitted wire bytes"));
                    }
                    records.push(record);
                    abandon_if_stopped(records.len(), stop)?;
                }
                Ok(())
            })();
            let released = reader
                .get_mut()
                .inner
                .finish()
                .map_err(super::storage)
                .and_then(|evidence_release| {
                    super::merge_cache_release_evidence(
                        &mut counters.cache_release,
                        evidence_release,
                    )
                });
            combine_cache_cleanup(segment_loaded, released, "partition spill")?;
            let (segment_read_bytes, segment_read_operations) = counter.values();
            counters.read_bytes = counters
                .read_bytes
                .checked_add(segment_read_bytes)
                .ok_or_else(|| super::storage("partition read byte count overflows"))?;
            counters.read_operations = counters
                .read_operations
                .checked_add(segment_read_operations)
                .ok_or_else(|| super::storage("partition read operation count overflows"))?;
        }
        if expected_records.is_some_and(|expected| records.len() as u64 != expected) {
            return Err(super::storage(
                "partition differs from admitted record count",
            ));
        }
        Ok(records)
    })();
    let mut records = combine_cache_cleanup(loaded, Ok(()), "partition spill")?;
    counters.records = records.len() as u64;
    // The sort key is the whole record, whose leading 16 bytes are the
    // UUID. Records are globally unique on that prefix, so this is a total
    // order and no stability assumption is needed.
    records.sort();
    Ok((records, counters))
}

/// Routes normalized Arrow rows into per-partition spill segments, then emits
/// one sorted Parquet artifact with pinned writer properties.
pub(super) struct RowRangePartitioner<'a> {
    root: &'a StableDirectory,
    #[cfg(any(test, feature = "test-support"))]
    diagnostic_family: String,
    namespace: String,
    schema: Option<SchemaRef>,
    window: std::num::NonZeroU64,
    spills: Vec<Option<RowSpill>>,
    sealed: Vec<Vec<ArtifactReceipt>>,
    balance: PartitionBalance,
    rows: u64,
    reservations: Vec<super::partition_memory::RowReservation>,
    max_partition_bytes: u64,
}

struct RowSpill {
    writer: arrow::ipc::writer::StreamWriter<BufWriter<HashingWriter>>,
    temporary: std::ffi::OsString,
    identity: FileIdentity,
}

impl Drop for RowRangePartitioner<'_> {
    fn drop(&mut self) {
        for slot in &mut self.spills {
            if let Some(spill) = slot.take() {
                let RowSpill {
                    writer,
                    temporary,
                    identity,
                } = spill;
                drop(writer);
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
            sealed: (0..partitions).map(|_| Vec::new()).collect(),
            balance: PartitionBalance::new(partitions),
            rows: 0,
            reservations: vec![super::partition_memory::RowReservation::default(); partitions],
            max_partition_bytes: super::partition::default_materialization_bytes(),
        })
    }

    pub(super) fn with_materialization_limit(mut self, bytes: u64) -> Self {
        self.max_partition_bytes = bytes;
        self
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
        let slice = batch.slice(offset, length);
        let reservation = self
            .reservations
            .get_mut(partition)
            .ok_or_else(|| super::storage("row reservation partition is out of range"))?;
        let next = reservation.with_batch(&slice)?;
        next.admit(0, self.max_partition_bytes)?;
        *reservation = next;
        let slot = self
            .spills
            .get_mut(partition)
            .ok_or_else(|| super::storage("routed row partition is out of range"))?;
        if slot.is_none() {
            let name = row_spill_name(&self.namespace, UNSEALED_BOUNDARY, partition);
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
                temporary,
                identity,
            });
        }
        let spill = slot
            .as_mut()
            .ok_or_else(|| super::storage("row partition spill is absent"))?;
        spill.writer.write(&slice).map_err(super::storage)?;
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

    /// How many row spills are currently open and unsealed.
    pub(super) fn open_spill_count(&self) -> usize {
        self.spills.iter().filter(|slot| slot.is_some()).count()
    }

    /// Measured per-partition row counts.
    pub(super) fn balance(&self) -> &PartitionBalance {
        &self.balance
    }

    /// Adopt the sealed segments and cumulative routing balance restored from
    /// an interrupted shape (#1418). See [`FixedRangePartitioner::restore`].
    pub(super) fn restore(
        &mut self,
        segments: &[Vec<ArtifactReceipt>],
        balance_rows: &[u64],
    ) -> Result<(), GfError> {
        if segments.len() > self.sealed.len() || balance_rows.len() != self.sealed.len() {
            return Err(super::storage(
                "restored row partition inventory differs from the plan",
            ));
        }
        for (slot, segments) in self.sealed.iter_mut().zip(segments) {
            slot.extend(segments.iter().cloned());
        }
        let mut total = 0_u64;
        for (partition, rows) in balance_rows.iter().enumerate() {
            self.balance.record_many(partition, *rows)?;
            total = total
                .checked_add(*rows)
                .ok_or_else(|| super::storage("row partition count overflows"))?;
        }
        self.rows = total;
        Ok(())
    }

    /// Install the Arrow IPC schema carried by restored row segments, for a
    /// resumed partitioner whose remaining routing may push no rows.
    pub(super) fn restore_schema(&mut self, schema: SchemaRef) {
        self.schema.get_or_insert(schema);
    }

    /// Seal every open row spill into this boundary's segment set, sharing one
    /// directory-durability batch (#1418, #1452).
    pub(super) fn seal_at_boundary(
        &mut self,
        boundary: u64,
        evidence: &mut GraphConstructionEvidence,
        batch: &mut SealDirectoryBatch,
    ) -> Result<(), GfError> {
        for partition in 0..self.spills.len() {
            let Some(spill) = self.spills[partition].take() else {
                continue;
            };
            let RowSpill {
                writer,
                temporary,
                identity,
            } = spill;
            let buffered = writer.into_inner().map_err(super::storage)?;
            let name = row_spill_name(&self.namespace, boundary, partition);
            let receipt = SpillWriter {
                temporary,
                identity,
                writer: buffered,
            }
            .seal(&name, self.root, evidence, batch)?;
            self.sealed[partition].push(receipt);
        }
        Ok(())
    }

    /// Seal every open row spill.
    ///
    /// Same batched durability as the fixed-width seal: the whole group of
    /// row spills shares one directory flush (#1452).
    fn seal(
        &mut self,
        boundary: u64,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let mut batch = SealDirectoryBatch::new(self.root);
        self.seal_at_boundary(boundary, evidence, &mut batch)?;
        construction_failpoint("shape.partition_spill.before_flush");
        batch.flush(evidence)
    }

    /// Sort each partition and write one globally ordered Parquet artifact.
    ///
    /// `boundary` names the final segment set when open spills remain (#1418).
    /// Sealed segments are **not** unlinked here: they remain until
    /// supersession retires them (#1418).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One publication lifecycle.
    pub(super) fn finish(
        mut self,
        output: &str,
        boundary: u64,
        retain_segments: bool,
        output_rows: usize,
        output_bytes: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<String, GfError> {
        if output_rows == 0 || output_bytes == 0 {
            return Err(super::storage("invalid row partition group"));
        }
        let root = self.root;
        self.seal(boundary, evidence)?;
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(&self.diagnostic_family, self.rows);
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic = super::diagnostics::Group::start(evidence);
        #[cfg(any(test, feature = "test-support"))]
        let partitions = self.sealed.iter().filter(|slot| !slot.is_empty()).count();
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
                if self.sealed[partition].is_empty() {
                    continue;
                }
                reject_cancelled(cancelled)?;
                let names = self.sealed[partition]
                    .iter()
                    .map(|receipt| receipt.name.clone())
                    .collect::<Vec<_>>();
                let sorted = self.load_partition(partition, &names, &schema, evidence)?;
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
        #[cfg(any(test, feature = "test-support"))]
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(&self.diagnostic_family, 1, partitions, evidence, true);
        }
        if !retain_segments {
            for partition in 0..self.sealed.len() {
                for receipt in self.sealed[partition].drain(..) {
                    unlink_shape_artifact(root, &receipt.name, evidence)?;
                }
            }
        }
        Ok(output.to_owned())
    }

    /// Read one row partition's sealed segments into memory and sort them by
    /// UUID.
    ///
    /// Segments are read in boundary order and concatenated before the sort,
    /// so a partition routed across several sealing boundaries loads exactly
    /// as one spill would (#1418).
    #[allow(clippy::too_many_lines)] // One authenticated multi-segment load; bounds before decode is the invariant.
    fn load_partition(
        &self,
        partition: usize,
        names: &[String],
        schema: &SchemaRef,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<RecordBatch, GfError> {
        if names.is_empty() {
            return Err(super::storage("row partition has no sealed segments"));
        }
        let mut segment_bytes = 0_u64;
        let mut receipts = Vec::with_capacity(names.len());
        for name in names {
            let receipt = self
                .sealed
                .get(partition)
                .and_then(|segments| segments.iter().find(|receipt| &receipt.name == name))
                .ok_or_else(|| super::storage("row partition receipt is absent"))?;
            segment_bytes = segment_bytes
                .checked_add(receipt.bytes)
                .ok_or_else(|| super::storage("row partition byte count overflows"))?;
            receipts.push(receipt.clone());
        }
        self.reservations[partition].admit(segment_bytes, self.max_partition_bytes)?;
        // Authenticate with bounded buffers BEFORE Arrow reads bodyLength and
        // allocates an IPC body. Post-decode checks cannot protect that step.
        let mut authenticated = Vec::with_capacity(receipts.len());
        let mut work = ReadWork::default();
        for receipt in &receipts {
            let (file, segment_work) = super::recovery::authenticate_row_spill(self.root, receipt)?;
            work.bytes = work
                .bytes
                .checked_add(segment_work.bytes)
                .ok_or_else(|| super::storage("partition read bytes overflow"))?;
            work.operations = work
                .operations
                .checked_add(segment_work.operations)
                .ok_or_else(|| super::storage("partition read operations overflow"))?;
            merge_cache_release_evidence(&mut work.cache_release, segment_work.cache_release)?;
            authenticated.push(file);
        }
        account_cache_release(work.cache_release, evidence)?;
        evidence.merge_read_bytes = evidence
            .merge_read_bytes
            .checked_add(work.bytes)
            .ok_or_else(|| super::storage("partition read bytes overflow"))?;
        evidence.merge_read_operations = evidence
            .merge_read_operations
            .checked_add(work.operations)
            .ok_or_else(|| super::storage("partition read operations overflow"))?;

        let counter = IoCounter::default();
        let mut readers = Vec::with_capacity(authenticated.len());
        for file in authenticated {
            readers.push(std::io::BufReader::with_capacity(
                BLOCK_BYTES,
                super::CountingRead {
                    inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                        file,
                        self.window,
                        graphforge_filesystem::FileCacheReleaseTracker::default(),
                    )
                    .map_err(super::storage)?,
                    counter: counter.clone(),
                },
            ));
        }
        let loaded = (|| -> Result<RecordBatch, GfError> {
            let mut batches = Vec::new();
            let mut decoded = super::partition_memory::RowReservation::default();
            for reader in &mut readers {
                let stream = arrow::ipc::reader::StreamReader::try_new(reader, None)
                    .map_err(super::storage)?;
                for batch in stream {
                    let batch = batch.map_err(super::storage)?;
                    decoded = decoded.with_batch(&batch)?;
                    batches.push(batch);
                }
            }
            decoded.admit(segment_bytes, self.max_partition_bytes)?;
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
        let mut pending_releases: Vec<graphforge_filesystem::FileCacheReleaseEvidence> =
            Vec::with_capacity(readers.len());
        let mut release_error = Ok(());
        for reader in readers.iter_mut().rev() {
            match reader.get_mut().inner.finish().map_err(super::storage) {
                Ok(evidence_release) => pending_releases.push(evidence_release),
                Err(error) => release_error = Err(error),
            }
        }
        let released = release_error.and_then(|()| {
            let mut merged = graphforge_filesystem::FileCacheReleaseEvidence::default();
            for evidence_release in pending_releases {
                merge_cache_release_evidence(&mut merged, evidence_release)?;
            }
            account_cache_release(merged, evidence)
        });
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

#[cfg(test)]
mod tests;
