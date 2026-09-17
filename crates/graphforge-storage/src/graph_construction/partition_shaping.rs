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
//! # Concurrency
//!
//! Loading and sorting a partition is independent of every other partition:
//! nothing here shares mutable state between them. [`load_partitions_ordered`]
//! exploits that by materializing partitions on a bounded worker pool while
//! still delivering each sorted partition to the writer in ascending index
//! order — the same order the sequential implementation visited them in, so
//! concatenation, digests and evidence totals are unaffected by how many
//! workers ran. Only the write side (`finish`/`finish_optional`) stays on one
//! thread, because the writer's byte stream, not the loader, is what has to be
//! ordered.
//!
//! The worker count is a scheduling decision, never a recorded format
//! parameter (see [`shaping_worker_count`] and the partition-count note in
//! [`super::partition`]): the same logical input produces the same partition
//! plan and the same concatenated bytes no matter how many threads happened to
//! load them.
//!
//! **Memory bound.** The channel between workers and the orchestrating thread
//! is a rendezvous (capacity 0), so a worker's send blocks until the
//! orchestrator receives it. A result that arrives out of turn waits in a
//! reorder buffer; because there are only `threads` workers, at most
//! `threads` results can be ahead of the one the orchestrator is waiting for.
//! So at most `threads` partitions are being actively computed plus at most
//! `threads` completed results are buffered awaiting their turn: peak
//! resident partition memory is bounded by `2 * threads *
//! max_partition_bytes`, a constant multiple of the worker count, never of
//! the partition count.

use super::partition::{PartitionBalance, PartitionPlan};
use super::{
    ArtifactReceipt, BLOCK_BYTES, CountingChunkReader, GraphConstructionEvidence, HashingWriter,
    IoCounter, account_cache_release, account_fixed_read_operations,
    account_fixed_write_operations, account_merge_read_bytes, account_merge_write_bytes,
    account_sequential_write, artifact_temp, cleanup_failed_shape_output,
    cleanup_shape_publication, combine_cache_cleanup, combine_secondary_cleanup,
    construction_failpoint, hex, open_counted_fixed_reader, persist_shape_receipt, read_run_record,
    record_shape_artifact_install, reject_cancelled, release_counted_reader_cache,
    run_record_bytes, sha256, shape_publication_failure, shape_publication_io_failure,
    unlink_shape_artifact, unlink_writer_capability, uuid_column, uuid_value,
};
use crate::construction_detail_codec::DetailCodec;
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use arrow::array::{Array, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::SchemaRef;
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, file_identity};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::Digest;
use std::ffi::OsStr;
use std::io::{BufWriter, Write};

/// Per-spill write buffer. Every partition holds one open spill during the
/// routing pass, so this is multiplied by the partition count: it is
/// deliberately far below `BLOCK_BYTES`, which is sized for the handful of
/// streams a merge group used to open.
const SPILL_BLOCK_BYTES: usize = 64 * 1024;

/// Worker count for concurrently materializing sealed partitions.
///
/// Bounded by the host's available parallelism and by the number of
/// partitions actually worth loading, never by `partition_count` itself
/// (that stays a recorded format parameter; see [`super::partition`]).
/// Deliberately not overridable by an environment variable in production: a
/// resumed or re-run import must reproduce the same bytes on a
/// differently-sized host, and this function only ever chooses *how many
/// threads* load the same partitions in the same order, never *which*
/// partitions exist. [`test_support::set_worker_count_override`] provides a
/// test-only knob (compiled only under `cfg(test)`/`test-support`) so the
/// determinism suite can force adverse thread counts without depending on
/// the host's core count.
fn shaping_worker_count(work_items: usize) -> usize {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(threads) = test_support::worker_count_override() {
        return threads.clamp(1, work_items.max(1));
    }
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(work_items.max(1))
}

/// Whether the current partition load should sleep briefly before returning,
/// scrambling completion order relative to partition index. Test-only: used
/// to force adverse worker/writer interleaving in the determinism suite
/// rather than relying on incidental scheduling luck.
#[cfg(any(test, feature = "test-support"))]
fn shaping_worker_jitter(index: usize) {
    if test_support::jitter_enabled() {
        // A deterministic, index-dependent stagger: partitions do not
        // complete in index order, which is exactly the interleaving that
        // would expose a broken reorder buffer.
        std::thread::sleep(std::time::Duration::from_micros(((index * 37) % 900) as u64));
    }
}

/// Test-only knobs for [`shaping_worker_count`] and [`shaping_worker_jitter`].
///
/// Both are thread-local so tests running concurrently (`cargo test` without
/// `--test-threads=1`) do not interfere with each other, and both default to
/// "no override" / "no jitter" so every path outside a test that explicitly
/// sets them behaves exactly as production does.
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support {
    use std::cell::Cell;

    thread_local! {
        static WORKER_COUNT_OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
        static JITTER_ENABLED: Cell<bool> = const { Cell::new(false) };
    }

    /// Force [`super::shaping_worker_count`] to return `threads` (clamped to
    /// the item count) on the calling thread, until cleared with `None`.
    pub(crate) fn set_worker_count_override(threads: Option<usize>) {
        WORKER_COUNT_OVERRIDE.with(|cell| cell.set(threads));
    }

    pub(super) fn worker_count_override() -> Option<usize> {
        WORKER_COUNT_OVERRIDE.with(Cell::get)
    }

    /// Enable or disable the completion-order jitter on the calling thread.
    pub(crate) fn set_jitter_enabled(enabled: bool) {
        JITTER_ENABLED.with(|cell| cell.set(enabled));
    }

    pub(super) fn jitter_enabled() -> bool {
        JITTER_ENABLED.with(Cell::get)
    }
}

/// Merge one worker's partition-load evidence delta into the shared,
/// sequentially-accumulated evidence.
///
/// Every field a partition load can touch is either a sum (`checked_add`,
/// order-independent) or a running maximum (`max`, also order-independent),
/// so the merged totals are identical to what the sequential loader would
/// have produced, regardless of which worker finished first.
fn merge_partition_load_evidence(
    delta: &GraphConstructionEvidence,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    evidence.merge_read_records = evidence
        .merge_read_records
        .checked_add(delta.merge_read_records)
        .ok_or_else(|| super::storage("merge read record count overflows"))?;
    evidence.merge_read_bytes = evidence
        .merge_read_bytes
        .checked_add(delta.merge_read_bytes)
        .ok_or_else(|| super::storage("merge read byte count overflows"))?;
    evidence.merge_read_operations = evidence
        .merge_read_operations
        .checked_add(delta.merge_read_operations)
        .ok_or_else(|| super::storage("merge read operation count overflows"))?;
    evidence.parquet_read_bytes = evidence
        .parquet_read_bytes
        .checked_add(delta.parquet_read_bytes)
        .ok_or_else(|| super::storage("Parquet read byte count overflows"))?;
    evidence.parquet_read_operations = evidence
        .parquet_read_operations
        .checked_add(delta.parquet_read_operations)
        .ok_or_else(|| super::storage("Parquet read operation count overflows"))?;
    evidence.cache_release_operations = evidence
        .cache_release_operations
        .checked_add(delta.cache_release_operations)
        .ok_or_else(|| super::storage("cache release operations overflows"))?;
    evidence.cache_release_unsupported_operations = evidence
        .cache_release_unsupported_operations
        .checked_add(delta.cache_release_unsupported_operations)
        .ok_or_else(|| super::storage("cache release unsupported operations overflows"))?;
    evidence.cache_released_bytes = evidence
        .cache_released_bytes
        .checked_add(delta.cache_released_bytes)
        .ok_or_else(|| super::storage("cache released bytes overflows"))?;
    evidence.peak_cache_release_window_bytes = evidence
        .peak_cache_release_window_bytes
        .max(delta.peak_cache_release_window_bytes);
    evidence.peak_partition_records = evidence
        .peak_partition_records
        .max(delta.peak_partition_records);
    Ok(())
}

/// `(partition index, spill name)` for every sealed, non-empty partition, in
/// ascending index order.
fn sealed_items(sealed: &[Option<ArtifactReceipt>]) -> Vec<(usize, String)> {
    sealed
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| slot.as_ref().map(|receipt| (index, receipt.name.clone())))
        .collect()
}

/// Materialize every `(index, name)` in `items` with `load`, on up to
/// [`shaping_worker_count`] OS threads, and deliver each result to `consume`
/// strictly in ascending partition-index order. See the module-level
/// "Concurrency" section for the ordering and memory-bound argument.
///
/// `load` must be safe to call concurrently from multiple threads for
/// distinct names; it is never called twice for the same name. `consume` runs
/// only on the orchestrating thread, in index order, so it is free to hold
/// `&mut` state (a writer, a running digest, an evidence accumulator) exactly
/// as the sequential implementation did.
fn load_partitions_ordered<T: Send>(
    items: &[(usize, String)],
    cancelled: &mut impl FnMut() -> bool,
    load: impl Fn(&str) -> Result<(T, GraphConstructionEvidence), GfError> + Sync,
    mut consume: impl FnMut(usize, T, &GraphConstructionEvidence) -> Result<(), GfError>,
) -> Result<(), GfError> {
    if items.is_empty() {
        return Ok(());
    }
    let threads = shaping_worker_count(items.len());
    if threads <= 1 {
        for (index, name) in items {
            reject_cancelled(cancelled)?;
            let (value, delta) = load(name)?;
            consume(*index, value, &delta)?;
        }
        return Ok(());
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let abort = std::sync::atomic::AtomicBool::new(false);
    // Rendezvous channel: see the memory-bound argument in the module doc.
    let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, Result<(T, GraphConstructionEvidence), GfError>)>(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let tx = tx.clone();
            let load = &load;
            let next = &next;
            let abort = &abort;
            scope.spawn(move || {
                loop {
                    if abort.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    let slot = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((index, name)) = items.get(slot) else {
                        break;
                    };
                    let result = load(name);
                    let failed = result.is_err();
                    if tx.send((*index, result)).is_err() {
                        break;
                    }
                    if failed {
                        break;
                    }
                }
            });
        }
        drop(tx);
        let mut pending_values: std::collections::BTreeMap<usize, T> =
            std::collections::BTreeMap::new();
        let mut pending_deltas: std::collections::BTreeMap<usize, GraphConstructionEvidence> =
            std::collections::BTreeMap::new();
        let mut wanted = items.iter().map(|(index, _)| *index);
        let mut next_wanted = wanted.next();
        let mut outcome: Result<(), GfError> = Ok(());
        for (index, result) in &rx {
            if outcome.is_err() {
                // Keep draining so blocked senders can make progress and the
                // scope can join, but stop doing any further work.
                continue;
            }
            let value = match result {
                Ok((value, delta)) => {
                    pending_deltas.insert(index, delta);
                    value
                }
                Err(error) => {
                    outcome = Err(error);
                    abort.store(true, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
            };
            pending_values.insert(index, value);
            while let Some(want) = next_wanted {
                let (Some(value), Some(delta)) =
                    (pending_values.remove(&want), pending_deltas.remove(&want))
                else {
                    break;
                };
                if let Err(error) =
                    reject_cancelled(cancelled).and_then(|()| consume(want, value, &delta))
                {
                    outcome = Err(error);
                    abort.store(true, std::sync::atomic::Ordering::Relaxed);
                    break;
                }
                next_wanted = wanted.next();
            }
        }
        outcome
    })
}

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
    fn create(
        root: &StableDirectory,
        name: String,
        window: std::num::NonZeroU64,
    ) -> Result<Self, GfError> {
        let temporary = artifact_temp(&name);
        let file = root
            .create_replaceable_child_file(temporary.as_os_str())
            .map_err(super::storage)?;
        let identity = file_identity(&file).map_err(super::storage)?;
        let hashing = HashingWriter::with_cache_window(file, window)?;
        Ok(Self {
            name,
            temporary,
            identity,
            writer: BufWriter::with_capacity(SPILL_BLOCK_BYTES, hashing),
        })
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), GfError> {
        self.writer.write_all(bytes).map_err(super::storage)
    }

    /// Drop an unsealed spill and remove its temporary.
    ///
    /// A shaping pass that fails part-way must not leave owned temporaries
    /// behind: the identity-checked unlink here is the same one the session's
    /// reopen cleanup would eventually perform, done immediately instead.
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

/// Routes fixed-width records into per-partition spills, then emits one sorted
/// artifact by sorting each partition and concatenating them in index order.
pub(super) struct FixedRangePartitioner<'a, const N: usize> {
    root: &'a StableDirectory,
    family: PartitionFamily,
    codec: Option<DetailCodec>,
    reject_duplicates: bool,
    window: std::num::NonZeroU64,
    spills: Vec<Option<SpillWriter>>,
    sealed: Vec<Option<ArtifactReceipt>>,
    balance: PartitionBalance,
    records: u64,
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
        })
    }

    /// Route one record into the partition owning `key`.
    pub(super) fn route(
        &mut self,
        plan: &PartitionPlan,
        key: &[u8; 16],
        record: &[u8; N],
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let partition = plan.partition_of(key);
        let slot = self
            .spills
            .get_mut(partition)
            .ok_or_else(|| super::storage("routed partition is out of range"))?;
        if slot.is_none() {
            *slot = Some(SpillWriter::create(
                self.root,
                fixed_spill_name(self.family, partition),
                self.window,
            )?);
        }
        let wire = run_record_bytes(record, self.codec)?;
        slot.as_mut()
            .ok_or_else(|| super::storage("partition spill is absent"))?
            .write(wire)?;
        account_merge_read_bytes(evidence, wire.len() as u64)?;
        account_merge_write_bytes(evidence, wire.len() as u64)?;
        self.balance.record(partition)?;
        self.records = self
            .records
            .checked_add(1)
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
        let items = sealed_items(&self.sealed);
        let root = self.root;
        let codec = self.codec;
        let reject_duplicates = self.reject_duplicates;
        let concatenated = (|| -> Result<u64, GfError> {
            let mut previous: Option<[u8; N]> = None;
            let mut written = 0_u64;
            load_partitions_ordered(
                &items,
                cancelled,
                |name| Self::load_partition(root, codec, name),
                |_index, records, delta| {
                    merge_partition_load_evidence(delta, evidence)?;
                    for record in &records {
                        // Partition order is key order, so the concatenation is
                        // the global order. Prove it rather than assume it: this
                        // is the invariant the external merge's heap used to
                        // provide, and it holds regardless of which worker
                        // loaded which partition, because consumption here is
                        // always in ascending partition-index order.
                        if let Some(prior) = previous.as_ref() {
                            if prior[..16] > record[..16] {
                                return Err(super::storage(
                                    "range partition concatenation is not globally ordered",
                                ));
                            }
                            if reject_duplicates && prior[..16] == record[..16] {
                                return Err(super::storage(
                                    "duplicate identity across construction runs",
                                ));
                            }
                        }
                        let wire = run_record_bytes(record, codec)?;
                        writer.write_all(wire).map_err(super::storage)?;
                        account_merge_write_bytes(evidence, wire.len() as u64)?;
                        previous = Some(*record);
                        written = written.checked_add(1).ok_or_else(|| {
                            super::storage("partition output record count overflows")
                        })?;
                    }
                    Ok(())
                },
            )?;
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

    /// Read one partition spill into memory and sort it.
    ///
    /// This is the materialization the design's rule R4 requires: the sorted
    /// partition exists in full before any of it is written, so row-group and
    /// record boundaries are a pure function of row count, never of arrival.
    ///
    /// Takes `root`/`codec` rather than `&self` so it can run on a worker
    /// thread in [`load_partitions_ordered`]: it touches nothing but the
    /// directory handle, the codec and its own freshly-zeroed evidence delta,
    /// which the caller merges into the live evidence back on the
    /// orchestrating thread.
    fn load_partition(
        root: &StableDirectory,
        codec: Option<DetailCodec>,
        name: &str,
    ) -> Result<(Vec<[u8; N]>, GraphConstructionEvidence), GfError> {
        let mut evidence = GraphConstructionEvidence::default();
        let (mut reader, counter) = open_counted_fixed_reader(root, name, &mut evidence)?;
        let loaded = (|| -> Result<Vec<[u8; N]>, GfError> {
            let mut records = Vec::new();
            while let Some(record) = read_run_record::<N>(&mut reader, codec)? {
                account_merge_read_bytes(
                    &mut evidence,
                    run_record_bytes(&record, codec)?.len() as u64,
                )?;
                records.push(record);
            }
            Ok(records)
        })();
        let released = release_counted_reader_cache(&mut reader, &mut evidence);
        let mut records = combine_cache_cleanup(loaded, released, "partition spill")?;
        account_fixed_read_operations(&counter, &mut evidence)?;
        evidence.peak_partition_records = evidence.peak_partition_records.max(records.len() as u64);
        // The sort key is the whole record, whose leading 16 bytes are the
        // UUID. Records are globally unique on that prefix, so this is a total
        // order and no stability assumption is needed.
        records.sort_unstable();
        Ok((records, evidence))
    }
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
        let items = sealed_items(&self.sealed);
        let root = self.root;
        let window = self.window;
        let written = (|| -> Result<u64, GfError> {
            let mut written = 0_u64;
            load_partitions_ordered(
                &items,
                cancelled,
                |name| Self::load_partition(root, window, name, &schema),
                |_index, sorted, delta| {
                    merge_partition_load_evidence(delta, evidence)?;
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
                    write_bounded_row_groups(&mut writer, &sorted, output_rows, output_bytes, evidence)
                },
            )?;
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
    ///
    /// Takes `root`/`window`/`schema` rather than `&self` so it can run on a
    /// worker thread in [`load_partitions_ordered`]: like the fixed-width
    /// loader, it accumulates into its own freshly-zeroed evidence delta,
    /// which the caller merges back into the live evidence on the
    /// orchestrating thread.
    fn load_partition(
        root: &StableDirectory,
        window: std::num::NonZeroU64,
        name: &str,
        schema: &SchemaRef,
    ) -> Result<(RecordBatch, GraphConstructionEvidence), GfError> {
        let mut evidence = GraphConstructionEvidence::default();
        let file = root.open_child_file(OsStr::new(name)).map_err(super::storage)?;
        let counter = IoCounter::default();
        let reader = super::CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                file,
                window,
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
            Ok(evidence_release) => account_cache_release(evidence_release, &mut evidence),
            Err(error) => Err(error),
        };
        let sorted = combine_cache_cleanup(loaded, released, "row partition spill")?;
        counter.add_to(&mut evidence)?;
        evidence.peak_partition_records = evidence
            .peak_partition_records
            .max(sorted.num_rows() as u64);
        Ok((sorted, evidence))
    }
}

/// Write one sorted partition as row groups bounded by row count and bytes.
///
/// Boundaries are a pure function of the sorted row sequence, never of arrival
/// order or buffer pressure, which is what keeps the produced bytes identical
/// across runs.
///
/// Takes no cancellation callback: it is only ever called from inside
/// [`load_partitions_ordered`]'s `consume`, which already checks cancellation
/// once before every partition it hands over. That bounds the delay between
/// a cancellation request and its observation by one partition's write, the
/// same bound a formula-derived partition count already puts on partition
/// size.
fn write_bounded_row_groups<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    sorted: &RecordBatch,
    output_rows: usize,
    output_bytes: usize,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let mut start = 0;
    while start < sorted.num_rows() {
        let mut length = 0;
        let mut bytes = 0_usize;
        while start + length < sorted.num_rows() && length < output_rows {
            let row_bytes = sorted.columns().iter().try_fold(0_usize, |total, column| {
                column
                    .slice(start + length, 1)
                    .to_data()
                    .get_slice_memory_size()
                    .map_err(super::storage)?
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
    }
    Ok(())
}
