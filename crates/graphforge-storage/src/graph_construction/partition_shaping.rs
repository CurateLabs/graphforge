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
//! The partitions run sequentially here. Nothing in this module shares mutable
//! state between partitions, which is what makes the concurrent step that
//! follows a scheduling change rather than a correctness change.

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
        let concatenated = (|| -> Result<u64, GfError> {
            let mut previous: Option<[u8; N]> = None;
            let mut written = 0_u64;
            for partition in 0..self.sealed.len() {
                let Some(name) = self.sealed[partition]
                    .as_ref()
                    .map(|receipt| receipt.name.clone())
                else {
                    continue;
                };
                reject_cancelled(cancelled)?;
                let records = self.load_partition(&name, evidence)?;
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
                    }
                    let wire = run_record_bytes(record, self.codec)?;
                    writer.write_all(wire).map_err(super::storage)?;
                    account_merge_write_bytes(evidence, wire.len() as u64)?;
                    previous = Some(*record);
                    written = written
                        .checked_add(1)
                        .ok_or_else(|| super::storage("partition output record count overflows"))?;
                    if written.is_multiple_of(4096) {
                        reject_cancelled(cancelled)?;
                    }
                }
            }
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
    fn load_partition(
        &self,
        name: &str,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<Vec<[u8; N]>, GfError> {
        let (mut reader, counter) = open_counted_fixed_reader(self.root, name, evidence)?;
        let loaded = (|| -> Result<Vec<[u8; N]>, GfError> {
            let mut records = Vec::new();
            while let Some(record) = read_run_record::<N>(&mut reader, self.codec)? {
                account_merge_read_bytes(
                    evidence,
                    run_record_bytes(&record, self.codec)?.len() as u64,
                )?;
                records.push(record);
            }
            Ok(records)
        })();
        let released = release_counted_reader_cache(&mut reader, evidence);
        let mut records = combine_cache_cleanup(loaded, released, "partition spill")?;
        account_fixed_read_operations(&counter, evidence)?;
        evidence.peak_partition_records = evidence.peak_partition_records.max(records.len() as u64);
        // The sort key is the whole record, whose leading 16 bytes are the
        // UUID. Records are globally unique on that prefix, so this is a total
        // order and no stability assumption is needed.
        records.sort_unstable();
        Ok(records)
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
        reject_cancelled(cancelled)?;
    }
    Ok(())
}
