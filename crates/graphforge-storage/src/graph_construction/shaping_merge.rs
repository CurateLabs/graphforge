//! Authenticated bounded shaping merges for immutable construction artifacts.

use super::{
    ArtifactReceipt, BASE_IDENTITY_WIDTH, BLOCK_BYTES, ConstructionChunkKind,
    ConstructionChunkReceipt, CountingChunkReader, CountingRead, GraphConstructionEvidence,
    HashingWriter, IDENTITY_WIDTH, IoCounter, account_cache_release, account_fixed_read_operations,
    account_fixed_write_operations, account_merge_read, account_merge_read_bytes,
    account_merge_write, account_merge_write_bytes, account_sequential_read,
    account_sequential_write, artifact_temp, cleanup_failed_shape_output,
    cleanup_shape_publication, combine_cache_cleanup, combine_secondary_cleanup,
    construction_failpoint, hex, persist_shape_receipt, read_fixed, read_run_record,
    record_shape_artifact_install, reject_cancelled, release_counted_reader_cache,
    run_record_bytes, sha256, shape_publication_failure, shape_publication_io_failure, storage,
    unlink_shape_artifact, unlink_writer_capability, uuid_column, uuid_value,
};
use crate::construction_detail_codec::DetailCodec;
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use arrow::array::{Array, MutableArrayData, RecordBatch, make_array};
use arrow::datatypes::SchemaRef;
use graphforge_core::GfError;
use graphforge_filesystem::{file_identity, file_link_count};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::OsStr;
use std::io::{BufReader, BufWriter, Write};

#[allow(clippy::too_many_lines)]
pub(super) fn convert_identity_run(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
    output: &str,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.identities.name))
        .map_err(storage)?;
    account_sequential_read(receipt.identities.bytes, evidence)?;
    if !receipt
        .identities
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("identity source authority changed before merge"));
    }
    let temporary = artifact_temp(output);
    let mut publication = root
        .create_unpublished_replaceable_child(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = match publication
        .verify_identity_with(|| shape_publication_io_failure("initial_file_identity"))
        .map_err(storage)
    {
        Ok(identity) => identity,
        Err(primary) => {
            let cleanup = cleanup_shape_publication(&mut publication);
            return combine_secondary_cleanup(Err(primary), cleanup, "identity setup cleanup");
        }
    };
    let file = publication.take_file().map_err(storage)?;
    let read_counter = IoCounter::default();
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                input,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage)?,
            counter: read_counter.clone(),
        },
    );
    let hashing = match HashingWriter::with_cache_window(file, cache_window) {
        Ok(hashing) => hashing,
        Err(primary) => {
            let cleanup = cleanup_shape_publication(&mut publication);
            return combine_secondary_cleanup(Err(primary), cleanup, "identity setup cleanup");
        }
    };
    let configured = graphforge_filesystem::validate_cache_release_operation_windows(&[
        reader.get_ref().inner.window_bytes(),
        hashing.inner.window_bytes(),
    ])
    .map_err(storage)
    .and_then(|_| shape_publication_failure("window_validation"));
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    if let Err(primary) = configured {
        let released = release_counted_reader_cache(&mut reader, evidence);
        let primary = combine_cache_cleanup::<()>(Err(primary), released, "identity source")
            .expect_err("primary cache configuration failure is retained");
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "identity output cleanup");
    }
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let copied = (|| -> Result<(), GfError> {
        while let Some(uuid) = read_fixed::<IDENTITY_WIDTH>(&mut reader)? {
            digest.update(uuid);
            bytes = bytes
                .checked_add(IDENTITY_WIDTH as u64)
                .ok_or_else(|| storage("bytes overflows"))?;
            let mut record = [0_u8; BASE_IDENTITY_WIDTH];
            record[..16].copy_from_slice(&uuid);
            record[16] = u8::from(receipt.kind == ConstructionChunkKind::Edge);
            writer.write_all(&record).map_err(storage)?;
            account_merge_read::<IDENTITY_WIDTH>(evidence)?;
            account_merge_write::<BASE_IDENTITY_WIDTH>(evidence)?;
            reject_cancelled(cancelled)?;
        }
        if bytes != receipt.identities.bytes || hex(&digest.finalize()) != receipt.identities.sha256
        {
            return Err(storage("identity source content changed before merge"));
        }
        account_fixed_read_operations(&read_counter, evidence)
    })();
    let released = release_counted_reader_cache(&mut reader, evidence);
    let copied = combine_cache_cleanup(copied, released, "identity source");
    if let Err(primary) = copied {
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "identity output cleanup");
    }
    let finalized = writer.flush().map_err(storage).and_then(|()| {
        writer
            .get_mut()
            .inner
            .sync_all_and_release()
            .map_err(storage)
    });
    if let Err(primary) = finalized {
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "identity output cleanup");
    }
    let cache_release = writer.get_ref().inner.evidence();
    let prepared = (|| -> Result<(_, _), GfError> {
        shape_publication_failure("fsync_evidence_overflow")?;
        let mut committed_evidence = evidence.clone();
        account_cache_release(cache_release, &mut committed_evidence)?;
        let aggregate_peak = reader
            .get_ref()
            .inner
            .tracker()
            .evidence()
            .peak_window_bytes
            .checked_add(cache_release.peak_window_bytes)
            .ok_or_else(|| storage("identity copy aggregate cache window overflow"))?;
        committed_evidence.peak_cache_release_window_bytes = committed_evidence
            .peak_cache_release_window_bytes
            .max(aggregate_peak);
        account_sequential_write(writer.get_ref().bytes, &mut committed_evidence)?;
        shape_publication_failure("final_file_identity")?;
        if file_identity(writer.get_ref().inner.file()).map_err(storage)?
            != publication.identity().map_err(storage)?
        {
            return Err(storage(
                "identity output authority changed before publication",
            ));
        }
        shape_publication_failure("file_space_usage")?;
        let output_receipt = ArtifactReceipt {
            name: output.to_owned(),
            bytes: writer.get_ref().bytes,
            allocated_bytes: graphforge_filesystem::file_space_usage(writer.get_ref().inner.file())
                .map_err(storage)?
                .allocated_bytes,
            sha256: hex(&writer.get_ref().digest.clone().finalize()),
            identity: identity.into(),
            write_operations: writer.get_ref().operations,
            fsync_operations: cache_release
                .sync_operations
                .checked_add(1)
                .ok_or_else(|| storage("artifact synchronization count overflows"))?,
        };
        record_shape_artifact_install(&mut committed_evidence, &output_receipt)?;
        account_fixed_write_operations(&output_receipt, &mut committed_evidence)?;
        committed_evidence.merge_fsync_operations = committed_evidence
            .merge_fsync_operations
            .checked_add(output_receipt.fsync_operations)
            .ok_or_else(|| storage("merge fsync operations overflows"))?;
        Ok((output_receipt, committed_evidence))
    })();
    let (output_receipt, committed_evidence) = match prepared {
        Ok(prepared) => prepared,
        Err(primary) => {
            let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
            return combine_secondary_cleanup(
                Err(primary),
                cleanup,
                "identity setup publication cleanup",
            );
        }
    };
    drop(writer);
    let published = (|| -> Result<(), GfError> {
        shape_publication_failure("install_child")?;
        let allocation_file = if root.allocation().is_some() {
            let file = root
                .open_child_file(OsStr::new(&temporary))
                .map_err(storage)?;
            root.observe_file(OsStr::new(&temporary), &file)
                .map_err(storage)?;
            Some(file)
        } else {
            None
        };
        publication
            .install_child(OsStr::new(output))
            .map_err(storage)?;
        if let Some(file) = &allocation_file {
            root.record_replacement(OsStr::new(&temporary), OsStr::new(output), file)
                .map_err(storage)?;
        }
        drop(allocation_file);
        publication.sync_parent().map_err(storage)?;
        shape_publication_failure("directory_sync")?;
        shape_publication_failure("post_publication_metric_overflow")?;
        shape_publication_failure("manifest_update")?;
        construction_failpoint("shape.fixed.after_install");
        persist_shape_receipt(root, &output_receipt)
    })();
    if let Err(primary) = published {
        let receipt_cleanup =
            unlink_writer_capability(root, output, Some(&output_receipt)).map_err(storage);
        let primary = combine_secondary_cleanup::<()>(
            Err(primary),
            receipt_cleanup,
            "identity receipt cleanup",
        )
        .expect_err("primary identity publication failure is retained");
        let cleanup = cleanup_shape_publication(&mut publication);
        return combine_secondary_cleanup(Err(primary), cleanup, "identity publication cleanup");
    }
    publication.commit().map_err(storage)?;
    *evidence = committed_evidence;
    Ok(())
}

pub(super) fn copy_authenticated_run<const N: usize>(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    output: &str,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    copy_authenticated_run_with_codec::<N>(root, receipt, output, cancelled, evidence, None)
}

#[allow(clippy::too_many_lines)] // Retain the existing coupled authentication/publication cleanup scope.
pub(super) fn copy_authenticated_run_with_codec<const N: usize>(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    output: &str,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
    codec: Option<DetailCodec>,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    account_sequential_read(receipt.bytes, evidence)?;
    if !receipt
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("construction merge source authority changed"));
    }
    let temporary = artifact_temp(output);
    let mut publication = root
        .create_unpublished_replaceable_child(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = match publication
        .verify_identity_with(|| shape_publication_io_failure("initial_file_identity"))
        .map_err(storage)
    {
        Ok(identity) => identity,
        Err(primary) => {
            let cleanup = cleanup_shape_publication(&mut publication);
            return combine_secondary_cleanup(Err(primary), cleanup, "authenticated setup cleanup");
        }
    };
    let file = publication.take_file().map_err(storage)?;
    let read_counter = IoCounter::default();
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                input,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage)?,
            counter: read_counter.clone(),
        },
    );
    let hashing = match HashingWriter::with_cache_window(file, cache_window) {
        Ok(hashing) => hashing,
        Err(primary) => {
            let cleanup = cleanup_shape_publication(&mut publication);
            return combine_secondary_cleanup(Err(primary), cleanup, "authenticated setup cleanup");
        }
    };
    let configured = graphforge_filesystem::validate_cache_release_operation_windows(&[
        reader.get_ref().inner.window_bytes(),
        hashing.inner.window_bytes(),
    ])
    .map_err(storage)
    .and_then(|_| shape_publication_failure("window_validation"));
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    if let Err(primary) = configured {
        let released = release_counted_reader_cache(&mut reader, evidence);
        let primary =
            combine_cache_cleanup::<()>(Err(primary), released, "construction merge source")
                .expect_err("primary cache configuration failure is retained");
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "authenticated output cleanup");
    }
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let copied = (|| -> Result<(), GfError> {
        while let Some(record) = read_run_record::<N>(&mut reader, codec)? {
            let wire = run_record_bytes(&record, codec)?;
            digest.update(wire);
            bytes = bytes
                .checked_add(wire.len() as u64)
                .ok_or_else(|| storage("bytes overflows"))?;
            writer.write_all(wire).map_err(storage)?;
            account_merge_read_bytes(evidence, wire.len() as u64)?;
            account_merge_write_bytes(evidence, wire.len() as u64)?;
            reject_cancelled(cancelled)?;
        }
        if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
            return Err(storage("construction merge source content changed"));
        }
        account_fixed_read_operations(&read_counter, evidence)
    })();
    let released = release_counted_reader_cache(&mut reader, evidence);
    let copied = combine_cache_cleanup(copied, released, "construction merge source");
    if let Err(primary) = copied {
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "authenticated output cleanup");
    }
    let finalized = writer.flush().map_err(storage).and_then(|()| {
        writer
            .get_mut()
            .inner
            .sync_all_and_release()
            .map_err(storage)
    });
    if let Err(primary) = finalized {
        let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
        return combine_secondary_cleanup(Err(primary), cleanup, "authenticated output cleanup");
    }
    let cache_release = writer.get_ref().inner.evidence();
    let prepared = (|| -> Result<(_, _), GfError> {
        shape_publication_failure("fsync_evidence_overflow")?;
        let mut committed_evidence = evidence.clone();
        account_cache_release(cache_release, &mut committed_evidence)?;
        let aggregate_peak = reader
            .get_ref()
            .inner
            .tracker()
            .evidence()
            .peak_window_bytes
            .checked_add(cache_release.peak_window_bytes)
            .ok_or_else(|| storage("authenticated copy aggregate cache window overflow"))?;
        committed_evidence.peak_cache_release_window_bytes = committed_evidence
            .peak_cache_release_window_bytes
            .max(aggregate_peak);
        account_sequential_write(bytes, &mut committed_evidence)?;
        shape_publication_failure("final_file_identity")?;
        if file_identity(writer.get_ref().inner.file()).map_err(storage)?
            != publication.identity().map_err(storage)?
        {
            return Err(storage(
                "authenticated output authority changed before publication",
            ));
        }
        shape_publication_failure("file_space_usage")?;
        let allocated_bytes =
            graphforge_filesystem::file_space_usage(writer.get_ref().inner.file())
                .map_err(storage)?
                .allocated_bytes;
        let output_receipt = ArtifactReceipt {
            name: output.to_owned(),
            bytes,
            allocated_bytes,
            sha256: receipt.sha256.clone(),
            identity: identity.into(),
            write_operations: writer.get_ref().operations,
            fsync_operations: cache_release
                .sync_operations
                .checked_add(1)
                .ok_or_else(|| storage("artifact synchronization count overflows"))?,
        };
        record_shape_artifact_install(&mut committed_evidence, &output_receipt)?;
        account_fixed_write_operations(&output_receipt, &mut committed_evidence)?;
        committed_evidence.merge_fsync_operations = committed_evidence
            .merge_fsync_operations
            .checked_add(output_receipt.fsync_operations)
            .ok_or_else(|| storage("merge fsync operations overflows"))?;
        Ok((output_receipt, committed_evidence))
    })();
    let (output_receipt, committed_evidence) = match prepared {
        Ok(prepared) => prepared,
        Err(primary) => {
            let cleanup = cleanup_failed_shape_output(writer, &mut publication, evidence);
            return combine_secondary_cleanup(
                Err(primary),
                cleanup,
                "authenticated setup publication cleanup",
            );
        }
    };
    drop(writer);
    let published = (|| -> Result<(), GfError> {
        shape_publication_failure("install_child")?;
        let allocation_file = if root.allocation().is_some() {
            let file = root
                .open_child_file(OsStr::new(&temporary))
                .map_err(storage)?;
            root.observe_file(OsStr::new(&temporary), &file)
                .map_err(storage)?;
            Some(file)
        } else {
            None
        };
        publication
            .install_child(OsStr::new(output))
            .map_err(storage)?;
        if let Some(file) = &allocation_file {
            root.record_replacement(OsStr::new(&temporary), OsStr::new(output), file)
                .map_err(storage)?;
        }
        drop(allocation_file);
        publication.sync_parent().map_err(storage)?;
        shape_publication_failure("directory_sync")?;
        shape_publication_failure("post_publication_metric_overflow")?;
        shape_publication_failure("manifest_update")?;
        persist_shape_receipt(root, &output_receipt)
    })();
    if let Err(primary) = published {
        let receipt_cleanup =
            unlink_writer_capability(root, output, Some(&output_receipt)).map_err(storage);
        let primary = combine_secondary_cleanup::<()>(
            Err(primary),
            receipt_cleanup,
            "authenticated receipt cleanup",
        )
        .expect_err("primary authenticated publication failure is retained");
        let cleanup = cleanup_shape_publication(&mut publication);
        return combine_secondary_cleanup(
            Err(primary),
            cleanup,
            "authenticated publication cleanup",
        );
    }
    publication.commit().map_err(storage)?;
    *evidence = committed_evidence;
    Ok(())
}

/// Online external-merge scheduler.  It retains at most `fan_in - 1` names per
/// logarithmic level rather than one name per accepted chunk.
pub(super) struct FixedMergeAccumulator {
    prefix: &'static str,
    detail_codec: Option<DetailCodec>,
    fan_in: usize,
    reject_duplicates: bool,
    levels: Vec<Vec<String>>,
    groups: Vec<usize>,
    inputs: u64,
}

#[cfg(test)]
pub(super) fn online_merge_name_slot_bound(mut inputs: u64, fan_in: usize) -> u64 {
    let radix = fan_in as u64;
    let mut slots = 0_u64;
    while inputs != 0 {
        slots = slots
            .checked_add(inputs % radix)
            .expect("online merge slot bound overflow");
        inputs /= radix;
    }
    slots.max(1)
}

impl FixedMergeAccumulator {
    pub(super) fn new(prefix: &'static str, fan_in: usize, reject_duplicates: bool) -> Self {
        Self {
            prefix,
            detail_codec: None,
            fan_in,
            reject_duplicates,
            levels: Vec::new(),
            groups: Vec::new(),
            inputs: 0,
        }
    }

    pub(super) fn with_detail_codec(mut self, codec: DetailCodec) -> Self {
        self.detail_codec = Some(codec);
        self
    }

    pub(super) fn slot_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    pub(super) fn push<const N: usize>(
        &mut self,
        root: &StableDirectory,
        mut name: String,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        self.inputs = self
            .inputs
            .checked_add(1)
            .ok_or_else(|| storage("fixed merge input count overflow"))?;
        let mut level = 0;
        loop {
            if self.levels.len() <= level {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level].push(name);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            let group = self.groups[level];
            self.groups[level] = group
                .checked_add(1)
                .ok_or_else(|| storage("fixed merge group count overflow"))?;
            evidence.merge_passes = evidence.merge_passes.max(
                u64::try_from(level)
                    .map_err(storage)?
                    .checked_add(1)
                    .ok_or_else(|| storage("fixed merge pass count overflow"))?,
            );
            name = format!("{}-l{level:03}-g{group:08}.run", self.prefix);
            merge_fixed_group::<N>(
                root,
                &inputs,
                &name,
                self.reject_duplicates,
                self.detail_codec,
                cancelled,
                evidence,
            )?;
            for input in inputs {
                if input.starts_with("merge-") {
                    unlink_shape_artifact(root, &input, evidence)?;
                }
            }
            level += 1;
        }
    }

    pub(super) fn finish_optional<const N: usize>(
        self,
        root: &StableDirectory,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<Option<String>, GfError> {
        if self.inputs == 0 {
            return Ok(None);
        }
        self.finish::<N>(root, cancelled, evidence).map(Some)
    }

    pub(super) fn finish<const N: usize>(
        mut self,
        root: &StableDirectory,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<String, GfError> {
        if self.inputs == 0 {
            return Err(storage("external merge has no input"));
        }
        let mut level = 0;
        loop {
            if level >= self.levels.len() {
                return Err(storage("external merge scheduler lost its root"));
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            if inputs.is_empty() {
                level += 1;
                continue;
            }
            let higher_empty = self.levels[level + 1..].iter().all(Vec::is_empty);
            if inputs.len() == 1 && higher_empty {
                return Ok(inputs.into_iter().next().expect("one merge root"));
            }
            let name = if inputs.len() == 1 {
                inputs[0].clone()
            } else {
                let group = self.groups[level];
                self.groups[level] = group
                    .checked_add(1)
                    .ok_or_else(|| storage("fixed merge group count overflow"))?;
                evidence.merge_passes = evidence.merge_passes.max(
                    u64::try_from(level)
                        .map_err(storage)?
                        .checked_add(1)
                        .ok_or_else(|| storage("fixed merge pass count overflow"))?,
                );
                let output = format!("{}-l{level:03}-g{group:08}.run", self.prefix);
                merge_fixed_group::<N>(
                    root,
                    &inputs,
                    &output,
                    self.reject_duplicates,
                    self.detail_codec,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-") {
                        unlink_shape_artifact(root, &input, evidence)?;
                    }
                }
                output
            };
            if self.levels.len() <= level + 1 {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level + 1].push(name);
            level += 1;
        }
    }
}

#[allow(clippy::too_many_lines)] // One streamed merge keeps reader release and output durability coupled.
fn merge_fixed_group<const N: usize>(
    root: &StableDirectory,
    inputs: &[String],
    output: &str,
    reject_duplicates: bool,
    codec: Option<DetailCodec>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    let read_counter = IoCounter::default();
    let active_streams = inputs
        .len()
        .checked_add(1)
        .ok_or_else(|| storage("fixed merge stream count overflow"))?;
    let reader_cache_window =
        graphforge_filesystem::cache_release_window_for_streams(active_streams).map_err(storage)?;
    let mut readers = inputs
        .iter()
        .map(|name| -> Result<_, GfError> {
            let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
            account_sequential_read(file.metadata().map_err(storage)?.len(), evidence)?;
            Ok(BufReader::with_capacity(
                BLOCK_BYTES,
                CountingRead {
                    inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                        file,
                        reader_cache_window,
                        graphforge_filesystem::FileCacheReleaseTracker::default(),
                    )
                    .map_err(storage)?,
                    counter: read_counter.clone(),
                },
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    evidence.peak_merge_inputs = evidence.peak_merge_inputs.max(readers.len() as u64);
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::with_cache_window(file, reader_cache_window)?;
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_run_record::<N>(reader, codec)? {
            heap.push(Reverse((record, index)));
            account_merge_read_bytes(evidence, run_record_bytes(&record, codec)?.len() as u64)?;
        }
    }
    let mut previous = None;
    while let Some(Reverse((record, index))) = heap.pop() {
        if reject_duplicates
            && previous
                .as_ref()
                .is_some_and(|prior: &[u8; N]| prior[..16] == record[..16])
        {
            return Err(storage("duplicate identity across construction runs"));
        }
        let wire = run_record_bytes(&record, codec)?;
        writer.write_all(wire).map_err(storage)?;
        account_merge_write_bytes(evidence, wire.len() as u64)?;
        previous = Some(record);
        if evidence.merge_written_records.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
        if let Some(next) = read_run_record::<N>(&mut readers[index], codec)? {
            heap.push(Reverse((next, index)));
            account_merge_read_bytes(evidence, run_record_bytes(&next, codec)?.len() as u64)?;
        }
    }
    writer.flush().map_err(storage)?;
    account_fixed_read_operations(&read_counter, evidence)?;
    for reader in &mut readers {
        release_counted_reader_cache(reader, evidence)?;
    }
    writer
        .get_mut()
        .inner
        .sync_all_and_release()
        .map_err(storage)?;
    let cache_release = writer.get_ref().inner.evidence();
    account_cache_release(cache_release, evidence)?;
    let aggregate_peak =
        readers
            .iter()
            .try_fold(cache_release.peak_window_bytes, |peak, reader| {
                peak.checked_add(
                    reader
                        .get_ref()
                        .inner
                        .tracker()
                        .evidence()
                        .peak_window_bytes,
                )
                .ok_or_else(|| storage("fixed merge aggregate cache window overflow"))
            })?;
    evidence.peak_cache_release_window_bytes =
        evidence.peak_cache_release_window_bytes.max(aggregate_peak);
    account_sequential_write(writer.get_ref().bytes, evidence)?;
    let receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: writer.get_ref().bytes,
        allocated_bytes: graphforge_filesystem::file_space_usage(writer.get_ref().inner.file())
            .map_err(storage)?
            .allocated_bytes,
        sha256: hex(&writer.get_ref().digest.clone().finalize()),
        identity: identity.into(),
        write_operations: writer.get_ref().operations,
        fsync_operations: cache_release
            .sync_operations
            .checked_add(1)
            .ok_or_else(|| storage("artifact synchronization count overflows"))?,
    };
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint("shape.fixed_merge.after_install");
    persist_shape_receipt(root, &receipt)?;
    record_shape_artifact_install(evidence, &receipt)?;
    account_fixed_write_operations(&receipt, evidence)?;
    evidence.merge_fsync_operations = evidence
        .merge_fsync_operations
        .checked_add(receipt.fsync_operations)
        .ok_or_else(|| storage("merge fsync operations overflows"))?;
    evidence.merge_groups = evidence
        .merge_groups
        .checked_add(1)
        .ok_or_else(|| storage("merge groups overflows"))?;
    Ok(receipt)
}

struct RowCursor {
    reader: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>,
    batch: Option<RecordBatch>,
    row: usize,
}

impl RowCursor {
    fn advance(&mut self) -> Result<Option<[u8; 16]>, GfError> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                return uuid_value(
                    uuid_column(batch, batch.schema().field(0).name())?,
                    self.row,
                )
                .map(Some);
            }
            self.batch = self.reader.next().transpose().map_err(storage)?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

fn materialize_selected_rows(
    schema: SchemaRef,
    selected: &[(RecordBatch, usize)],
) -> Result<RecordBatch, GfError> {
    let columns = (0..schema.fields().len())
        .map(|column| {
            let data = selected
                .iter()
                .map(|(batch, _)| batch.column(column).to_data())
                .collect::<Vec<_>>();
            let refs = data.iter().collect::<Vec<_>>();
            let mut mutable = MutableArrayData::new(refs, false, selected.len());
            for (source, (_, row)) in selected.iter().enumerate() {
                mutable.extend(
                    source,
                    *row,
                    row.checked_add(1)
                        .ok_or_else(|| storage("selected row bound overflow"))?,
                );
            }
            Ok(make_array(mutable.freeze()))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    RecordBatch::try_new(schema, columns).map_err(storage)
}

/// Merge one exact-schema set of UUID-sorted normalized Parquet row artifacts.
/// Memory is bounded by one decoder window per input plus one caller-sized
/// output window; no property values are projected away.
#[allow(clippy::too_many_lines)]
fn merge_row_group(
    root: &StableDirectory,
    inputs: &[String],
    output: &str,
    output_rows: usize,
    output_bytes: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    if inputs.is_empty() || output_rows == 0 || output_bytes == 0 {
        return Err(storage("invalid row merge group"));
    }
    evidence.peak_merge_inputs = evidence.peak_merge_inputs.max(inputs.len() as u64);
    let mut cursors = Vec::with_capacity(inputs.len());
    let mut counters = Vec::with_capacity(inputs.len());
    let mut cache_release_trackers = Vec::with_capacity(inputs.len());
    let active_streams = inputs
        .len()
        .checked_add(1)
        .ok_or_else(|| storage("row merge stream count overflow"))?;
    let reader_cache_window =
        graphforge_filesystem::cache_release_window_for_streams(active_streams).map_err(storage)?;
    let mut schema: Option<SchemaRef> = None;
    let initialized = (|| -> Result<(), GfError> {
        for input in inputs {
            let file = root.open_child_file(OsStr::new(input)).map_err(storage)?;
            let counter = IoCounter::default();
            let chunk_reader =
                CountingChunkReader::with_cache_window(file, counter.clone(), reader_cache_window);
            let cache_release = chunk_reader.cache_release_tracker();
            counters.push(counter);
            cache_release_trackers.push(cache_release);
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(chunk_reader).map_err(storage)?;
            if schema
                .as_ref()
                .is_some_and(|known| known.as_ref() != builder.schema().as_ref())
            {
                return Err(storage("row merge schemas differ"));
            }
            schema.get_or_insert_with(|| builder.schema().clone());
            cursors.push(RowCursor {
                reader: Box::new(
                    builder
                        .with_batch_size(output_rows.min(4096))
                        .build()
                        .map_err(storage)?,
                ),
                batch: None,
                row: 0,
            });
        }
        Ok(())
    })();
    if let Err(primary) = initialized {
        drop(cursors);
        let mut result = Err(primary);
        for tracker in &cache_release_trackers {
            result = combine_cache_cleanup(
                result,
                tracker.check_error().map_err(storage),
                "row merge source",
            );
            account_cache_release(tracker.evidence(), evidence)?;
        }
        for counter in counters {
            counter.add_to(evidence)?;
        }
        return result;
    }
    let schema = schema.ok_or_else(|| storage("row merge lacks schema"))?;
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::with_cache_window(file, reader_cache_window)?;
    let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut writer = ArrowWriter::try_new(buffered, schema.clone(), None).map_err(storage)?;
    let merged = (|| -> Result<(), GfError> {
        let mut heap = BinaryHeap::new();
        for (source, cursor) in cursors.iter_mut().enumerate() {
            if let Some(uuid) = cursor.advance()? {
                heap.push((Reverse(uuid), Reverse(source)));
            }
        }
        let mut selected = Vec::with_capacity(output_rows);
        let mut selected_bytes = 0_usize;
        let mut previous = None;
        while let Some((Reverse(uuid), Reverse(source))) = heap.pop() {
            reject_cancelled(cancelled)?;
            if previous.is_some_and(|prior| prior >= uuid) {
                return Err(storage("duplicate or unordered UUID in row merge"));
            }
            previous = Some(uuid);
            let cursor = &mut cursors[source];
            let batch = cursor
                .batch
                .as_ref()
                .ok_or_else(|| storage("row cursor lacks batch"))?;
            let row_bytes = batch.columns().iter().try_fold(0_usize, |total, column| {
                column
                    .slice(cursor.row, 1)
                    .to_data()
                    .get_slice_memory_size()
                    .map_err(storage)?
                    .checked_add(total)
                    .ok_or_else(|| storage("merge row byte total overflows"))
            })?;
            if row_bytes > output_bytes {
                return Err(storage("one normalized row exceeds merge byte window"));
            }
            if !selected.is_empty()
                && (selected.len() == output_rows
                    || selected_bytes
                        .checked_add(row_bytes)
                        .ok_or_else(|| storage("merge selected bytes overflow"))?
                        > output_bytes)
            {
                let output_batch = materialize_selected_rows(schema.clone(), &selected)?;
                evidence.merge_read_records = evidence
                    .merge_read_records
                    .checked_add(output_batch.num_rows() as u64)
                    .ok_or_else(|| storage("merge read records overflows"))?;
                writer.write(&output_batch).map_err(storage)?;
                evidence.merge_written_records = evidence
                    .merge_written_records
                    .checked_add(output_batch.num_rows() as u64)
                    .ok_or_else(|| storage("merge written records overflows"))?;
                selected.clear();
                selected_bytes = 0;
            }
            selected.push((batch.clone(), cursor.row));
            selected_bytes = selected_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| storage("merge selected bytes overflow"))?;
            cursor.row = cursor
                .row
                .checked_add(1)
                .ok_or_else(|| storage("merge row index overflows"))?;
            if let Some(next) = cursor.advance()? {
                heap.push((Reverse(next), Reverse(source)));
            }
            if heap.is_empty() {
                let batch = materialize_selected_rows(schema.clone(), &selected)?;
                evidence.merge_read_records = evidence
                    .merge_read_records
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| storage("merge read records overflows"))?;
                writer.write(&batch).map_err(storage)?;
                evidence.merge_written_records = evidence
                    .merge_written_records
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| storage("merge written records overflows"))?;
                selected.clear();
                selected_bytes = 0;
            }
        }
        writer.finish().map_err(storage)?;
        writer.sync().map_err(storage)
    })();
    drop(cursors);
    let mut merged = merged;
    let mut reader_cache_peak = 0_u64;
    for tracker in &cache_release_trackers {
        merged = combine_cache_cleanup(
            merged,
            tracker.check_error().map_err(storage),
            "row merge source",
        );
        let cache_release = tracker.evidence();
        reader_cache_peak = reader_cache_peak
            .checked_add(cache_release.peak_window_bytes)
            .ok_or_else(|| storage("row merge reader cache window overflow"))?;
        account_cache_release(cache_release, evidence)?;
    }
    for counter in counters {
        counter.add_to(evidence)?;
    }
    merged?;
    writer
        .inner_mut()
        .get_mut()
        .inner
        .sync_all_and_release()
        .map_err(storage)?;
    let hashing = writer.inner().get_ref();
    let cache_release = hashing.inner.evidence();
    account_cache_release(cache_release, evidence)?;
    let aggregate_peak = reader_cache_peak
        .checked_add(cache_release.peak_window_bytes)
        .ok_or_else(|| storage("row merge aggregate cache window overflow"))?;
    evidence.peak_cache_release_window_bytes =
        evidence.peak_cache_release_window_bytes.max(aggregate_peak);
    let receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: hashing.bytes,
        allocated_bytes: graphforge_filesystem::file_space_usage(hashing.inner.file())
            .map_err(storage)?
            .allocated_bytes,
        sha256: hex(&hashing.digest.clone().finalize()),
        identity: identity.into(),
        write_operations: hashing.operations,
        fsync_operations: cache_release
            .sync_operations
            .checked_add(3)
            .ok_or_else(|| storage("artifact synchronization count overflows"))?,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint("shape.row_merge.after_install");
    persist_shape_receipt(root, &receipt)?;
    record_shape_artifact_install(evidence, &receipt)?;
    evidence.merge_fsync_operations = evidence
        .merge_fsync_operations
        .checked_add(receipt.fsync_operations)
        .ok_or_else(|| storage("merge fsync operations overflows"))?;
    evidence.merge_groups = evidence
        .merge_groups
        .checked_add(1)
        .ok_or_else(|| storage("merge groups overflows"))?;
    evidence.parquet_write_bytes = evidence
        .parquet_write_bytes
        .checked_add(receipt.bytes)
        .ok_or_else(|| storage("parquet write bytes overflows"))?;
    evidence.parquet_write_operations = evidence
        .parquet_write_operations
        .checked_add(receipt.write_operations)
        .ok_or_else(|| storage("parquet write operations overflows"))?;
    Ok(receipt)
}

pub(super) struct RowMergeAccumulator {
    fan_in: usize,
    namespace: String,
    levels: Vec<Vec<String>>,
    groups: Vec<usize>,
    inputs: u64,
}

impl RowMergeAccumulator {
    pub(super) fn new(fan_in: usize, authority: &str) -> Self {
        Self {
            fan_in,
            namespace: sha256(authority.as_bytes())[..16].to_owned(),
            levels: Vec::new(),
            groups: Vec::new(),
            inputs: 0,
        }
    }

    pub(super) fn slot_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn push(
        &mut self,
        root: &StableDirectory,
        mut name: String,
        output_rows: usize,
        output_bytes: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        self.inputs = self
            .inputs
            .checked_add(1)
            .ok_or_else(|| storage("row merge input count overflow"))?;
        let mut level = 0;
        loop {
            if self.levels.len() <= level {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level].push(name);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            let group = self.groups[level];
            self.groups[level] = group
                .checked_add(1)
                .ok_or_else(|| storage("row merge group count overflow"))?;
            evidence.merge_passes = evidence.merge_passes.max(
                u64::try_from(level)
                    .map_err(storage)?
                    .checked_add(1)
                    .ok_or_else(|| storage("row merge pass count overflow"))?,
            );
            name = format!(
                "merge-rows-{}-l{level:03}-g{group:020}.parquet",
                self.namespace
            );
            merge_row_group(
                root,
                &inputs,
                &name,
                output_rows,
                output_bytes,
                cancelled,
                evidence,
            )?;
            for input in inputs {
                if input.starts_with("merge-rows-") {
                    unlink_shape_artifact(root, &input, evidence)?;
                }
            }
            level += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish(
        mut self,
        root: &StableDirectory,
        output: &str,
        output_rows: usize,
        output_bytes: usize,
        _fan_in: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<ArtifactReceipt, GfError> {
        if self.inputs == 0 {
            return Err(storage("row merge has no input"));
        }
        let mut level = 0;
        loop {
            let inputs = std::mem::take(
                self.levels
                    .get_mut(level)
                    .ok_or_else(|| storage("row merge scheduler lost its root"))?,
            );
            if inputs.is_empty() {
                level += 1;
                continue;
            }
            let higher_empty = self.levels[level + 1..].iter().all(Vec::is_empty);
            if higher_empty {
                let receipt = merge_row_group(
                    root,
                    &inputs,
                    output,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-rows-") {
                        unlink_shape_artifact(root, &input, evidence)?;
                    }
                }
                return Ok(receipt);
            }
            let name = if inputs.len() == 1 {
                inputs[0].clone()
            } else {
                let group = self.groups[level];
                self.groups[level] = group
                    .checked_add(1)
                    .ok_or_else(|| storage("row merge group count overflow"))?;
                evidence.merge_passes = evidence.merge_passes.max(
                    u64::try_from(level)
                        .map_err(storage)?
                        .checked_add(1)
                        .ok_or_else(|| storage("row merge pass count overflow"))?,
                );
                let output_name = format!(
                    "merge-rows-{}-l{level:03}-g{group:020}.parquet",
                    self.namespace
                );
                merge_row_group(
                    root,
                    &inputs,
                    &output_name,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-rows-") {
                        unlink_shape_artifact(root, &input, evidence)?;
                    }
                }
                output_name
            };
            if self.levels.len() <= level + 1 {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level + 1].push(name);
            level += 1;
        }
    }
}
