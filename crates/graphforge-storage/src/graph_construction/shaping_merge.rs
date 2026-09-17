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

// The ordinary build expands to the original call only. Diagnostic counters
// measure completed group work without changing checkpoint/receipt formats.
macro_rules! measured_merge {
    ($family:expr, $level:expr, $inputs:expr, $evidence:ident, $call:expr) => {{
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic = super::diagnostics::Group::start($evidence);
        let result = $call;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish($family, $level, $inputs, $evidence, result.is_ok());
        }
        result
    }};
}

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
            xxh64: crate::corruption_checksum::hex(writer.get_ref().checksum.finish()),
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
            xxh64: crate::corruption_checksum::hex(writer.get_ref().checksum.finish()),
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
            measured_merge!(
                self.prefix,
                level + 1,
                inputs.len(),
                evidence,
                merge_fixed_group::<N>(
                    root,
                    &inputs,
                    &name,
                    self.reject_duplicates,
                    self.detail_codec,
                    cancelled,
                    evidence,
                )
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
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(self.prefix, self.inputs);
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
                measured_merge!(
                    self.prefix,
                    level + 1,
                    inputs.len(),
                    evidence,
                    merge_fixed_group::<N>(
                        root,
                        &inputs,
                        &output,
                        self.reject_duplicates,
                        self.detail_codec,
                        cancelled,
                        evidence,
                    )
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
        xxh64: crate::corruption_checksum::hex(writer.get_ref().checksum.finish()),
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
    batch_number: usize,
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
            self.batch_number = self
                .batch_number
                .checked_add(1)
                .ok_or_else(|| storage("row cursor batch number overflows"))?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

// A decoded batch can supply many selected rows. Retain its Arrow descriptors
// once per output window, including when heap order interleaves input streams.
struct SelectedRows {
    batches: Vec<RecordBatch>,
    rows: Vec<(usize, usize)>,
    current_batches: Vec<Option<(usize, usize)>>,
}

impl SelectedRows {
    fn new(output_rows: usize, sources: usize) -> Self {
        Self {
            batches: Vec::new(),
            rows: Vec::with_capacity(output_rows),
            current_batches: vec![None; sources],
        }
    }

    fn push(&mut self, source: usize, cursor: &RowCursor, batch: &RecordBatch) {
        let index = match self.current_batches[source] {
            Some((number, index)) if number == cursor.batch_number => index,
            _ => {
                let index = self.batches.len();
                self.batches.push(batch.clone());
                self.current_batches[source] = Some((cursor.batch_number, index));
                index
            }
        };
        self.rows.push((index, cursor.row));
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.batches.clear();
        self.current_batches.fill(None);
    }
}

fn materialize_selected_rows(
    schema: SchemaRef,
    selected: &SelectedRows,
) -> Result<RecordBatch, GfError> {
    let columns = (0..schema.fields().len())
        .map(|column| {
            let data = selected
                .batches
                .iter()
                .map(|batch| batch.column(column).to_data())
                .collect::<Vec<_>>();
            let refs = data.iter().collect::<Vec<_>>();
            let mut mutable = MutableArrayData::new(refs, false, selected.len());
            for (source, row) in &selected.rows {
                mutable.extend(
                    *source,
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
                batch_number: 0,
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
        let mut selected = SelectedRows::new(output_rows, cursors.len());
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
            selected.push(source, cursor, batch);
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
        xxh64: crate::corruption_checksum::hex(hashing.checksum.finish()),
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
    #[cfg(any(test, feature = "test-support"))]
    diagnostic_family: String,
    fan_in: usize,
    namespace: String,
    levels: Vec<Vec<String>>,
    groups: Vec<usize>,
    inputs: u64,
}

impl RowMergeAccumulator {
    pub(super) fn new(fan_in: usize, authority: &str) -> Self {
        Self {
            #[cfg(any(test, feature = "test-support"))]
            diagnostic_family: super::diagnostics::row_family(authority),
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
            measured_merge!(
                &self.diagnostic_family,
                level + 1,
                inputs.len(),
                evidence,
                merge_row_group(
                    root,
                    &inputs,
                    &name,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )
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
    ) -> Result<String, GfError> {
        #[cfg(any(test, feature = "test-support"))]
        super::diagnostics::inputs(&self.diagnostic_family, self.inputs);
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
                reject_cancelled(cancelled)?;
                // A scheduler-produced root already has the requested sorted,
                // bounded Parquet layout and a durable writer receipt. Keep its
                // identity in the completed shape inventory. Accepted source
                // chunks still need the ordinary materialization below.
                if inputs.len() == 1 && inputs[0].starts_with("merge-rows-") {
                    return Ok(inputs.into_iter().next().expect("one row merge root"));
                }
                let receipt = measured_merge!(
                    &self.diagnostic_family,
                    level + 1,
                    inputs.len(),
                    evidence,
                    merge_row_group(
                        root,
                        &inputs,
                        output,
                        output_rows,
                        output_bytes,
                        cancelled,
                        evidence,
                    )
                )?;
                for input in inputs {
                    if input.starts_with("merge-rows-") {
                        unlink_shape_artifact(root, &input, evidence)?;
                    }
                }
                return Ok(receipt.name);
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
                measured_merge!(
                    &self.diagnostic_family,
                    level + 1,
                    inputs.len(),
                    evidence,
                    merge_row_group(
                        root,
                        &inputs,
                        &output_name,
                        output_rows,
                        output_bytes,
                        cancelled,
                        evidence,
                    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    const FINAL_ROWS: &str = concat!(
        "shaped-rows-0-",
        "0000000000000000000000000000000000000000000000000000000000000000.parquet"
    );

    fn row_merge_fixture(
        root: &StableDirectory,
        inputs: u64,
    ) -> (RowMergeAccumulator, GraphConstructionEvidence) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let mut accumulator = RowMergeAccumulator::new(32, "0-retention-test");
        let mut evidence = GraphConstructionEvidence::default();
        // Session admission normally initializes the authoritative allocation
        // categories before any derived merge artifact can be installed.
        let category = crate::ArtifactCategory::ConstructionStaging;
        evidence.storage_current.entry(category).or_default();
        evidence
            .storage_receipt_category_authorities
            .entry(category)
            .or_default();
        evidence
            .storage_transient_peak_allocated_bytes
            .entry(category)
            .or_default();
        evidence
            .storage_receipt_transient_peak_authorities
            .entry(category)
            .or_default();
        // Reverse submission order independently checks that the final root
        // retains sorted results, including groups carried across levels.
        for id in (1..=inputs).rev() {
            let name = format!("source-{id}.parquet");
            let values = [u128::from(id).to_be_bytes()];
            let array =
                FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice()))
                    .unwrap();
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
            let file = std::fs::File::create(root.path().join(&name)).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            accumulator
                .push(root, name, 64, 1 << 20, &mut || false, &mut evidence)
                .unwrap();
        }
        (accumulator, evidence)
    }

    fn assert_row_merge_ids(root: &StableDirectory, name: &str, inputs: u64) {
        let file = root.open_child_file(OsStr::new(name)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut expected = 1;
        for batch in reader {
            let batch = batch.unwrap();
            let ids = uuid_column(&batch, "node_uuid").unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(
                    uuid_value(ids, row).unwrap(),
                    u128::from(expected).to_be_bytes()
                );
                expected += 1_u64;
            }
        }
        assert_eq!(expected, inputs + 1);
    }

    #[test]
    fn row_merge_retains_exact_completed_root_without_io_or_reinstallation() {
        for inputs in [32, 1024] {
            let temporary = tempfile::TempDir::new().unwrap();
            let root = StableDirectory::open(temporary.path()).unwrap();
            let (accumulator, mut evidence) = row_merge_fixture(&root, inputs);
            let names = accumulator.levels.iter().flatten().collect::<Vec<_>>();
            assert_eq!(names.len(), 1);
            let name = names[0].clone();
            let receipt = super::super::receipt_for_existing(&root, &name).unwrap();
            let capability = super::super::shape_receipt_name(&name);
            let before_payload = std::fs::read(root.path().join(&name)).unwrap();
            let before_capability = std::fs::read(root.path().join(&capability)).unwrap();
            let before_evidence = evidence.clone();
            let before_names = root.child_names().unwrap();
            let output = accumulator
                .finish(
                    &root,
                    FINAL_ROWS,
                    64,
                    1 << 20,
                    32,
                    &mut || false,
                    &mut evidence,
                )
                .unwrap();
            assert_eq!(output, name);
            assert_eq!(evidence, before_evidence);
            assert_eq!(root.child_names().unwrap(), before_names);
            assert_eq!(
                super::super::receipt_for_existing(&root, &output).unwrap(),
                receipt
            );
            assert_eq!(
                std::fs::read(root.path().join(&output)).unwrap(),
                before_payload
            );
            assert_eq!(
                std::fs::read(root.path().join(capability)).unwrap(),
                before_capability
            );
            assert_eq!(sha256(&before_payload), receipt.sha256);
            assert_row_merge_ids(&root, &output, inputs);
        }
    }

    #[test]
    fn row_merge_work_is_exact_across_production_fan_in_boundaries() {
        for (inputs, rows, groups) in [
            (1, 1, 1),
            (31, 31, 1),
            (32, 32, 1),
            (33, 65, 2),
            (1023, 2046, 33),
            (1024, 2048, 33),
            (1025, 3073, 34),
        ] {
            let temporary = tempfile::TempDir::new().unwrap();
            let root = StableDirectory::open(temporary.path()).unwrap();
            let (accumulator, mut evidence) = row_merge_fixture(&root, inputs);
            let output = accumulator
                .finish(
                    &root,
                    FINAL_ROWS,
                    64,
                    1 << 20,
                    32,
                    &mut || false,
                    &mut evidence,
                )
                .unwrap();
            assert_eq!(evidence.merge_read_records, rows, "inputs={inputs}");
            assert_eq!(evidence.merge_written_records, rows, "inputs={inputs}");
            assert_eq!(evidence.merge_groups, groups, "inputs={inputs}");
            assert!(evidence.parquet_read_bytes > 0);
            assert!(evidence.parquet_write_bytes > 0);
            assert!(evidence.peak_merge_inputs <= 32);
            assert_eq!(
                output.starts_with("merge-rows-"),
                matches!(inputs, 32 | 1024)
            );
            assert_row_merge_ids(&root, &output, inputs);
            if inputs == 1 {
                assert_eq!(output, FINAL_ROWS);
                assert!(root.path().join("source-1.parquet").exists());
                let source = root
                    .open_child_file(OsStr::new("source-1.parquet"))
                    .unwrap();
                let output = root.open_child_file(OsStr::new(&output)).unwrap();
                assert_ne!(
                    file_identity(&source).unwrap(),
                    file_identity(&output).unwrap()
                );
            }
        }
    }

    #[test]
    fn row_merge_empty_and_cancelled_finalization_preserve_artifacts_and_evidence() {
        for inputs in [0, 1, 32] {
            let temporary = tempfile::TempDir::new().unwrap();
            let root = StableDirectory::open(temporary.path()).unwrap();
            let (accumulator, mut evidence) = row_merge_fixture(&root, inputs);
            let before = evidence.clone();
            let names = root.child_names().unwrap();
            let error = accumulator
                .finish(
                    &root,
                    FINAL_ROWS,
                    64,
                    1 << 20,
                    32,
                    &mut || true,
                    &mut evidence,
                )
                .unwrap_err();
            let expected = if inputs == 0 {
                "no input"
            } else {
                "construction cancelled"
            };
            assert!(error.to_string().contains(expected));
            assert_eq!(evidence, before);
            assert_eq!(root.child_names().unwrap(), names);
        }
    }

    #[test]
    fn selected_rows_share_sources_across_interleaving_refill_and_flush() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = |values: Vec<Option<&str>>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap()
        };
        let mut cursors = [
            RowCursor {
                reader: Box::new(std::iter::empty()),
                batch: None,
                row: 0,
                batch_number: 1,
            },
            RowCursor {
                reader: Box::new(std::iter::empty()),
                batch: None,
                row: 0,
                batch_number: 1,
            },
        ];
        // Sliced arrays retain nonzero offsets; nullable variable-width values
        // must survive source sharing exactly as they did with per-row sources.
        let first = batch(vec![Some("padding"), Some("a"), None, Some("c")]).slice(1, 3);
        let second = batch(vec![Some("x"), Some("y")]);
        let refill = batch(vec![Some("d"), Some("e")]);
        let mut selected = SelectedRows::new(6, 2);
        for (source, row) in [(0, 0), (1, 0), (0, 1), (1, 1), (0, 2)] {
            cursors[source].row = row;
            selected.push(
                source,
                &cursors[source],
                if source == 0 { &first } else { &second },
            );
        }
        assert_eq!(selected.batches.len(), 2);
        cursors[0].batch_number = 2;
        cursors[0].row = 0;
        selected.push(0, &cursors[0], &refill);
        assert_eq!(selected.batches.len(), 3);
        let output = materialize_selected_rows(schema.clone(), &selected).unwrap();
        assert_eq!(
            output,
            batch(vec![
                Some("a"),
                Some("x"),
                None,
                Some("y"),
                Some("c"),
                Some("d")
            ])
        );
        selected.clear();
        assert!(selected.batches.is_empty());
        cursors[0].row = 1;
        selected.push(0, &cursors[0], &refill);
        assert_eq!(selected.batches.len(), 1);
        assert_eq!(
            materialize_selected_rows(schema.clone(), &selected).unwrap(),
            batch(vec![Some("e")])
        );
    }

    #[test]
    fn selected_rows_follow_actual_decoder_refills() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let batch = |ids: &[u128]| {
            let values = ids.iter().map(|id| id.to_be_bytes()).collect::<Vec<_>>();
            let array =
                FixedSizeBinaryArray::try_from_iter(values.iter().map(|id| id.as_slice())).unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
        };
        let first = batch(&[1, 3]);
        let second = batch(&[5, 7]);
        let mut cursor = RowCursor {
            reader: Box::new(vec![Ok(first), Ok(second)].into_iter()),
            batch: None,
            row: 0,
            batch_number: 0,
        };
        let mut selected = SelectedRows::new(4, 1);
        for id in [1_u128, 3, 5, 7] {
            assert_eq!(cursor.advance().unwrap(), Some(id.to_be_bytes()));
            selected.push(0, &cursor, cursor.batch.as_ref().unwrap());
            cursor.row += 1;
        }
        assert_eq!(selected.batches.len(), 2);
        assert_eq!(
            materialize_selected_rows(schema.clone(), &selected).unwrap(),
            batch(&[1, 3, 5, 7])
        );
        assert_eq!(cursor.advance().unwrap(), None);
    }

    #[test]
    fn selected_source_descriptors_scale_with_decoded_batches_not_rows() {
        let values = Arc::new(StringArray::from(vec![Some("retained"); 4096]));
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let mut cursor = RowCursor {
            reader: Box::new(std::iter::empty()),
            batch: None,
            row: 0,
            batch_number: 1,
        };
        let mut selected = SelectedRows::new(4096, 1);
        for row in 0..4096 {
            cursor.row = row;
            selected.push(0, &cursor, &batch);
        }
        assert_eq!(selected.len(), 4096);
        assert_eq!(selected.batches.len(), 1);
        assert_eq!(materialize_selected_rows(schema, &selected).unwrap(), batch);
    }
}
