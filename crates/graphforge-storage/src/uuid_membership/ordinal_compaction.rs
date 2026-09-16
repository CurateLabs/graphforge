//! Bounded binary-carry compaction of ordinal identity artifacts.

use super::V4_ORDINAL_BLOCK_BYTES;
use super::V4OrdinalBuildMetrics;
use super::construction::combine_cache_cleanup;
use super::construction::combine_v4_cleanup;
use super::construction::merge_cache_release_evidence;
use super::inject_v4_input_release_result;
use super::ordinal_artifacts::GuardedV4Artifact;
use super::ordinal_artifacts::StreamingV4Artifact;
use super::ordinal_artifacts::V4OrdinalRangeWriter;
use super::ordinal_artifacts::V4PublicationGuard;
use super::ordinal_artifacts::V4TombstoneStreamWriter;
use super::ordinal_artifacts::clone_pinned_v4_file;
use super::ordinal_artifacts::finish_streamed_v4_artifact;
use super::ordinal_artifacts::finish_streamed_v4_range;
use super::ordinal_artifacts::retain_v4_publication;
use super::rebuild::read_exact_record;
use super::storage_err;
use super::v4_compaction_post_write_failure;
use graphforge_core::GfError;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

#[derive(Default)]
pub(super) struct V4CompactionWork {
    pub(super) compactions: u64,
    pub(super) created_artifacts: u64,
    pub(super) read_bytes: u64,
    pub(super) read_calls: u64,
    pub(super) read_blocks: u64,
    pub(super) write_bytes: u64,
    pub(super) write_blocks: u64,
    pub(super) fsync_operations: u64,
    pub(super) cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
    pub(super) peak_configured_cache_window_bytes: u64,
    pub(super) publications: Vec<(String, V4PublicationGuard)>,
}

pub(super) fn compact_v4_binary_carry(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    artifacts: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
) -> Result<V4CompactionWork, GfError> {
    compact_v4_binary_carry_with_cancellation(
        pinned,
        artifacts,
        artifacts_path,
        manifest,
        created,
        &mut || false,
        None,
    )
}

pub(super) fn compact_v4_binary_carry_with_cancellation(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    artifacts: &graphforge_filesystem::StableDirectory,
    artifacts_path: &Path,
    manifest: &mut crate::V4OrdinalIdentityManifest,
    created: &mut HashMap<String, PathBuf>,
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<V4CompactionWork, GfError> {
    let prior_manifest = manifest.clone();
    let prior_created = created.clone();
    match compact_v4_binary_carry_inner(
        pinned,
        artifacts,
        artifacts_path,
        manifest,
        created,
        cancelled,
        allocation,
    ) {
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
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<V4CompactionWork, GfError> {
    let mut work = V4CompactionWork::default();
    loop {
        if cancelled() {
            return Err(storage_err("construction ordinal compaction cancelled"));
        }
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
            cancelled,
            allocation,
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
            cancelled,
            allocation,
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
            cancelled,
            allocation,
        )?;
        work.compactions = work.compactions.saturating_add(1);
    }
    Ok(work)
}

/// Return `(first_generation, last_generation, descriptor_index)`. The first
/// artifact is the immutable construction base. Every later retained forward
/// run closes one contiguous delta interval, so binary-carry level is encoded
/// without expanding the public manifest schema.
pub(super) fn v4_forward_intervals(
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

pub(super) fn read_v4_forward_record(
    reader: &mut impl Read,
) -> Result<Option<([u8; 16], u64)>, GfError> {
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
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
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
    let mut writer = StreamingV4Artifact::create_with_window(
        index,
        "forward-compact",
        cache_window,
        allocation,
    )?;
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
            if cancelled() {
                return Err(storage_err("construction ordinal compaction cancelled"));
            }
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
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
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
                allocation,
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
                            if cancelled() {
                                return Err(storage_err(
                                    "construction ordinal compaction cancelled",
                                ));
                            }
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
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
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
    let mut merged = V4TombstoneStreamWriter::new_with_cache_window(
        index,
        last_generation,
        cache_window,
        allocation,
    )?;
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
            if cancelled() {
                return Err(storage_err("construction ordinal compaction cancelled"));
            }
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

#[cfg(test)]
mod tests;
