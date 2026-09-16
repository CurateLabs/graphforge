//! Construction identity encoding, merge cursors, and guarded recovery.

use super::AuthenticatedUuidIndexSnapshot;
use super::BULK_IO_BYTES;
use super::CONSTRUCTION_INTENT;
use super::ConstructionIndexEncoding;
use super::ConstructionIndexOutput;
use super::FORMAT_VERSION;
use super::FileRecord;
use super::IDENTITY_RECORD_BYTES;
use super::IDENTITY_RECORD_WIDTH;
use super::MANIFEST;
use super::MAX_MANIFEST_BYTES;
use super::Manifest;
use super::NODE_LOOKUP_RECORD_BYTES;
use super::NODE_LOOKUP_RECORD_WIDTH;
use super::RunRecord;
use super::TopologyIndexReceipt;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::block_matches;
use super::describe_stream;
use super::hex_bytes;
use super::identity_codec;
use super::maintenance::manifest_file_names;
use super::ordinal_artifacts::V4AuthorityTransactionProof;
use super::ordinal_artifacts::V4PublicationGuard;
use super::ordinal_artifacts::admit_v4_construction_manifest;
use super::ordinal_artifacts::cleanup_v4_publication;
use super::ordinal_artifacts::commit_v4_publications;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::topology_delta::read_bounded;
use super::v4_authority_failure;
use super::validate_run_descriptors;
use crate::construction_record_layout::BASE_IDENTITY_WIDTH as CONSTRUCTION_IDENTITY_WIDTH;
use crate::construction_record_layout::IDENTITY_SURROGATE_OFFSET;
use graphforge_core::GfError;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use uuid::Uuid;

#[derive(Default)]
pub(super) struct ConstructionIndexWork {
    read_bytes: u64,
    read_operations: u64,
    pub(super) write_bytes: u64,
    pub(super) write_operations: u64,
    pub(super) fsync_operations: u64,
    created_runs: u64,
    retained_runs: u64,
    retained_payload_bytes: u64,
    peak_buffer_bytes: u64,
    peak_temporary_bytes: u64,
    cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

pub(super) fn merge_cache_release_evidence(
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
    allocation: Option<crate::StorageAllocationOperation>,
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
            let _ = cleanup_private_construction_index_with_allocation(
                self.encoded,
                self.allocation.as_ref(),
            );
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
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<ConstructionIndexEncoding, GfError> {
    cleanup_private_construction_index_with_allocation(encoded, allocation)?;
    let mut cleanup_guard = ConstructionIndexCleanupGuard {
        encoded,
        armed: true,
        allocation: allocation.cloned(),
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
        allocation,
    );
    match result {
        Ok(value) => {
            cleanup_guard.disarm();
            Ok(value)
        }
        Err(original) => {
            cleanup_private_construction_index_with_allocation(encoded, allocation).map_err(
                |cleanup| storage_err(format!("{original}; exact cleanup also failed: {cleanup}")),
            )?;
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
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<ConstructionIndexEncoding, GfError> {
    let encoded =
        crate::construction_directory::ConstructionDirectory::from_physical(encoded, allocation)
            .map_err(storage_err)?;
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
    if input_len % CONSTRUCTION_IDENTITY_WIDTH as u64 != 0 {
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
    let aligned_input_bytes =
        (BULK_IO_BYTES / CONSTRUCTION_IDENTITY_WIDTH) * CONSTRUCTION_IDENTITY_WIDTH;
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
            let mut packed_len = 0;
            for source_offset in (0..count).step_by(CONSTRUCTION_IDENTITY_WIDTH) {
                let record =
                    &input_block[source_offset..source_offset + CONSTRUCTION_IDENTITY_WIDTH];
                let mut packed = [0_u8; IDENTITY_RECORD_WIDTH];
                packed[..17].copy_from_slice(&record[..17]);
                packed[17..].copy_from_slice(
                    &record[IDENTITY_SURROGATE_OFFSET..CONSTRUCTION_IDENTITY_WIDTH],
                );
                let uuid: [u8; 16] = record[..16].try_into().expect("fixed UUID");
                if previous_uuid.is_some_and(|prior| prior >= uuid) || record[17] != 0 {
                    return Err(storage_err("construction identity stream is not canonical"));
                }
                previous_uuid = Some(uuid);
                match record[16] {
                    0 => {
                        let surrogate = u64::from_be_bytes(
                            record[IDENTITY_SURROGATE_OFFSET..CONSTRUCTION_IDENTITY_WIDTH]
                                .try_into()
                                .expect("fixed"),
                        );
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
                        // Edge surrogates belong to topology; membership stores only the UUID.
                        packed[17..].fill(0);
                        edge_count = edge_count.saturating_add(1);
                    }
                    _ => return Err(storage_err("construction identity kind is invalid")),
                }
                let packed = identity_codec::encoded(&packed)?;
                input_block[packed_len..packed_len + packed.len()].copy_from_slice(packed);
                packed_len += packed.len();
            }
            identity_writer
                .write_all(&input_block[..packed_len])
                .map_err(storage_err)?;
            work.write_bytes = work.write_bytes.saturating_add(packed_len as u64);
            work.write_operations = work.write_operations.saturating_add(1);
            remaining -= count as u64;
        }
        if hex_bytes(&source_digest.finalize()) != identities_sha256 {
            return Err(storage_err("construction identity source digest changed"));
        }
        Ok(())
    })();
    let observed = (|| {
        index
            .observe_file(std::ffi::OsStr::new(&identity_temp), identity_writer.file())
            .map_err(storage_err)?;
        index
            .observe_file(
                std::ffi::OsStr::new(&surrogate_temp),
                surrogate_writer.file(),
            )
            .map_err(storage_err)
    })();
    let streamed = combine_v4_cleanup(streamed, observed, "construction allocation observation");
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
    index
        .observe_file(std::ffi::OsStr::new(&identity_temp), identity_writer.file())
        .map_err(storage_err)?;
    index
        .observe_file(
            std::ffi::OsStr::new(&surrogate_temp),
            surrogate_writer.file(),
        )
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
        "identities-v5",
        generation,
        IDENTITY_RECORD_WIDTH,
        &mut artifacts,
        &mut work,
    )?;
    let surrogate_record = describe_and_install_construction_run(
        &index,
        &surrogate_temp,
        surrogate_identity,
        "node-surrogates-v5",
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
            "identities-v5-base",
            0,
            IDENTITY_RECORD_WIDTH,
            &mut artifacts,
            &mut work,
        )?;
        let base_surrogate = install_empty_construction_run(
            &index,
            "node-surrogates-v5-base",
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
        install_construction_bytes(index.physical(), MANIFEST, &body, &mut work, allocation)?;
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

#[cfg(test)]
fn cleanup_private_construction_index(
    encoded: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    cleanup_private_construction_index_with_allocation(encoded, None)
}

fn cleanup_private_construction_index_with_allocation(
    encoded: &graphforge_filesystem::StableDirectory,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let encoded =
        crate::construction_directory::ConstructionDirectory::from_physical(encoded, allocation)
            .map_err(storage_err)?;
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
    let names = if allocation.is_some() {
        index.child_names_bounded(1_000_000)
    } else {
        index.child_names()
    }
    .map_err(storage_err)?;
    let v4_allowed = authenticate_private_v4_residue(index.physical(), &names)?;
    if allocation.is_some() {
        for name in &names {
            let file = index.open_child_file(name).map_err(storage_err)?;
            index.observe_file(name, &file).map_err(storage_err)?;
        }
    }
    for name in names {
        let name_text = name
            .to_str()
            .ok_or_else(|| storage_err("construction recovery inventory name is not UTF-8"))?;
        if name_text != CONSTRUCTION_INTENT
            && name_text != MANIFEST
            && !name_text.starts_with(".construction-")
            && !name_text.starts_with(".manifest.json-")
            && !name_text.starts_with("identities-v5")
            && !name_text.starts_with("node-surrogates-v5")
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

pub(super) fn authenticate_private_v4_artifact_file(
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
    index: &crate::construction_directory::ConstructionDirectory,
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
    let written = file.write_all(&body).map_err(storage_err);
    let observed = index
        .observe_file(std::ffi::OsStr::new(&temporary), &file)
        .map_err(storage_err);
    written?;
    observed?;
    work.write_bytes = work.write_bytes.saturating_add(body.len() as u64);
    work.write_operations = work.write_operations.saturating_add(1);
    file.sync_all().map_err(storage_err)?;
    index
        .observe_file(std::ffi::OsStr::new(&temporary), &file)
        .map_err(storage_err)?;
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
    output: &crate::construction_directory::ConstructionDirectory,
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
                &format!("identities-v5-l{}", level + 1),
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
                &format!("node-surrogates-v5-l{}", level + 1),
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
    output: &crate::construction_directory::ConstructionDirectory,
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
    let observed = output
        .observe_file(std::ffi::OsStr::new(&temporary), writer.file())
        .map_err(storage_err);
    let merged = combine_v4_cleanup(merged, observed, "construction merge allocation");
    merged?;
    writer.sync_all_and_release().map_err(storage_err)?;
    output
        .observe_file(std::ffi::OsStr::new(&temporary), writer.file())
        .map_err(storage_err)?;
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

    fn record_width(&self) -> usize {
        if self.width == IDENTITY_RECORD_WIDTH && self.block[self.within + 16] == 1 {
            identity_codec::EDGE_WIDTH
        } else {
            self.width
        }
    }

    fn current(&self) -> Option<&[u8]> {
        (!self.finished).then(|| &self.block[self.within..self.within + self.record_width()])
    }

    fn advance(&mut self) -> Result<(), GfError> {
        if self.finished {
            return Ok(());
        }
        self.within += self.record_width();
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
        if expected.offset != self.read_bytes
            || expected.len == 0
            || expected.len as usize > BULK_IO_BYTES
        {
            return Err(storage_err("construction merge block framing changed"));
        }
        self.block.resize(expected.len as usize, 0);
        self.file.read_exact(&mut self.block).map_err(storage_err)?;
        self.read_bytes = self.read_bytes.saturating_add(self.block.len() as u64);
        self.read_operations = self.read_operations.saturating_add(1);
        if !block_matches(&self.block, expected, self.width) {
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

pub(super) fn combine_cache_cleanup<T>(
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

pub(super) fn combine_v4_cleanup<T>(
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
    output: &crate::construction_directory::ConstructionDirectory,
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
    output: &crate::construction_directory::ConstructionDirectory,
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
    output: &crate::construction_directory::ConstructionDirectory,
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
    let mut file =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage_err)?;
    let mut reads = (0_u64, 0_u64);
    let described = describe_stream(&mut file, width, &mut reads);
    work.read_bytes = work.read_bytes.saturating_add(reads.0);
    work.read_operations = work.read_operations.saturating_add(reads.1);
    let released = file.finish().map_err(storage_err);
    let ((sha256, blocks, count), read_cache_release) = match (described, released) {
        (Ok(described), Ok(released)) => (described, released),
        (Ok(_), Err(release)) => return Err(release),
        (Err(primary), Ok(_)) => return Err(primary),
        (Err(primary), Err(release)) => {
            return Err(storage_err(format!(
                "{primary}; construction run cache release also failed: {release}"
            )));
        }
    };
    merge_cache_release_evidence(&mut work.cache_release, read_cache_release);
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
        count,
        sha256,
        blocks,
    })
}

pub(super) fn install_construction_bytes(
    output: &graphforge_filesystem::StableDirectory,
    name: &str,
    bytes: &[u8],
    work: &mut ConstructionIndexWork,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(ConstructionIndexOutput, V4PublicationGuard), GfError> {
    let temporary = format!(".{name}-{}.tmp", Uuid::new_v4().simple());
    let mut publication =
        V4PublicationGuard::create(output, &temporary, allocation).map_err(storage_err)?;
    let mut file = publication.take_file().map_err(storage_err)?;
    let written = file.write_all(bytes).map_err(storage_err);
    let observed = publication.observe(&file).map_err(storage_err);
    written?;
    observed?;
    work.write_bytes = work.write_bytes.saturating_add(bytes.len() as u64);
    work.write_operations = work.write_operations.saturating_add(1);
    work.peak_temporary_bytes = work
        .peak_temporary_bytes
        .max(u64::try_from(bytes.len()).map_err(storage_err)?);
    file.sync_all().map_err(storage_err)?;
    publication.observe(&file).map_err(storage_err)?;
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

#[cfg(test)]
mod tests;
