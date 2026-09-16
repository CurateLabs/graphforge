//! Ordinal artifact writers and their publication guards.

use super::ConstructionIndexOutput;
use super::TopologyIndexReceipt;
use super::V4_MAX_RANGES;
use super::V4_ORDINAL_BLOCK_BYTES;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::V4OrdinalBuildMetrics;
use super::construction::ConstructionIndexWork;
use super::construction::authenticate_private_v4_artifact_file;
use super::construction::combine_v4_cleanup;
use super::construction::install_construction_bytes;
use super::construction::merge_cache_release_evidence;
use super::construction_ordinal_event;
use super::hex_bytes;
use super::ordinal_compaction::compact_v4_binary_carry_with_cancellation;
use super::ordinal_compaction::v4_forward_intervals;
use super::storage_err;
use super::take_v4_output_cleanup_failure;
use super::topology_delta::hex_sha256;
use super::v4_authority_failure;
use super::v4_publication_failure;
use super::v4_publication_io_failure;
use graphforge_core::GfError;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct V4ConstructionArtifactBundle {
    pub(crate) manifest: crate::V4OrdinalIdentityManifest,
    pub(crate) metrics: V4OrdinalBuildMetrics,
    pub(super) publications: Vec<(String, V4PublicationGuard)>,
}

// Publication ownership carries the optional observer through explicit cleanup
// and ordinary RAII cleanup. Only current file owners and a high-water survive.
#[derive(Debug)]
pub(super) struct V4PublicationGuard {
    directory: Option<graphforge_filesystem::StableDirectory>,
    inner: Option<graphforge_filesystem::UnpublishedArtifactGuard>,
    path: PathBuf,
    allocation: Option<crate::StorageAllocationOperation>,
}
impl V4PublicationGuard {
    pub(super) fn create(
        directory: &graphforge_filesystem::StableDirectory,
        name: &str,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            directory: allocation.map(|_| directory.try_clone()).transpose()?,
            inner: Some(
                directory.create_unpublished_replaceable_child(std::ffi::OsStr::new(name))?,
            ),
            path: directory.path().join(name),
            allocation: allocation.cloned(),
        })
    }
    fn inner(&self) -> &graphforge_filesystem::UnpublishedArtifactGuard {
        self.inner.as_ref().expect("live publication")
    }
    fn inner_mut(&mut self) -> &mut graphforge_filesystem::UnpublishedArtifactGuard {
        self.inner.as_mut().expect("live publication")
    }
    fn identity(&self) -> std::io::Result<graphforge_filesystem::FileIdentity> {
        self.inner().identity()
    }
    fn verify_identity_with(
        &mut self,
        check: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<graphforge_filesystem::FileIdentity> {
        self.inner_mut().verify_identity_with(check)
    }
    pub(super) fn take_file(&mut self) -> std::io::Result<File> {
        self.inner_mut().take_file()
    }
    fn open_sibling(&self, name: &std::ffi::OsStr) -> std::io::Result<File> {
        self.inner().open_sibling(name)
    }
    pub(super) fn observe(&self, file: &File) -> std::io::Result<()> {
        if let Some(allocation) = &self.allocation {
            allocation
                .replace_file_at(&self.path, file)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }
    fn observe_named(&self) -> std::io::Result<()> {
        if let Some(directory) = &self.directory {
            match directory.open_child_file(self.path.file_name().expect("artifact name")) {
                Ok(file) => {
                    if let Ok(expected) = self.identity()
                        && graphforge_filesystem::file_identity(&file)? != expected
                    {
                        return Err(std::io::Error::other("observed artifact identity changed"));
                    }
                    self.observe(&file)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn reconcile_removed(
        &self,
        expected: Option<graphforge_filesystem::FileIdentity>,
    ) -> std::io::Result<()> {
        let Some(directory) = &self.directory else {
            return Ok(());
        };
        let removed = match directory.open_child_file(self.path.file_name().expect("artifact name"))
        {
            Ok(file) => match expected {
                Some(expected) => graphforge_filesystem::file_identity(&file)? != expected,
                None => {
                    return Err(std::io::Error::other(
                        "artifact identity unavailable during cleanup",
                    ));
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(error),
        };
        if removed {
            self.allocation
                .as_ref()
                .expect("observed directory")
                .remove_file_at(&self.path)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }
    pub(super) fn install_child(&mut self, target: &std::ffi::OsStr) -> std::io::Result<()> {
        self.observe_named()?;
        let installed = self.inner_mut().install_child(target);
        if installed.is_err() {
            // Installation can rename successfully before a later validation
            // fails. Rebind only the actual retained artifact, never a collision.
            if let Some(directory) = &self.directory
                && let Ok(file) = directory.open_child_file(target)
                && matches!(
                    (graphforge_filesystem::file_identity(&file), self.identity()),
                    (Ok(actual), Ok(expected)) if actual == expected
                )
            {
                let previous = self.path.clone();
                self.path.set_file_name(target);
                let _ = self.observe(&file);
                if let Some(allocation) = &self.allocation {
                    let _ = allocation.remove_file_at(&previous);
                }
            }
            return installed;
        }
        let previous = self.path.clone();
        self.path.set_file_name(target);
        self.observe_named()?;
        if let Some(allocation) = &self.allocation {
            allocation
                .remove_file_at(&previous)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }
    pub(super) fn sync_parent(&mut self) -> std::io::Result<()> {
        self.inner_mut().sync_parent()
    }
    fn commit(mut self) -> std::io::Result<()> {
        let identity = self.identity().ok();
        let committed = self.inner.take().expect("live publication").commit();
        if committed.is_err() {
            let _ = self.reconcile_removed(identity);
        }
        committed
    }
    fn cleanup_checked(
        &mut self,
        check: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let observed = self.observe_named();
        let identity = self.identity().ok();
        let cleaned = self.inner_mut().cleanup_checked(check);
        // Cleanup may unlink successfully then fail its directory barrier or
        // finalization hook. Release the absent route even on that error.
        let reconciled = self.reconcile_removed(identity);
        cleaned?;
        observed?;
        reconciled
    }
}
impl Drop for V4PublicationGuard {
    fn drop(&mut self) {
        if self.inner.is_some() {
            let _ = self.cleanup_checked(|| Ok(()));
        }
    }
}

pub(super) struct V4AuthorityTransactionProof;

pub(super) fn commit_v4_publications(
    publications: Vec<(String, V4PublicationGuard)>,
    _proof: V4AuthorityTransactionProof,
) -> Result<(), GfError> {
    for (_, publication) in publications {
        publication.commit().map_err(storage_err)?;
    }
    Ok(())
}

pub(super) fn retain_v4_publication(
    publications: &mut Vec<(String, V4PublicationGuard)>,
    name: String,
    publication: Option<V4PublicationGuard>,
) {
    if let Some(publication) = publication {
        publications.push((name, publication));
    }
}

pub(super) fn cleanup_v4_publication(publication: &mut V4PublicationGuard) -> Result<(), GfError> {
    publication
        .cleanup_checked(|| {
            take_v4_output_cleanup_failure()
                .map_err(|_| std::io::Error::other("injected v4 output cleanup failure"))
        })
        .map_err(storage_err)
}

pub(super) fn clone_pinned_v4_file(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    name: &str,
) -> Result<File, GfError> {
    let mut file = pinned
        .artifacts
        .iter()
        .find(|artifact| artifact.descriptor.name == name)
        .ok_or_else(|| storage_err("authenticated v4 update input is absent"))?
        .file
        .try_clone()
        .map_err(storage_err)?;
    // Update inputs are writer-exclusive retained handles. `try_clone` may
    // share an OS file offset with the retained handle, so every sequential
    // consumer must establish its own logical start before reading.
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    Ok(file)
}

/// Encode the already-canonical construction node stream without rescanning
/// topology or consulting v3 reverse authority. The caller owns the durable
/// rewrite transaction and publishes the returned manifest later.
#[cfg(test)]
pub(crate) fn stage_v4_ordinal_artifacts<I, F>(
    records: I,
    generation: u64,
    index: &graphforge_filesystem::StableDirectory,
    mut cancelled: F,
) -> Result<(crate::V4OrdinalIdentityManifest, V4OrdinalBuildMetrics), GfError>
where
    I: IntoIterator<Item = (Uuid, u64)>,
    F: FnMut() -> bool,
{
    let bundle = stage_v4_ordinal_bundle(records, generation, index, &mut cancelled)?;
    index.sync().map_err(storage_err)?;
    let V4ConstructionArtifactBundle {
        manifest,
        metrics,
        publications,
    } = bundle;
    commit_v4_publications(publications, V4AuthorityTransactionProof)?;
    Ok((manifest, metrics))
}

#[cfg(test)]
pub(super) fn stage_v4_ordinal_bundle<I, F>(
    records: I,
    generation: u64,
    index: &graphforge_filesystem::StableDirectory,
    cancelled: &mut F,
) -> Result<V4ConstructionArtifactBundle, GfError>
where
    I: IntoIterator<Item = (Uuid, u64)>,
    F: FnMut() -> bool,
{
    let mut writer = V4OrdinalConstructionWriter::start(generation, index)?;
    for (uuid, node_id) in records {
        writer.push_pair(uuid, node_id, cancelled)?;
    }
    writer.finish()
}

/// Incremental v4 encoder used to tee the already-assigned fresh-construction
/// node stream without retaining it or reading it a second time.
pub(crate) struct V4OrdinalConstructionWriter<'a> {
    allocation: Option<crate::StorageAllocationOperation>,
    generation: u64,
    index: &'a graphforge_filesystem::StableDirectory,
    cache_window: std::num::NonZeroU64,
    forward: StreamingV4Artifact,
    ranges: Vec<crate::V4OrdinalRange>,
    publications: Vec<(String, V4PublicationGuard)>,
    current: Option<V4OrdinalRangeWriter>,
    previous_forward_uuid: Option<[u8; 16]>,
    previous_ordinal_node_id: u64,
    forward_count: u64,
    ordinal_count: u64,
    forward_commitment: [u8; 32],
    ordinal_commitment: [u8; 32],
    forward_commitment_two: [u8; 32],
    ordinal_commitment_two: [u8; 32],
    metrics: V4OrdinalBuildMetrics,
}

impl<'a> V4OrdinalConstructionWriter<'a> {
    pub(crate) fn start(
        generation: u64,
        index: &'a graphforge_filesystem::StableDirectory,
    ) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage_err)?;
        Self::start_with_cache_window(generation, index, cache_window, None)
    }

    pub(crate) fn start_with_allocation(
        generation: u64,
        index: &'a graphforge_filesystem::StableDirectory,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage_err)?;
        Self::start_with_cache_window(generation, index, cache_window, allocation)
    }

    pub(crate) fn start_with_cache_window(
        generation: u64,
        index: &'a graphforge_filesystem::StableDirectory,
        cache_window: std::num::NonZeroU64,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        if generation == 0 {
            return Err(storage_err("v4 ordinal generation is zero"));
        }
        let forward =
            StreamingV4Artifact::create_with_window(index, "forward", cache_window, allocation)?;
        let validated = graphforge_filesystem::validate_cache_release_operation_windows(&[forward
            .writer
            .get_ref()
            .window_bytes()])
        .map_err(storage_err)
        .and_then(|_| v4_publication_failure("window_validation"));
        if let Err(primary) = validated {
            let (_, cleanup) = forward.cleanup_unpublished();
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup cleanup");
        }
        Ok(Self {
            generation,
            index,
            allocation: allocation.cloned(),
            cache_window,
            forward,
            ranges: Vec::new(),
            publications: Vec::new(),
            current: None,
            previous_forward_uuid: None,
            previous_ordinal_node_id: 0,
            forward_count: 0,
            ordinal_count: 0,
            forward_commitment: [0; 32],
            ordinal_commitment: [0; 32],
            forward_commitment_two: [0; 32],
            ordinal_commitment_two: [0; 32],
            metrics: V4OrdinalBuildMetrics {
                // Forward BufWriter + ordinal BufWriter + ordinal authentication block.
                peak_buffer_bytes: V4_ORDINAL_BLOCK_BYTES * 3,
                ..Default::default()
            },
        })
    }

    pub(crate) fn push_pair(
        &mut self,
        uuid: Uuid,
        node_id: u64,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_forward_inner(uuid, node_id)?;
        self.push_ordinal_inner(node_id, uuid)
    }

    pub(crate) fn push_forward(
        &mut self,
        uuid: Uuid,
        node_id: u64,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_forward_inner(uuid, node_id)
    }

    pub(crate) fn push_ordinal(
        &mut self,
        node_id: u64,
        uuid: Uuid,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        self.poll_cancelled(cancelled)?;
        self.push_ordinal_inner(node_id, uuid)
    }

    fn poll_cancelled(&mut self, cancelled: &mut impl FnMut() -> bool) -> Result<(), GfError> {
        self.metrics.cancellation_polls = self
            .metrics
            .cancellation_polls
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 cancellation poll count overflow"))?;
        if cancelled() {
            return Err(storage_err("v4 ordinal construction cancelled"));
        }
        Ok(())
    }

    fn push_forward_inner(&mut self, uuid: Uuid, node_id: u64) -> Result<(), GfError> {
        let uuid_bytes = *uuid.as_bytes();
        if uuid_bytes == [0; 16]
            || self
                .previous_forward_uuid
                .is_some_and(|prior| prior >= uuid_bytes)
            || node_id == 0
        {
            return Err(storage_err(
                "v4 forward identities are not canonical and increasing",
            ));
        }
        self.forward.push(&uuid_bytes)?;
        self.forward.push(&node_id.to_be_bytes())?;
        add_v4_mapping_commitment(&mut self.forward_commitment, 0, uuid_bytes, node_id);
        add_v4_mapping_commitment(&mut self.forward_commitment_two, 1, uuid_bytes, node_id);
        self.forward_count = self
            .forward_count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 forward record count overflow"))?;
        self.previous_forward_uuid = Some(uuid_bytes);
        self.metrics.peak_temporary_bytes =
            self.metrics.peak_temporary_bytes.max(self.forward.bytes);
        Ok(())
    }

    fn push_ordinal_inner(&mut self, node_id: u64, uuid: Uuid) -> Result<(), GfError> {
        let uuid_bytes = *uuid.as_bytes();
        if uuid_bytes == [0; 16] || node_id == 0 || node_id <= self.previous_ordinal_node_id {
            return Err(storage_err(
                "v4 ordinal identities are not canonical and increasing",
            ));
        }
        if self.current.is_some()
            && self
                .previous_ordinal_node_id
                .checked_add(1)
                .is_none_or(|expected| node_id != expected)
        {
            finish_streamed_v4_range(
                self.generation,
                self.current.take().expect("range exists"),
                &mut self.ranges,
                &mut self.publications,
                &mut self.metrics,
            )?;
        }
        if self.current.is_none() {
            if self.ranges.len() >= V4_MAX_RANGES {
                return Err(storage_err("v4 ordinal range inventory exceeds bound"));
            }
            self.current = Some(V4OrdinalRangeWriter::new(
                self.index,
                self.ranges.len(),
                node_id,
                self.cache_window,
                self.allocation.as_ref(),
            )?);
        }
        self.current
            .as_mut()
            .expect("range exists")
            .push(uuid_bytes)?;
        add_v4_mapping_commitment(&mut self.ordinal_commitment, 0, uuid_bytes, node_id);
        add_v4_mapping_commitment(&mut self.ordinal_commitment_two, 1, uuid_bytes, node_id);
        self.ordinal_count = self
            .ordinal_count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 ordinal record count overflow"))?;
        self.previous_ordinal_node_id = node_id;
        let live_temporary_bytes = self
            .metrics
            .artifact_bytes
            .checked_add(self.forward.bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.current
                        .as_ref()
                        .map_or(0, |range| range.artifact.bytes),
                )
            })
            .ok_or_else(|| storage_err("v4 live temporary byte count overflow"))?;
        self.metrics.peak_temporary_bytes =
            self.metrics.peak_temporary_bytes.max(live_temporary_bytes);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<V4ConstructionArtifactBundle, GfError> {
        if self.forward_count != self.ordinal_count
            || self.forward_commitment != self.ordinal_commitment
            || self.forward_commitment_two != self.ordinal_commitment_two
        {
            return Err(storage_err(
                "v4 forward and ordinal projections describe different mappings",
            ));
        }
        self.metrics.input_records = self.forward_count;
        if let Some(writer) = self.current.take() {
            finish_streamed_v4_range(
                self.generation,
                writer,
                &mut self.ranges,
                &mut self.publications,
                &mut self.metrics,
            )?;
        }
        let forward = finish_streamed_v4_artifact(
            self.forward,
            "forward-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::ForwardIdentities,
            &mut self.metrics,
        )?;

        // Construction has no deletions, but an explicit authenticated empty run
        // commits that fact instead of leaving tombstone authority implicit.
        let tombstone = finish_streamed_v4_artifact(
            StreamingV4Artifact::create_with_window(
                self.index,
                "tombstones",
                self.cache_window,
                self.allocation.as_ref(),
            )?,
            "tombstones-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::NodeTombstones,
            &mut self.metrics,
        )?;
        self.metrics.ranges = self.ranges.len();
        let manifest = crate::V4OrdinalIdentityManifest {
            format_version: crate::ORDINAL_IDENTITY_V4,
            topology_generation: self.generation,
            forward_identities: vec![forward.artifact],
            ordinal_ranges: self.ranges,
            tombstones: vec![crate::V4OrdinalTombstones {
                generation: self.generation,
                artifact: tombstone.artifact,
                blocks: Vec::new(),
            }],
        };
        v4_publication_failure("manifest_update")?;
        admit_v4_construction_manifest(&manifest)?;
        retain_v4_publication(
            &mut self.publications,
            manifest.forward_identities[0].name.clone(),
            forward.publication,
        );
        retain_v4_publication(
            &mut self.publications,
            manifest.tombstones[0].artifact.name.clone(),
            tombstone.publication,
        );
        Ok(V4ConstructionArtifactBundle {
            manifest,
            metrics: self.metrics,
            publications: self.publications,
        })
    }
}

pub(super) fn admit_v4_construction_manifest(
    manifest: &crate::V4OrdinalIdentityManifest,
) -> Result<(), GfError> {
    let bytes = serde_json::to_vec(manifest).map_err(storage_err)?;
    if bytes.len() as u64 > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES {
        return Err(storage_err(
            "v4 ordinal manifest exceeds the reader admission bound",
        ));
    }
    Ok(())
}

fn add_v4_mapping_commitment(commitment: &mut [u8; 32], domain: u8, uuid: [u8; 16], node_id: u64) {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-v4-mapping-v1\0");
    digest.update([domain]);
    digest.update(uuid);
    digest.update(node_id.to_be_bytes());
    let mapping: [u8; 32] = digest.finalize().into();
    let mut carry = 0_u16;
    for (target, value) in commitment.iter_mut().rev().zip(mapping.iter().rev()) {
        let sum = u16::from(*target) + u16::from(*value) + carry;
        *target = sum.to_be_bytes()[1];
        carry = sum >> 8;
    }
}

pub(super) struct StreamingV4Artifact {
    pub(super) writer: BufWriter<graphforge_filesystem::DurableFileCacheWriter>,
    publication: V4PublicationGuard,
    digest: Sha256,
    pub(super) bytes: u64,
}

impl StreamingV4Artifact {
    pub(super) fn create_with_window(
        index: &graphforge_filesystem::StableDirectory,
        role: &str,
        cache_window: std::num::NonZeroU64,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        let temporary_name = format!(".v4-{role}-{}.tmp", Uuid::new_v4().simple());
        let mut publication =
            V4PublicationGuard::create(index, &temporary_name, allocation).map_err(storage_err)?;
        if let Err(primary) = publication
            .verify_identity_with(|| v4_publication_io_failure("initial_file_identity"))
            .map_err(storage_err)
        {
            let cleanup = cleanup_v4_publication(&mut publication);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup cleanup");
        }
        let file = publication.take_file().map_err(storage_err)?;
        let writer = match graphforge_filesystem::DurableFileCacheWriter::with_window_bytes_checked(
            file,
            cache_window,
            || v4_publication_io_failure("writer_construction"),
        ) {
            Ok(writer) => writer,
            Err(primary) => {
                let cleanup = cleanup_v4_publication(&mut publication);
                return combine_v4_cleanup(Err(storage_err(primary)), cleanup, "v4 setup cleanup");
            }
        };
        Ok(Self {
            writer: BufWriter::with_capacity(V4_ORDINAL_BLOCK_BYTES, writer),
            publication,
            digest: Sha256::new(),
            bytes: 0,
        })
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> Result<(), GfError> {
        let flushes =
            self.writer.buffer().len().saturating_add(bytes.len()) > self.writer.capacity();
        let written = self.writer.write_all(bytes).map_err(storage_err);
        let observed = if flushes || written.is_err() {
            self.publication
                .observe(self.writer.get_ref().file())
                .map_err(storage_err)
        } else {
            Ok(())
        };
        written?;
        observed?;
        self.digest.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(storage_err)?)
            .ok_or_else(|| storage_err("v4 artifact length overflow"))?;
        Ok(())
    }

    pub(super) fn cleanup_unpublished(
        mut self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        let flushed = self.writer.flush().map_err(storage_err);
        let synchronized = self
            .writer
            .get_mut()
            .sync_all_and_release()
            .map_err(storage_err);
        let observed = self
            .publication
            .observe(self.writer.get_ref().file())
            .map_err(storage_err);
        let cache_release = self.writer.get_ref().evidence();
        let synchronized = combine_v4_cleanup(synchronized, observed, "v4 allocation observation");
        let finalized = combine_v4_cleanup(flushed, synchronized, "v4 output synchronization");
        drop(self.writer);
        let removed = cleanup_v4_publication(&mut self.publication);
        (
            cache_release,
            combine_v4_cleanup(finalized, removed, "v4 output removal"),
        )
    }
}

#[derive(Debug)]
pub(super) struct GuardedV4Artifact {
    pub(super) artifact: crate::V4OrdinalArtifact,
    pub(super) publication: Option<V4PublicationGuard>,
}

#[allow(clippy::too_many_lines)]
pub(super) fn finish_streamed_v4_artifact(
    mut writer: StreamingV4Artifact,
    prefix: &str,
    generation: u64,
    kind: crate::V4OrdinalArtifactKind,
    metrics: &mut V4OrdinalBuildMetrics,
) -> Result<GuardedV4Artifact, GfError> {
    let finalized = writer.writer.flush().map_err(storage_err).and_then(|()| {
        writer
            .writer
            .get_mut()
            .sync_all_and_release()
            .map_err(storage_err)
    });
    if let Err(primary) = finalized {
        let (cache_release, cleanup) = writer.cleanup_unpublished();
        merge_cache_release_evidence(&mut metrics.cache_release, cache_release);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 failed output cleanup");
    }
    writer
        .publication
        .observe(writer.writer.get_ref().file())
        .map_err(storage_err)?;
    let cache_release = writer.writer.get_ref().evidence();
    let prepared = (|| -> Result<(_, _), GfError> {
        v4_publication_failure("fsync_evidence_overflow")?;
        let mut committed_metrics = metrics.clone();
        merge_cache_release_evidence(&mut committed_metrics.cache_release, cache_release);
        committed_metrics.fsync_operations = committed_metrics
            .fsync_operations
            .checked_add(cache_release.sync_operations)
            .ok_or_else(|| storage_err("v4 fsync count overflow"))?;
        v4_publication_failure("final_file_identity")?;
        let identity = graphforge_filesystem::file_identity(writer.writer.get_ref().file())
            .map_err(storage_err)?;
        if identity != writer.publication.identity().map_err(storage_err)? {
            return Err(storage_err("v4 final output identity changed"));
        }
        v4_publication_failure("file_space_usage")?;
        let space = graphforge_filesystem::file_space_usage(writer.writer.get_ref().file())
            .map_err(storage_err)?;
        if space.logical_bytes != writer.bytes {
            return Err(storage_err("v4 final output length changed"));
        }
        let sha256 = hex_bytes(&writer.digest.clone().finalize());
        let artifact = crate::V4OrdinalArtifact {
            name: format!("{prefix}-{generation}-{}.uuidx", &sha256[..16]),
            kind,
            generation,
            bytes: writer.bytes,
            sha256,
        };
        committed_metrics.artifact_bytes = committed_metrics
            .artifact_bytes
            .checked_add(artifact.bytes)
            .ok_or_else(|| storage_err("v4 aggregate artifact length overflow"))?;
        committed_metrics.peak_temporary_bytes = committed_metrics
            .peak_temporary_bytes
            .max(committed_metrics.artifact_bytes);
        committed_metrics.write_blocks = committed_metrics
            .write_blocks
            .checked_add(artifact.bytes.div_ceil(V4_ORDINAL_BLOCK_BYTES as u64))
            .ok_or_else(|| storage_err("v4 write block count overflow"))?;
        Ok((artifact, committed_metrics))
    })();
    let (artifact, committed_metrics) = match prepared {
        Ok(prepared) => prepared,
        Err(primary) => {
            let (cleanup_evidence, cleanup) = writer.cleanup_unpublished();
            merge_cache_release_evidence(&mut metrics.cache_release, cleanup_evidence);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 setup publication cleanup");
        }
    };
    drop(writer.writer);
    let published = (|| -> Result<bool, GfError> {
        v4_publication_failure("replace_child")?;
        match writer
            .publication
            .install_child(std::ffi::OsStr::new(&artifact.name))
        {
            Ok(()) => {
                writer.publication.sync_parent().map_err(storage_err)?;
                v4_publication_failure("directory_sync")?;
                v4_publication_failure("post_publication_metric_overflow")?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = writer
                    .publication
                    .open_sibling(std::ffi::OsStr::new(&artifact.name))
                    .map_err(storage_err)?;
                authenticate_private_v4_artifact_file(existing, &artifact)?;
                cleanup_v4_publication(&mut writer.publication)?;
                Ok(false)
            }
            Err(error) => Err(storage_err(error)),
        }
    })();
    let owns_publication = match published {
        Ok(owns_publication) => owns_publication,
        Err(primary) => {
            let cleanup = cleanup_v4_publication(&mut writer.publication);
            return combine_v4_cleanup(Err(primary), cleanup, "v4 publication cleanup");
        }
    };
    let publication = if owns_publication {
        Some(writer.publication)
    } else {
        None
    };
    *metrics = committed_metrics;
    Ok(GuardedV4Artifact {
        artifact,
        publication,
    })
}

pub(super) struct V4OrdinalRangeWriter {
    pub(super) artifact: StreamingV4Artifact,
    first_node_id: u64,
    count: u64,
    block: Vec<u8>,
    blocks: Vec<crate::V4OrdinalBlock>,
}

impl V4OrdinalRangeWriter {
    pub(super) fn new(
        index: &graphforge_filesystem::StableDirectory,
        ordinal: usize,
        first_node_id: u64,
        cache_window: std::num::NonZeroU64,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        Ok(Self {
            artifact: StreamingV4Artifact::create_with_window(
                index,
                &format!("ordinal-{ordinal:08}"),
                cache_window,
                allocation,
            )?,
            first_node_id,
            count: 0,
            block: Vec::with_capacity(V4_ORDINAL_BLOCK_BYTES),
            blocks: Vec::new(),
        })
    }

    pub(super) fn push(&mut self, uuid: [u8; 16]) -> Result<(), GfError> {
        self.artifact.push(&uuid)?;
        self.block.extend_from_slice(&uuid);
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 ordinal range count overflow"))?;
        if self.block.len() == V4_ORDINAL_BLOCK_BYTES {
            self.finish_block()?;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> Result<(), GfError> {
        if self.block.is_empty() {
            return Ok(());
        }
        let offset = u64::try_from(self.blocks.len())
            .map_err(storage_err)?
            .checked_mul(V4_ORDINAL_BLOCK_BYTES as u64)
            .ok_or_else(|| storage_err("v4 ordinal block offset overflow"))?;
        self.blocks.push(crate::V4OrdinalBlock {
            offset,
            count: u64::try_from(self.block.len() / 16).map_err(storage_err)?,
            sha256: hex_sha256(&self.block),
        });
        self.block.clear();
        Ok(())
    }

    pub(super) fn cleanup_unpublished(
        self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        self.artifact.cleanup_unpublished()
    }
}

pub(super) fn finish_streamed_v4_range(
    generation: u64,
    mut writer: V4OrdinalRangeWriter,
    ranges: &mut Vec<crate::V4OrdinalRange>,
    publications: &mut Vec<(String, V4PublicationGuard)>,
    metrics: &mut V4OrdinalBuildMetrics,
) -> Result<(), GfError> {
    let prepared = if writer.count == 0 {
        Err(storage_err("v4 ordinal range is empty"))
    } else {
        writer.finish_block()
    };
    if let Err(primary) = prepared {
        let (cache_release, cleanup) = writer.cleanup_unpublished();
        merge_cache_release_evidence(&mut metrics.cache_release, cache_release);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 ordinal failed output cleanup");
    }
    let artifact = finish_streamed_v4_artifact(
        writer.artifact,
        "ordinal-v4",
        generation,
        crate::V4OrdinalArtifactKind::OrdinalUuids,
        metrics,
    )?;
    let artifact_name = artifact.artifact.name.clone();
    ranges.push(crate::V4OrdinalRange {
        first_node_id: writer.first_node_id,
        count: writer.count,
        artifact: artifact.artifact,
        blocks: writer.blocks,
    });
    retain_v4_publication(publications, artifact_name, artifact.publication);
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct V4OrdinalPublicationMetrics {
    pub(crate) read_bytes: u64,
    pub(crate) read_operations: u64,
    pub(crate) write_bytes: u64,
    pub(crate) write_operations: u64,
    pub(crate) fsync_operations: u64,
    pub(crate) peak_temporary_bytes: u64,
}

pub(crate) fn publish_v4_construction_artifacts(
    encoded: &graphforge_filesystem::StableDirectory,
    bundle: V4ConstructionArtifactBundle,
    generation: u64,
    topology_delta_sha256: &str,
    parent: Option<(
        &crate::ResolvedProjectGeneration,
        &crate::V4OrdinalIdentityManifest,
    )>,
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<
    (
        Vec<ConstructionIndexOutput>,
        super::V4OrdinalPublicationMetrics,
        V4OrdinalBuildMetrics,
    ),
    GfError,
> {
    let V4ConstructionArtifactBundle {
        mut manifest,
        mut metrics,
        mut publications,
    } = bundle;
    let mut local_names = v4_manifest_artifact_names(&manifest);
    let published = (|| {
        let (read_bytes, read_operations) = match parent {
            Some((selected, prior)) => merge_construction_v4_delta(
                encoded,
                selected,
                prior,
                &mut manifest,
                &mut metrics,
                &mut publications,
                &mut local_names,
                cancelled,
                allocation,
            )?,
            None => (0, 0),
        };
        if cancelled() {
            return Err(storage_err("construction ordinal publication cancelled"));
        }
        let (outputs, mut publication, metrics) = publish_v4_construction_artifacts_inner(
            encoded,
            &manifest,
            &metrics,
            &mut publications,
            generation,
            topology_delta_sha256,
            &local_names,
            allocation,
        )?;
        publication.read_bytes = read_bytes;
        publication.read_operations = read_operations;
        Ok((outputs, publication, metrics))
    })();
    match published {
        Ok(published) => {
            commit_v4_publications(publications, V4AuthorityTransactionProof)?;
            Ok(published)
        }
        Err(primary) => {
            let cleanup = cleanup_v4_publications(&mut publications);
            combine_v4_cleanup(Err(primary), cleanup, "v4 authority transaction cleanup")
        }
    }
}

/// Combine a streamed construction delta with selected parent descriptors. Only
/// binary-carry inputs are opened; an ordinary append never rereads the base.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn merge_construction_v4_delta(
    encoded: &graphforge_filesystem::StableDirectory,
    selected: &crate::ResolvedProjectGeneration,
    prior: &crate::V4OrdinalIdentityManifest,
    delta: &mut crate::V4OrdinalIdentityManifest,
    metrics: &mut V4OrdinalBuildMetrics,
    publications: &mut Vec<(String, V4PublicationGuard)>,
    local_names: &mut BTreeSet<String>,
    cancelled: &mut impl FnMut() -> bool,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(u64, u64), GfError> {
    if prior.topology_generation.checked_add(1) != Some(delta.topology_generation) {
        return Err(storage_err(
            "construction ordinal parent generation changed",
        ));
    }
    let prior_end = match prior.ordinal_ranges.last() {
        Some(range) => range
            .first_node_id
            .checked_add(range.count - 1)
            .ok_or_else(|| storage_err("ordinal parent range overflows"))?,
        None => 0,
    };
    if delta
        .ordinal_ranges
        .first()
        .is_some_and(|range| range.first_node_id <= prior_end)
    {
        return Err(storage_err(
            "construction ordinal delta reuses retained node IDs",
        ));
    }
    let index = encoded
        .open_child_directory(std::ffi::OsStr::new("graph"))
        .and_then(|graph| graph.open_child_directory(std::ffi::OsStr::new("topology")))
        .and_then(|topology| topology.open_child_directory(std::ffi::OsStr::new("uuid-membership")))
        .map_err(storage_err)?;
    let mut created = local_names
        .iter()
        .map(|name| (name.clone(), index.path().join(name)))
        .collect::<HashMap<_, _>>();
    let mut combined = prior.clone();
    combined.topology_generation = delta.topology_generation;
    combined
        .forward_identities
        .extend(delta.forward_identities.clone());
    combined.ordinal_ranges.extend(delta.ordinal_ranges.clone());
    combined.tombstones.extend(delta.tombstones.clone());
    let mut intervals = v4_forward_intervals(&combined.forward_identities)?;
    let mut first_consumed = None;
    while intervals.len() >= 3 {
        let right = intervals[intervals.len() - 1];
        let left = intervals[intervals.len() - 2];
        if right.1 - right.0 != left.1 - left.0 {
            break;
        }
        first_consumed = Some(left.0);
        intervals.pop();
        intervals.pop();
        intervals.push((left.0, right.1, left.2));
    }
    let lease = crate::graph_object_store::begin_graph_object_read(selected.container_root())?;
    let mut objects = Vec::new();
    let mut pinned = crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs {
        manifest: prior.clone(),
        artifacts: Vec::new(),
    };
    let mut read_bytes = 0_u64;
    let mut read_calls = 0_u64;
    for descriptor in prior
        .forward_identities
        .iter()
        .chain(prior.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(prior.tombstones.iter().map(|run| &run.artifact))
        .filter(|artifact| first_consumed.is_some_and(|first| artifact.generation >= first))
    {
        construction_ordinal_event("pin", descriptor.generation);
        let (object, io, cache) =
            lease.open_for_construction(&descriptor.sha256, descriptor.bytes, cancelled)?;
        read_bytes = read_bytes
            .checked_add(io.read_bytes)
            .ok_or_else(|| storage_err("ordinal authentication bytes overflow"))?;
        read_calls = read_calls
            .checked_add(io.read_calls)
            .ok_or_else(|| storage_err("ordinal authentication calls overflow"))?;
        merge_cache_release_evidence(&mut metrics.cache_release, cache);
        pinned
            .artifacts
            .push(crate::ordinal_identity_v4::PinnedV4OrdinalArtifact {
                descriptor: descriptor.clone(),
                file: object.try_clone_file().map_err(storage_err)?,
            });
        objects.push(object);
    }
    construction_ordinal_event("compaction", combined.topology_generation);
    let mut compaction = compact_v4_binary_carry_with_cancellation(
        &pinned,
        &index,
        index.path(),
        &mut combined,
        &mut created,
        cancelled,
        allocation,
    )?;
    metrics.artifact_bytes = metrics
        .artifact_bytes
        .checked_add(compaction.write_bytes)
        .ok_or_else(|| storage_err("ordinal writes overflow"))?;
    metrics.write_blocks = metrics
        .write_blocks
        .checked_add(compaction.write_blocks)
        .ok_or_else(|| storage_err("ordinal writes overflow"))?;
    metrics.fsync_operations = metrics
        .fsync_operations
        .checked_add(compaction.fsync_operations)
        .ok_or_else(|| storage_err("ordinal fsyncs overflow"))?;
    merge_cache_release_evidence(&mut metrics.cache_release, compaction.cache_release);
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(4 * V4_ORDINAL_BLOCK_BYTES);
    publications.append(&mut compaction.publications);
    let retained = v4_manifest_artifact_names(&combined);
    let mut current = Vec::new();
    for (name, mut publication) in publications.drain(..) {
        if retained.contains(&name) {
            current.push((name, publication));
        } else {
            cleanup_v4_publication(&mut publication)?;
        }
    }
    *publications = current;
    *local_names = created
        .into_keys()
        .filter(|name| retained.contains(name))
        .collect();
    admit_v4_construction_manifest(&combined)?;
    *delta = combined;
    Ok((
        read_bytes
            .checked_add(compaction.read_bytes)
            .ok_or_else(|| storage_err("ordinal read bytes overflow"))?,
        read_calls
            .checked_add(compaction.read_calls)
            .ok_or_else(|| storage_err("ordinal read calls overflow"))?,
    ))
}

fn cleanup_v4_publications(
    publications: &mut Vec<(String, V4PublicationGuard)>,
) -> Result<(), GfError> {
    let mut cleanup = Ok(());
    for (_, mut publication) in publications.drain(..).rev() {
        cleanup = combine_v4_cleanup(
            cleanup,
            cleanup_v4_publication(&mut publication),
            "v4 publication guard cleanup",
        );
    }
    cleanup
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn publish_v4_construction_artifacts_inner(
    encoded: &graphforge_filesystem::StableDirectory,
    manifest: &crate::V4OrdinalIdentityManifest,
    metrics: &V4OrdinalBuildMetrics,
    publications: &mut Vec<(String, V4PublicationGuard)>,
    generation: u64,
    topology_delta_sha256: &str,
    local_names: &BTreeSet<String>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<
    (
        Vec<ConstructionIndexOutput>,
        super::V4OrdinalPublicationMetrics,
        V4OrdinalBuildMetrics,
    ),
    GfError,
> {
    let graph = encoded
        .open_child_directory(std::ffi::OsStr::new("graph"))
        .map_err(storage_err)?;
    let topology = graph
        .open_child_directory(std::ffi::OsStr::new("topology"))
        .map_err(storage_err)?;
    let index = topology
        .open_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .map_err(storage_err)?;
    let manifest_body = serde_json::to_vec(manifest).map_err(storage_err)?;
    let receipt = TopologyIndexReceipt {
        nonce: Uuid::new_v4().simple().to_string(),
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256.to_owned(),
        manifest_sha256: hex_sha256(&manifest_body),
    };
    let receipt_body = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let mut work = ConstructionIndexWork::default();
    let mut outputs = manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .filter(|artifact| local_names.contains(&artifact.name))
        .map(|artifact| ConstructionIndexOutput {
            name: artifact.name.clone(),
            bytes: artifact.bytes,
            sha256: artifact.sha256.clone(),
        })
        .collect::<Vec<_>>();
    crate::graph_construction::construction_failpoint("v4_publish.after_artifacts");
    v4_authority_failure("after_artifacts")?;
    index.sync().map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    crate::graph_construction::construction_failpoint("v4_publish.after_artifacts_fsync");
    let artifact_bytes = outputs.iter().try_fold(0_u64, |total, output| {
        total
            .checked_add(output.bytes)
            .ok_or_else(|| storage_err("v4 publication artifact byte count overflow"))
    })?;
    // The selected project generation is still unpublished. Install the
    // receipt first, then its manifest, and create the lock last so every
    // visible construction inventory is complete and reopenable.
    let (receipt_output, receipt_publication) = install_construction_bytes(
        &index,
        V4_ORDINAL_RECEIPT,
        &receipt_body,
        &mut work,
        allocation,
    )?;
    outputs.push(receipt_output);
    publications.push((V4_ORDINAL_RECEIPT.to_owned(), receipt_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_receipt_install");
    let (manifest_output, manifest_publication) = install_construction_bytes(
        &index,
        V4_ORDINAL_MANIFEST,
        &manifest_body,
        &mut work,
        allocation,
    )?;
    outputs.push(manifest_output);
    publications.push((V4_ORDINAL_MANIFEST.to_owned(), manifest_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_manifest_install");
    let (lock_output, lock_publication) =
        install_construction_bytes(&index, "ordinal-v4.lock", &[], &mut work, allocation)?;
    outputs.push(lock_output);
    publications.push(("ordinal-v4.lock".to_owned(), lock_publication));
    crate::graph_construction::construction_failpoint("v4_publish.after_lock_install");
    index.sync().map_err(storage_err)?;
    topology.sync().map_err(storage_err)?;
    graph.sync().map_err(storage_err)?;
    encoded.sync().map_err(storage_err)?;
    v4_authority_failure("directory_sync")?;
    work.fsync_operations = work.fsync_operations.saturating_add(4);
    let publication_metrics = V4OrdinalPublicationMetrics {
        read_bytes: 0,
        read_operations: 0,
        write_bytes: work.write_bytes,
        write_operations: work.write_operations,
        fsync_operations: work.fsync_operations,
        peak_temporary_bytes: artifact_bytes
            .checked_add(work.write_bytes)
            .ok_or_else(|| storage_err("v4 publication temporary byte count overflow"))?,
    };
    Ok((outputs, publication_metrics, metrics.clone()))
}

pub(super) fn write_v4_tombstone_artifact(
    index: &graphforge_filesystem::StableDirectory,
    generation: u64,
    ids: &[u64],
) -> Result<(GuardedV4Tombstones, u64, u64), GfError> {
    let mut writer = V4TombstoneStreamWriter::new(index, generation)?;
    for &id in ids {
        writer.push(id)?;
    }
    writer.finish()
}

pub(super) struct GuardedV4Tombstones {
    pub(super) run: crate::V4OrdinalTombstones,
    pub(super) publication: Option<V4PublicationGuard>,
}

pub(super) struct V4TombstoneStreamWriter {
    generation: u64,
    pub(super) artifact: StreamingV4Artifact,
    blocks: Vec<crate::V4OrdinalTombstoneBlock>,
    block: Vec<u8>,
    offset: u64,
    previous: Option<u64>,
}

impl V4TombstoneStreamWriter {
    fn new(
        index: &graphforge_filesystem::StableDirectory,
        generation: u64,
    ) -> Result<Self, GfError> {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(1).map_err(storage_err)?;
        Self::new_with_cache_window(index, generation, cache_window, None)
    }

    pub(super) fn new_with_cache_window(
        index: &graphforge_filesystem::StableDirectory,
        generation: u64,
        cache_window: std::num::NonZeroU64,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        Ok(Self {
            generation,
            artifact: StreamingV4Artifact::create_with_window(
                index,
                "tombstones-delta",
                cache_window,
                allocation,
            )?,
            blocks: Vec::new(),
            block: Vec::with_capacity(V4_ORDINAL_BLOCK_BYTES),
            offset: 0,
            previous: None,
        })
    }

    pub(super) fn push(&mut self, id: u64) -> Result<(), GfError> {
        if id == 0 || self.previous.is_some_and(|previous| previous >= id) {
            return Err(storage_err(
                "v4 tombstone stream is not strictly increasing",
            ));
        }
        self.block.extend_from_slice(&id.to_be_bytes());
        self.previous = Some(id);
        if self.block.len() == V4_ORDINAL_BLOCK_BYTES {
            finish_v4_tombstone_block(
                &mut self.artifact,
                &mut self.blocks,
                &mut self.block,
                &mut self.offset,
            )?;
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<(GuardedV4Tombstones, u64, u64), GfError> {
        let (run, bytes, fsyncs, _) = self.finish_with_cache_evidence()?;
        Ok((run, bytes, fsyncs))
    }

    pub(super) fn finish_with_cache_evidence(
        mut self,
    ) -> Result<
        (
            GuardedV4Tombstones,
            u64,
            u64,
            graphforge_filesystem::FileCacheReleaseEvidence,
        ),
        GfError,
    > {
        let prepared = finish_v4_tombstone_block(
            &mut self.artifact,
            &mut self.blocks,
            &mut self.block,
            &mut self.offset,
        );
        if let Err(primary) = prepared {
            let (_, cleanup) = self.cleanup_unpublished();
            return combine_v4_cleanup(Err(primary), cleanup, "v4 tombstone failed output cleanup");
        }
        let byte_count = self.artifact.bytes;
        let mut metrics = V4OrdinalBuildMetrics::default();
        let artifact = finish_streamed_v4_artifact(
            self.artifact,
            "tombstones-v4",
            self.generation,
            crate::V4OrdinalArtifactKind::NodeTombstones,
            &mut metrics,
        )?;
        Ok((
            GuardedV4Tombstones {
                run: crate::V4OrdinalTombstones {
                    generation: self.generation,
                    artifact: artifact.artifact,
                    blocks: self.blocks,
                },
                publication: artifact.publication,
            },
            byte_count,
            metrics.fsync_operations,
            metrics.cache_release,
        ))
    }

    pub(super) fn cleanup_unpublished(
        mut self,
    ) -> (
        graphforge_filesystem::FileCacheReleaseEvidence,
        Result<(), GfError>,
    ) {
        let prepared = finish_v4_tombstone_block(
            &mut self.artifact,
            &mut self.blocks,
            &mut self.block,
            &mut self.offset,
        );
        let (cache_release, cleanup) = self.artifact.cleanup_unpublished();
        (
            cache_release,
            combine_v4_cleanup(prepared, cleanup, "v4 tombstone output cleanup"),
        )
    }
}

fn finish_v4_tombstone_block(
    writer: &mut StreamingV4Artifact,
    blocks: &mut Vec<crate::V4OrdinalTombstoneBlock>,
    bytes: &mut Vec<u8>,
    offset: &mut u64,
) -> Result<(), GfError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let first = u64::from_be_bytes(bytes[..8].try_into().expect("fixed tombstone"));
    let last = u64::from_be_bytes(
        bytes[bytes.len() - 8..]
            .try_into()
            .expect("fixed tombstone"),
    );
    blocks.push(crate::V4OrdinalTombstoneBlock {
        offset: *offset,
        count: u64::try_from(bytes.len() / 8).map_err(storage_err)?,
        first,
        last,
        sha256: hex_sha256(bytes),
    });
    writer.push(bytes)?;
    *offset = offset
        .checked_add(u64::try_from(bytes.len()).map_err(storage_err)?)
        .ok_or_else(|| storage_err("v4 tombstone offset overflow"))?;
    bytes.clear();
    Ok(())
}

pub(super) fn v4_manifest_artifact_names(
    manifest: &crate::V4OrdinalIdentityManifest,
) -> BTreeSet<String> {
    manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .map(|artifact| artifact.name.clone())
        .collect()
}

#[cfg(test)]
mod tests;
