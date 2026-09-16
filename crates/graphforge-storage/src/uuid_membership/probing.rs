//! Authenticated membership probes and retained construction snapshots.

use super::AuthenticatedRun;
use super::AuthenticatedUuidIndexSnapshot;
use super::BULK_IO_BYTES;
use super::BlockRecord;
use super::ConstructionIndexReference;
use super::ConstructionReferenceAuthentication;
use super::ConstructionReferenceAuthenticationWork;
use super::FORMAT_VERSION;
use super::FileRecord;
use super::IDENTITY_RECORD_BYTES;
#[cfg(test)]
use super::IDENTITY_RECORD_WIDTH;
use super::INDEX_DIR;
use super::MANIFEST;
use super::MAX_MANIFEST_BYTES;
use super::Manifest;
use super::NODE_LOOKUP_RECORD_BYTES;
#[cfg(test)]
use super::NODE_LOOKUP_RECORD_WIDTH;
use super::OpenRun;
#[cfg(test)]
use super::UuidConstructionSnapshotWork;
use super::UuidIndexKind;
use super::UuidMembershipIndex;
use super::UuidProbeMetrics;
use super::authenticate_file_blocks;
use super::batch_identity_states;
use super::block_matches;
use super::hex_bytes;
#[cfg(test)]
use super::identity_codec;
use super::open_uuid_child_file;
use super::open_verified;
use super::open_verified_at;
use super::record_length;
use super::retained_run_has_safe_links;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::topology_delta::read_bounded;
use super::validate_run_contents;
use super::validate_run_descriptors;
use super::validate_surrogate_pairs;
use graphforge_core::GfError;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(super) enum ProbeFileKind {
    Identity,
    Surrogate,
}

pub(super) fn authenticated_probe_block(
    file: &mut File,
    block: &BlockRecord,
    width: usize,
    kind: ProbeFileKind,
    metrics: &mut UuidProbeMetrics,
) -> Result<Vec<u8>, GfError> {
    file.seek(SeekFrom::Start(block.offset))
        .map_err(storage_err)?;
    metrics.file_seeks = metrics.file_seeks.saturating_add(1);
    let mut bytes = vec![0_u8; block.len as usize];
    file.read_exact(&mut bytes).map_err(storage_err)?;
    if !block_matches(&bytes, block, width) {
        return Err(storage_err("UUID probe block authentication failed"));
    }
    match kind {
        ProbeFileKind::Identity => {
            metrics.identity_blocks_read = metrics.identity_blocks_read.saturating_add(1);
            metrics.identity_bytes_read = metrics
                .identity_bytes_read
                .saturating_add(bytes.len() as u64);
        }
        ProbeFileKind::Surrogate => {
            metrics.surrogate_blocks_read = metrics.surrogate_blocks_read.saturating_add(1);
            metrics.surrogate_bytes_read = metrics
                .surrogate_bytes_read
                .saturating_add(bytes.len() as u64);
        }
    }
    Ok(bytes)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConstructionUuidIdentity {
    pub uuid: Uuid,
    pub kind: UuidIndexKind,
    pub surrogate: u64,
}

/// Retained authority for a UUID snapshot authenticated exactly once while its
/// live identities were emitted as one bounded sorted stream.
#[cfg(test)]
pub(crate) struct UuidConstructionSnapshot {
    root: graphforge_filesystem::StableDirectory,
    root_identity: graphforge_filesystem::FileIdentity,
    manifest_file: File,
    manifest_identity: graphforge_filesystem::FileIdentity,
    manifest_sha256: String,
    manifest_bytes: u64,
    manifest: Manifest,
    named_files: Vec<(String, graphforge_filesystem::FileIdentity)>,
    payload_consumed: bool,
}

#[cfg(test)]
impl UuidConstructionSnapshot {
    pub(crate) fn revalidate(&self) -> Result<(), GfError> {
        self.root.revalidate_named().map_err(storage_err)?;
        if self.root.identity() != self.root_identity
            || graphforge_filesystem::file_identity(&self.manifest_file).map_err(storage_err)?
                != self.manifest_identity
            || graphforge_filesystem::file_link_count(&self.manifest_file).map_err(storage_err)?
                != 1
        {
            return Err(storage_err("construction UUID snapshot authority changed"));
        }
        let mut manifest = self
            .root
            .open_child_file(std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        let body = read_bounded(&mut manifest, MAX_MANIFEST_BYTES)?;
        if graphforge_filesystem::file_identity(&manifest).map_err(storage_err)?
            != self.manifest_identity
            || hex_sha256(&body) != self.manifest_sha256
            || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
        {
            return Err(storage_err("construction UUID manifest changed"));
        }
        for (name, identity) in &self.named_files {
            let file = self
                .root
                .open_child_file(std::ffi::OsStr::new(name))
                .map_err(storage_err)?;
            if graphforge_filesystem::file_identity(&file).map_err(storage_err)? != *identity
                || graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1
            {
                return Err(storage_err("construction UUID run identity changed"));
            }
        }
        Ok(())
    }

    /// Authenticate each retained payload block exactly once and emit the live
    /// identity domain as a bounded UUID-ordered stream.
    pub(crate) fn stream_authenticated(
        &mut self,
        mut emit: impl FnMut(ConstructionUuidIdentity) -> Result<(), GfError>,
    ) -> Result<UuidConstructionSnapshotWork, GfError> {
        if self.payload_consumed {
            return Err(storage_err(
                "construction UUID payload was already consumed",
            ));
        }
        self.revalidate()?;
        let mut identity_cursors = Vec::with_capacity(self.manifest.runs.len());
        let mut work = UuidConstructionSnapshotWork {
            authentication_bytes: self.manifest_bytes,
            authentication_blocks: 1,
            ..Default::default()
        };
        for run in &self.manifest.runs {
            let identities = self
                .root
                .open_child_file(std::ffi::OsStr::new(&run.identities.name))
                .map_err(storage_err)?;
            identity_cursors.push(ConstructionRunCursor::new(
                identities,
                run.identities.clone(),
                IDENTITY_RECORD_WIDTH,
            ));
            let surrogates = self
                .root
                .open_child_file(std::ffi::OsStr::new(&run.node_surrogates.name))
                .map_err(storage_err)?;
            let mut cursor = ConstructionRunCursor::new(
                surrogates,
                run.node_surrogates.clone(),
                NODE_LOOKUP_RECORD_WIDTH,
            );
            while cursor.next_record()?.is_some() {}
            work.authentication_bytes = work.authentication_bytes.saturating_add(cursor.bytes);
            work.authentication_blocks = work.authentication_blocks.saturating_add(cursor.blocks);
        }
        let mut heads = identity_cursors
            .iter_mut()
            .map(ConstructionRunCursor::next_record)
            .collect::<Result<Vec<_>, _>>()?;
        loop {
            let Some(next_uuid) = heads
                .iter()
                .flatten()
                .map(|record| &record[..16])
                .min()
                .map(<[u8]>::to_vec)
            else {
                break;
            };
            let indexes = heads
                .iter()
                .enumerate()
                .filter_map(|(index, record)| {
                    record
                        .as_ref()
                        .is_some_and(|record| record[..16] == next_uuid)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            let selected = *indexes
                .iter()
                .max_by_key(|index| self.manifest.runs[**index].last_generation)
                .expect("one UUID head was selected");
            let record = heads[selected].as_ref().expect("selected head exists");
            let kind = record[16];
            if !matches!(kind, 0..=3) {
                return Err(storage_err(
                    "construction UUID identity record is malformed",
                ));
            }
            if matches!(kind, 0 | 1) {
                let identity = ConstructionUuidIdentity {
                    uuid: Uuid::from_bytes(record[..16].try_into().expect("fixed UUID width")),
                    kind: if kind == 0 {
                        UuidIndexKind::Node
                    } else {
                        UuidIndexKind::Edge
                    },
                    surrogate: u64::from_be_bytes(record[17..25].try_into().expect("fixed")),
                };
                if identity.kind == UuidIndexKind::Node {
                    if identity.surrogate == 0 {
                        return Err(storage_err("live node has zero surrogate"));
                    }
                    work.live_nodes = work.live_nodes.saturating_add(1);
                    work.max_node_surrogate = work.max_node_surrogate.max(identity.surrogate);
                } else {
                    work.live_edges = work.live_edges.saturating_add(1);
                }
                emit(identity)?;
            }
            for index in indexes {
                heads[index] = identity_cursors[index].next_record()?;
            }
        }
        for cursor in &identity_cursors {
            work.authentication_bytes = work.authentication_bytes.saturating_add(cursor.bytes);
            work.authentication_blocks = work.authentication_blocks.saturating_add(cursor.blocks);
        }
        if work.live_nodes != self.manifest.live_node_count
            || work.live_edges != self.manifest.live_edge_count
        {
            return Err(storage_err(
                "construction UUID live counts differ from manifest",
            ));
        }
        self.payload_consumed = true;
        self.revalidate()?;
        Ok(work)
    }
}

#[cfg(test)]
struct ConstructionRunCursor {
    file: File,
    descriptor: FileRecord,
    width: usize,
    block_index: usize,
    block: Vec<u8>,
    within: usize,
    records: u64,
    bytes: u64,
    blocks: u64,
    digest: Sha256,
    finished: bool,
}

#[cfg(test)]
impl ConstructionRunCursor {
    fn new(file: File, descriptor: FileRecord, width: usize) -> Self {
        Self {
            file,
            descriptor,
            width,
            block_index: 0,
            block: Vec::new(),
            within: 0,
            records: 0,
            bytes: 0,
            blocks: 0,
            digest: Sha256::new(),
            finished: false,
        }
    }

    fn next_record(&mut self) -> Result<Option<Vec<u8>>, GfError> {
        if self.finished {
            return Ok(None);
        }
        if self.within == self.block.len() {
            if self.block_index == self.descriptor.blocks.len() {
                self.finished = true;
                if self.records != self.descriptor.count
                    || self.bytes != record_length(&self.descriptor, self.width as u64)?
                    || hex_bytes(&self.digest.clone().finalize()) != self.descriptor.sha256
                {
                    return Err(storage_err("construction UUID run authentication failed"));
                }
                return Ok(None);
            }
            let descriptor = &self.descriptor.blocks[self.block_index];
            if descriptor.offset != self.bytes
                || descriptor.len as usize > BULK_IO_BYTES
                || descriptor.len == 0
            {
                return Err(storage_err("construction UUID block framing changed"));
            }
            self.block.resize(descriptor.len as usize, 0);
            self.file.read_exact(&mut self.block).map_err(storage_err)?;
            if !block_matches(&self.block, descriptor, self.width) {
                return Err(storage_err("construction UUID block digest changed"));
            }
            self.digest.update(&self.block);
            self.bytes = self.bytes.saturating_add(self.block.len() as u64);
            self.blocks = self.blocks.saturating_add(1);
            self.block_index += 1;
            self.within = 0;
        }
        let record = if self.width == IDENTITY_RECORD_WIDTH {
            let mut remaining = &self.block[self.within..];
            let record = identity_codec::take(&mut remaining)?
                .ok_or_else(|| storage_err("missing identity record"))?;
            self.within = self.block.len() - remaining.len();
            record.to_vec()
        } else {
            let end = self.within + self.width;
            let record = self.block[self.within..end].to_vec();
            self.within = end;
            record
        };
        self.records = self.records.saturating_add(1);
        Ok(Some(record))
    }
}

/// Authenticate each retained UUID byte once and emit the live identity set in
/// UUID order. The returned token revalidates inode/name authority without
/// rereading retained payload bytes.
#[cfg(test)]
pub(crate) fn open_uuid_construction_snapshot(
    project_dir: &Path,
    generation: u64,
    emit: impl FnMut(ConstructionUuidIdentity) -> Result<(), GfError>,
) -> Result<(UuidConstructionSnapshot, UuidConstructionSnapshotWork), GfError> {
    let mut token = super::pin_uuid_construction_snapshot(project_dir, generation)?;
    let work = token.stream_authenticated(emit)?;
    Ok((token, work))
}

/// Pin the generation's manifest and every run inode without reading retained
/// payload bytes.  The caller later consumes those bytes exactly once through
/// [`UuidConstructionSnapshot::stream_authenticated`].
#[cfg(test)]
pub(crate) fn pin_uuid_construction_snapshot(
    project_dir: &Path,
    generation: u64,
) -> Result<super::UuidConstructionSnapshot, GfError> {
    let root = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
    let root_identity = root.identity();
    let mut manifest_file = root
        .open_child_file(std::ffi::OsStr::new(MANIFEST))
        .map_err(storage_err)?;
    let manifest_identity =
        graphforge_filesystem::file_identity(&manifest_file).map_err(storage_err)?;
    let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
    let manifest_sha256 = hex_sha256(&body);
    let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
    if manifest.format_version != FORMAT_VERSION || manifest.current_generation != generation {
        return Err(storage_err(
            "construction UUID snapshot generation is stale",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let mut named_files = Vec::with_capacity(manifest.runs.len().saturating_mul(2));
    for run in &manifest.runs {
        let identities = root
            .open_child_file(std::ffi::OsStr::new(&run.identities.name))
            .map_err(storage_err)?;
        let identity = graphforge_filesystem::file_identity(&identities).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&identities).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "construction UUID identity run has extra links",
            ));
        }
        named_files.push((run.identities.name.clone(), identity));

        let surrogates = root
            .open_child_file(std::ffi::OsStr::new(&run.node_surrogates.name))
            .map_err(storage_err)?;
        let surrogate_identity =
            graphforge_filesystem::file_identity(&surrogates).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&surrogates).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "construction UUID surrogate run has extra links",
            ));
        }
        named_files.push((run.node_surrogates.name.clone(), surrogate_identity));
    }
    root.revalidate_named().map_err(storage_err)?;
    let token = UuidConstructionSnapshot {
        root,
        root_identity,
        manifest_file,
        manifest_identity,
        manifest_sha256,
        manifest_bytes: body.len() as u64,
        manifest,
        named_files,
        payload_consumed: false,
    };
    token.revalidate()?;
    Ok(token)
}

impl AuthenticatedUuidIndexSnapshot {
    pub(super) fn open_retained_file(&self, record: &FileRecord) -> Result<File, GfError> {
        let (held, expected) = self
            .runs
            .iter()
            .find_map(|run| {
                if run.descriptor.identities == *record {
                    Some((&run.identities, run.identities_identity))
                } else if run.descriptor.node_surrogates == *record {
                    Some((&run.node_surrogates, run.node_surrogates_identity))
                } else {
                    None
                }
            })
            .ok_or_else(|| storage_err("retained UUID descriptor is not authenticated"))?;
        let mut file = held.try_clone().map_err(storage_err)?;
        let identity_changed =
            graphforge_filesystem::file_identity(&file).map_err(storage_err)? != expected;
        let path_native_link_changed =
            self.cas_source_paths.is_none() && !retained_run_has_safe_links(&file)?;
        if identity_changed || path_native_link_changed {
            return Err(storage_err("retained UUID run identity changed"));
        }
        file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
        Ok(file)
    }

    pub(super) fn retained_reference(
        &self,
        record: &FileRecord,
    ) -> Result<ConstructionIndexReference, GfError> {
        let file = self.open_retained_file(record)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        let source_path = self
            .cas_source_paths
            .as_ref()
            .and_then(|paths| paths.get(&record.name))
            .map_or_else(
                || format!("{INDEX_DIR}/{}", record.name),
                |(path, _, _)| path.clone(),
            );
        Ok(ConstructionIndexReference {
            source_root: self.graph_root_path.to_string_lossy().into_owned(),
            source_root_volume: self.graph_root_identity.volume_serial,
            source_root_file_id: hex_bytes(&self.graph_root_identity.file_id),
            source_path,
            source_volume: identity.volume_serial,
            source_file_id: hex_bytes(&identity.file_id),
            target_path: format!("{INDEX_DIR}/{}", record.name),
            bytes: record_length(
                record,
                if record.name.starts_with("identities-") {
                    IDENTITY_RECORD_BYTES
                } else {
                    NODE_LOOKUP_RECORD_BYTES
                },
            )?,
            sha256: record.sha256.clone(),
            parent_manifest_sha256: self.manifest_sha256.clone(),
        })
    }

    pub(crate) fn authenticate_construction_references(
        &self,
        references: &[ConstructionReferenceAuthentication<'_>],
    ) -> Result<ConstructionReferenceAuthenticationWork, GfError> {
        self.authenticate_construction_references_with(references, || {})
    }

    fn authenticate_construction_references_with(
        &self,
        references: &[ConstructionReferenceAuthentication<'_>],
        before_final_revalidation: impl FnOnce(),
    ) -> Result<ConstructionReferenceAuthenticationWork, GfError> {
        self.revalidate()?;
        let mut referenced_payload_bytes = 0_u64;
        for reference in references {
            referenced_payload_bytes = referenced_payload_bytes
                .saturating_add(self.authenticate_construction_reference_once(reference)?);
        }
        before_final_revalidation();
        self.revalidate()?;
        Ok(ConstructionReferenceAuthenticationWork {
            global_revalidation_bytes: self.snapshot_authentication_bytes().saturating_mul(2),
            referenced_payload_bytes,
        })
    }

    fn snapshot_authentication_bytes(&self) -> u64 {
        let manifest_bytes = self.manifest_bytes;
        self.manifest
            .runs
            .iter()
            .fold(manifest_bytes, |total, run| {
                total
                    .saturating_add(
                        run.identities
                            .blocks
                            .iter()
                            .map(|block| u64::from(block.len))
                            .sum::<u64>(),
                    )
                    .saturating_add(
                        run.node_surrogates
                            .count
                            .saturating_mul(NODE_LOOKUP_RECORD_BYTES),
                    )
            })
    }

    fn authenticate_construction_reference_once(
        &self,
        reference: &ConstructionReferenceAuthentication<'_>,
    ) -> Result<u64, GfError> {
        if reference.source_root != self.graph_root_path.to_string_lossy()
            || reference.source_root_volume != self.graph_root_identity.volume_serial
            || reference.source_root_file_id != hex_bytes(&self.graph_root_identity.file_id)
            || reference.parent_manifest_sha256 != self.manifest_sha256
            || !reference.target_path.starts_with(&format!("{INDEX_DIR}/"))
        {
            return Err(storage_err(
                "retained construction reference authority changed",
            ));
        }
        let name = reference
            .target_path
            .strip_prefix(&format!("{INDEX_DIR}/"))
            .ok_or_else(|| storage_err("retained construction target path is invalid"))?;
        let expected_source = self
            .cas_source_paths
            .as_ref()
            .and_then(|paths| paths.get(name))
            .map_or(reference.target_path, |(path, _, _)| path.as_str());
        if reference.source_path != expected_source {
            return Err(storage_err("retained construction source path changed"));
        }
        let record = self
            .manifest
            .runs
            .iter()
            .flat_map(|run| [&run.identities, &run.node_surrogates])
            .find(|record| record.name == name)
            .ok_or_else(|| storage_err("retained construction run is absent"))?;
        let mut file = self.open_retained_file(record)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        let expected_bytes = record_length(
            record,
            if record.name.starts_with("identities-") {
                IDENTITY_RECORD_BYTES
            } else {
                NODE_LOOKUP_RECORD_BYTES
            },
        )?;
        file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
        let mut digest = Sha256::new();
        let mut actual_bytes = 0_u64;
        let mut block = vec![0_u8; BULK_IO_BYTES];
        loop {
            let count = file.read(&mut block).map_err(storage_err)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            actual_bytes = actual_bytes.saturating_add(count as u64);
        }
        if identity.volume_serial != reference.source_volume
            || hex_bytes(&identity.file_id) != reference.source_file_id
            || reference.bytes != expected_bytes
            || actual_bytes != expected_bytes
            || reference.sha256 != record.sha256
            || hex_bytes(&digest.finalize()) != record.sha256
        {
            return Err(storage_err(format!(
                "retained construction reference changed: volume={} expected_volume={} file_id={} expected_file_id={} bytes={} expected_bytes={} reference_sha={} manifest_sha={}",
                identity.volume_serial,
                reference.source_volume,
                hex_bytes(&identity.file_id),
                reference.source_file_id,
                actual_bytes,
                expected_bytes,
                reference.sha256,
                record.sha256
            )));
        }
        Ok(actual_bytes)
    }

    pub(crate) fn open_at_generation(project_dir: &Path, generation: u64) -> Result<Self, GfError> {
        let graph_root =
            graphforge_filesystem::StableDirectory::open(project_dir).map_err(storage_err)?;
        let graph_root_identity = graph_root.identity();
        let root_path = project_dir.join(INDEX_DIR);
        let root = graphforge_filesystem::StableDirectory::open(&root_path).map_err(storage_err)?;
        let root_identity = root.identity();
        let mut manifest_file = open_uuid_child_file(&root, std::ffi::OsStr::new(MANIFEST))?;
        let manifest_identity =
            graphforge_filesystem::file_identity(&manifest_file).map_err(storage_err)?;
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        let manifest_sha256 = hex_sha256(&body);
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION || manifest.current_generation != generation {
            return Err(storage_err("authenticated snapshot generation is stale"));
        }
        validate_run_descriptors(&manifest)?;
        let mut authenticated_bytes = body.len() as u64;
        let mut authenticated_blocks = 1_u64;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        for descriptor in &manifest.runs {
            let identities =
                open_verified_at(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_verified_at(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            authenticated_bytes = authenticated_bytes
                .saturating_add(record_length(
                    &descriptor.identities,
                    IDENTITY_RECORD_BYTES,
                )?)
                .saturating_add(descriptor.node_surrogates.count * NODE_LOOKUP_RECORD_BYTES);
            authenticated_blocks = authenticated_blocks
                .saturating_add(descriptor.identities.blocks.len() as u64)
                .saturating_add(descriptor.node_surrogates.blocks.len() as u64);
            runs.push(AuthenticatedRun {
                identities_identity: graphforge_filesystem::file_identity(&identities)
                    .map_err(storage_err)?,
                node_surrogates_identity: graphforge_filesystem::file_identity(&node_surrogates)
                    .map_err(storage_err)?,
                identities,
                node_surrogates,
                descriptor: descriptor.clone(),
            });
        }
        Ok(Self {
            graph_root,
            graph_root_path: project_dir.to_path_buf(),
            graph_root_identity,
            root,
            root_identity,
            manifest_bytes: body.len() as u64,
            manifest_file: Some(manifest_file),
            manifest_identity,
            manifest_sha256,
            manifest,
            runs,
            authenticated_bytes,
            authenticated_blocks,
            cas_source_paths: None,
            _cas_leases: Vec::new(),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one descriptor-lifetime authentication pass keeps every CAS file and manifest binding in scope"
    )]
    pub(crate) fn open_from_compact_inventory(
        container_root: &Path,
        inventory: &crate::GraphFilesInventory,
        generation: u64,
    ) -> Result<Self, GfError> {
        let graph_root =
            graphforge_filesystem::StableDirectory::open(container_root).map_err(storage_err)?;
        let graph_root_identity = graph_root.identity();
        let root =
            graphforge_filesystem::StableDirectory::open(container_root).map_err(storage_err)?;
        let root_identity = root.identity();
        let manifest_path = format!("{INDEX_DIR}/{MANIFEST}");
        let manifest_entry = inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == manifest_path)
            .ok_or_else(|| storage_err("compact UUID manifest is absent"))?;
        let mut manifest_lease = crate::graph_object_store::open_graph_object_by_digest(
            container_root,
            &manifest_entry.content_sha256,
            manifest_entry.byte_length,
        )?;
        let manifest_identity =
            graphforge_filesystem::file_identity(manifest_lease.as_ref()).map_err(storage_err)?;
        let body = read_bounded(&mut manifest_lease, MAX_MANIFEST_BYTES)?;
        let manifest_file = manifest_lease.try_clone_file().map_err(storage_err)?;
        let manifest_sha256 = hex_sha256(&body);
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION || manifest.current_generation != generation {
            return Err(storage_err(
                "authenticated compact snapshot generation is stale",
            ));
        }
        validate_run_descriptors(&manifest)?;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        let mut paths = BTreeMap::new();
        let manifest_physical =
            crate::graph_object_path(container_root, &manifest_entry.content_sha256)?;
        paths.insert(
            MANIFEST.to_owned(),
            (
                manifest_physical
                    .strip_prefix(container_root)
                    .map_err(storage_err)?
                    .to_string_lossy()
                    .into_owned(),
                manifest_entry.content_sha256.clone(),
                manifest_entry.byte_length,
            ),
        );
        let mut cas_leases = vec![manifest_lease];
        let mut authenticated_bytes = body.len() as u64;
        let mut authenticated_blocks = 1_u64;
        for descriptor in &manifest.runs {
            let mut open_record = |record: &FileRecord, width: u64| -> Result<File, GfError> {
                let logical = format!("{INDEX_DIR}/{}", record.name);
                let entry = inventory
                    .files
                    .iter()
                    .find(|entry| entry.relative_path == logical)
                    .ok_or_else(|| storage_err("compact UUID run is absent"))?;
                if entry.content_sha256 != record.sha256
                    || entry.byte_length != record_length(record, width)?
                {
                    return Err(storage_err("compact UUID run authority changed"));
                }
                let lease = crate::graph_object_store::open_graph_object_by_digest(
                    container_root,
                    &entry.content_sha256,
                    entry.byte_length,
                )?;
                let physical = crate::graph_object_path(container_root, &entry.content_sha256)?;
                let relative = physical
                    .strip_prefix(container_root)
                    .map_err(storage_err)?
                    .to_string_lossy()
                    .into_owned();
                paths.insert(
                    record.name.clone(),
                    (relative, entry.content_sha256.clone(), entry.byte_length),
                );
                let file = lease.try_clone_file().map_err(storage_err)?;
                cas_leases.push(lease);
                Ok(file)
            };
            let identities = open_record(&descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_record(&descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            authenticate_file_blocks(
                &mut identities.try_clone().map_err(storage_err)?,
                &descriptor.identities,
                IDENTITY_RECORD_BYTES,
                None,
            )?;
            authenticate_file_blocks(
                &mut node_surrogates.try_clone().map_err(storage_err)?,
                &descriptor.node_surrogates,
                NODE_LOOKUP_RECORD_BYTES,
                None,
            )?;
            authenticated_bytes = authenticated_bytes
                .saturating_add(record_length(
                    &descriptor.identities,
                    IDENTITY_RECORD_BYTES,
                )?)
                .saturating_add(descriptor.node_surrogates.count * NODE_LOOKUP_RECORD_BYTES);
            authenticated_blocks = authenticated_blocks
                .saturating_add(descriptor.identities.blocks.len() as u64)
                .saturating_add(descriptor.node_surrogates.blocks.len() as u64);
            runs.push(AuthenticatedRun {
                identities_identity: graphforge_filesystem::file_identity(&identities)
                    .map_err(storage_err)?,
                node_surrogates_identity: graphforge_filesystem::file_identity(&node_surrogates)
                    .map_err(storage_err)?,
                identities,
                node_surrogates,
                descriptor: descriptor.clone(),
            });
        }
        Ok(Self {
            graph_root,
            graph_root_path: container_root.to_path_buf(),
            graph_root_identity,
            root,
            root_identity,
            manifest_bytes: body.len() as u64,
            manifest_file: Some(manifest_file),
            manifest_identity,
            manifest_sha256,
            manifest,
            runs,
            authenticated_bytes,
            authenticated_blocks,
            cas_source_paths: Some(paths),
            _cas_leases: cas_leases,
        })
    }

    pub(crate) fn topology_generation(&self) -> u64 {
        self.manifest.current_generation
    }

    pub(crate) fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.manifest.live_node_count,
            UuidIndexKind::Edge => self.manifest.live_edge_count,
        }
    }

    pub(crate) fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }
    pub(crate) fn take_authentication_work(&mut self) -> (u64, u64) {
        (
            std::mem::take(&mut self.authenticated_bytes),
            std::mem::take(&mut self.authenticated_blocks),
        )
    }

    pub(crate) fn revalidate(&self) -> Result<(), GfError> {
        self.graph_root.revalidate_named().map_err(storage_err)?;
        if self.graph_root.identity() != self.graph_root_identity {
            return Err(storage_err("UUID graph root identity changed"));
        }
        if let Some(objects) = &self.cas_source_paths {
            let (_, manifest_digest, manifest_length) = objects
                .get(MANIFEST)
                .ok_or_else(|| storage_err("compact UUID manifest authority is absent"))?;
            let mut manifest_lease = crate::graph_object_store::open_graph_object_by_digest(
                &self.graph_root_path,
                manifest_digest,
                *manifest_length,
            )?;
            if graphforge_filesystem::file_identity(manifest_lease.as_ref()).map_err(storage_err)?
                != self.manifest_identity
            {
                return Err(storage_err("compact UUID manifest identity changed"));
            }
            let body = read_bounded(&mut manifest_lease, MAX_MANIFEST_BYTES)?;
            if hex_sha256(&body) != self.manifest_sha256
                || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
            {
                return Err(storage_err("compact UUID manifest authentication changed"));
            }
            for run in &self.runs {
                for (record, identity) in [
                    (&run.descriptor.identities, run.identities_identity),
                    (
                        &run.descriptor.node_surrogates,
                        run.node_surrogates_identity,
                    ),
                ] {
                    let (_, digest, length) = objects
                        .get(&record.name)
                        .ok_or_else(|| storage_err("compact UUID run authority is absent"))?;
                    let file = crate::graph_object_store::open_graph_object_by_digest(
                        &self.graph_root_path,
                        digest,
                        *length,
                    )?;
                    if graphforge_filesystem::file_identity(file.as_ref()).map_err(storage_err)?
                        != identity
                    {
                        return Err(storage_err("compact UUID run identity changed"));
                    }
                }
            }
            return Ok(());
        }
        self.root.revalidate_named().map_err(storage_err)?;
        if self.root.identity() != self.root_identity {
            return Err(storage_err("UUID index root identity changed"));
        }
        let manifest_file = self
            .manifest_file
            .as_ref()
            .ok_or_else(|| storage_err("UUID manifest descriptor is suspended for publication"))?;
        if graphforge_filesystem::file_identity(manifest_file).map_err(storage_err)?
            != self.manifest_identity
            || graphforge_filesystem::file_link_count(manifest_file).map_err(storage_err)? != 1
            || self.manifest_sha256.len() != 64
        {
            return Err(storage_err("retained UUID manifest identity changed"));
        }
        let mut named_manifest = open_uuid_child_file(&self.root, std::ffi::OsStr::new(MANIFEST))?;
        if graphforge_filesystem::file_identity(&named_manifest).map_err(storage_err)?
            != self.manifest_identity
            || graphforge_filesystem::file_link_count(&named_manifest).map_err(storage_err)? != 1
        {
            return Err(storage_err("UUID manifest identity changed"));
        }
        let body = read_bounded(&mut named_manifest, MAX_MANIFEST_BYTES)?;
        if hex_sha256(&body) != self.manifest_sha256
            || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
        {
            return Err(storage_err("UUID manifest authentication changed"));
        }
        for run in &self.runs {
            for (record, identity) in [
                (&run.descriptor.identities, run.identities_identity),
                (
                    &run.descriptor.node_surrogates,
                    run.node_surrogates_identity,
                ),
            ] {
                let named = open_uuid_child_file(&self.root, std::ffi::OsStr::new(&record.name))?;
                if graphforge_filesystem::file_identity(&named).map_err(storage_err)? != identity
                    || !retained_run_has_safe_links(&named)?
                {
                    return Err(storage_err("UUID retained run identity changed"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn suspend_owned_manifest(&mut self) {
        if self.cas_source_paths.is_none() {
            self.manifest_file.take();
        }
    }

    pub(super) fn restore_owned_manifest(&mut self) -> Result<(), GfError> {
        if self.manifest_file.is_some() {
            return Ok(());
        }
        self.root.revalidate_named().map_err(storage_err)?;
        let mut file = open_uuid_child_file(&self.root, std::ffi::OsStr::new(MANIFEST))?;
        if graphforge_filesystem::file_identity(&file).map_err(storage_err)?
            != self.manifest_identity
            || graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1
        {
            return Err(storage_err("suspended UUID manifest identity changed"));
        }
        let body = read_bounded(&mut file, MAX_MANIFEST_BYTES)?;
        if hex_sha256(&body) != self.manifest_sha256
            || serde_json::from_slice::<Manifest>(&body).map_err(storage_err)? != self.manifest
        {
            return Err(storage_err(
                "suspended UUID manifest authentication changed",
            ));
        }
        self.authenticated_bytes = self
            .authenticated_bytes
            .checked_add(body.len() as u64)
            .ok_or_else(|| storage_err("UUID restoration byte count overflow"))?;
        self.authenticated_blocks = self
            .authenticated_blocks
            .checked_add(1)
            .ok_or_else(|| storage_err("UUID restoration block count overflow"))?;
        self.manifest_file = Some(file);
        Ok(())
    }

    pub(super) fn advance_to(&mut self, manifest: Manifest) -> Result<u64, GfError> {
        self.root.revalidate_named().map_err(storage_err)?;
        let mut manifest_file = open_uuid_child_file(&self.root, std::ffi::OsStr::new(MANIFEST))?;
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        if hex_sha256(&body) != hex_sha256(&serde_json::to_vec(&manifest).map_err(storage_err)?) {
            return Err(storage_err("committed UUID manifest differs from plan"));
        }
        let mut next_runs = Vec::with_capacity(manifest.runs.len());
        let mut authenticated_bytes = 0_u64;
        for descriptor in &manifest.runs {
            if let Some(retained) = self.runs.iter().find(|run| run.descriptor == *descriptor) {
                next_runs.push(AuthenticatedRun {
                    identities: retained.identities.try_clone().map_err(storage_err)?,
                    identities_identity: retained.identities_identity,
                    node_surrogates: retained.node_surrogates.try_clone().map_err(storage_err)?,
                    node_surrogates_identity: retained.node_surrogates_identity,
                    descriptor: descriptor.clone(),
                });
            } else {
                let identities =
                    open_verified_at(&self.root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
                let node_surrogates = open_verified_at(
                    &self.root,
                    &descriptor.node_surrogates,
                    NODE_LOOKUP_RECORD_BYTES,
                )?;
                authenticated_bytes = authenticated_bytes
                    .saturating_add(record_length(
                        &descriptor.identities,
                        IDENTITY_RECORD_BYTES,
                    )?)
                    .saturating_add(descriptor.node_surrogates.count * NODE_LOOKUP_RECORD_BYTES);
                next_runs.push(AuthenticatedRun {
                    identities_identity: graphforge_filesystem::file_identity(&identities)
                        .map_err(storage_err)?,
                    node_surrogates_identity: graphforge_filesystem::file_identity(
                        &node_surrogates,
                    )
                    .map_err(storage_err)?,
                    identities,
                    node_surrogates,
                    descriptor: descriptor.clone(),
                });
            }
        }
        self.manifest_identity =
            graphforge_filesystem::file_identity(&manifest_file).map_err(storage_err)?;
        self.manifest_sha256 = hex_sha256(&body);
        self.manifest_bytes = body.len() as u64;
        self.manifest_file = Some(manifest_file);
        self.manifest = manifest;
        self.runs = next_runs;
        self.authenticated_bytes = 0;
        self.authenticated_blocks = 0;
        Ok(authenticated_bytes)
    }

    pub(crate) fn probe(
        &mut self,
        kind: UuidIndexKind,
        requested: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                kind,
                &unresolved,
                &mut metrics,
            )?;
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                metrics.found = metrics.found.saturating_add(u64::from(state.present));
                resolved.insert(uuid, state.present);
            }
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().unwrap_or(false))
                .collect(),
            metrics,
        ))
    }

    pub(crate) fn lookup_node_surrogates(
        &mut self,
        requested: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                UuidIndexKind::Node,
                &unresolved,
                &mut metrics,
            )?;
            let mut pairs = Vec::new();
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                let value = state.present.then_some(state.surrogate);
                if let Some(surrogate) = value {
                    pairs.push((surrogate, uuid));
                    metrics.found = metrics.found.saturating_add(1);
                }
                resolved.insert(uuid, value);
            }
            validate_surrogate_pairs(
                &mut run.node_surrogates,
                &run.descriptor.node_surrogates,
                &pairs,
                &mut metrics,
            )?;
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().flatten())
                .collect(),
            metrics,
        ))
    }
}

impl UuidMembershipIndex {
    /// Open and fully authenticate the current immutable index snapshot.
    pub fn open(project_dir: &Path) -> Result<Self, GfError> {
        let generation = crate::read_topology_generation(project_dir)?;
        Self::open_at_generation(project_dir, generation)
    }

    pub(crate) fn open_at_generation(project_dir: &Path, generation: u64) -> Result<Self, GfError> {
        let root = project_dir.join(INDEX_DIR);
        let body = fs::read(root.join(MANIFEST)).map_err(storage_err)?;
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(storage_err(format!(
                "unsupported format version {}",
                manifest.format_version
            )));
        }
        if manifest.current_generation != generation {
            return Err(storage_err(format!(
                "stale index generation {} (graph generation {generation})",
                manifest.current_generation
            )));
        }
        validate_run_descriptors(&manifest)?;
        let mut runs = Vec::with_capacity(manifest.runs.len());
        for descriptor in &manifest.runs {
            let identities = open_verified(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
            let node_surrogates =
                open_verified(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
            validate_run_contents(
                identities.try_clone().map_err(storage_err)?,
                node_surrogates.try_clone().map_err(storage_err)?,
                descriptor,
            )?;
            runs.push(OpenRun {
                identities,
                node_surrogates,
                descriptor: descriptor.clone(),
            });
        }
        Ok(Self { runs, manifest })
    }

    /// Topology generation authenticated by this open handle.
    #[must_use]
    pub const fn topology_generation(&self) -> u64 {
        self.manifest.current_generation
    }

    #[must_use]
    /// Return the authenticated unique-record count for one identity domain.
    pub fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.manifest.live_node_count,
            UuidIndexKind::Edge => self.manifest.live_edge_count,
        }
    }

    /// Probe a batch in caller order. Memory is O(unique requested UUIDs).
    pub fn probe(
        &mut self,
        kind: UuidIndexKind,
        requested: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut unresolved = unique;
        let mut membership = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                kind,
                &unresolved,
                &mut metrics,
            )?;
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                metrics.found = metrics.found.saturating_add(u64::from(state.present));
                membership.insert(uuid, state.present);
            }
        }
        Ok((
            requested
                .iter()
                .map(|uuid| membership.get(uuid).copied().unwrap_or(false))
                .collect(),
            metrics,
        ))
    }

    /// Resolve node UUIDs to their canonical surrogates without scanning
    /// topology. Results retain caller order; an absent UUID returns `None`.
    pub fn lookup_node_surrogates(
        &mut self,
        requested: &[Uuid],
    ) -> Result<(Vec<Option<u64>>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let mut unresolved = unique;
        let mut resolved = std::collections::BTreeMap::new();
        for run in self.runs.iter_mut().rev() {
            if unresolved.is_empty() {
                break;
            }
            metrics.runs_considered = metrics.runs_considered.saturating_add(1);
            let states = batch_identity_states(
                &mut run.identities,
                &run.descriptor.identities,
                UuidIndexKind::Node,
                &unresolved,
                &mut metrics,
            )?;
            let mut pairs = Vec::new();
            for (uuid, state) in states {
                unresolved.remove(&uuid);
                let value = state.present.then_some(state.surrogate);
                if let Some(surrogate) = value {
                    pairs.push((surrogate, uuid));
                    metrics.found = metrics.found.saturating_add(1);
                }
                resolved.insert(uuid, value);
            }
            validate_surrogate_pairs(
                &mut run.node_surrogates,
                &run.descriptor.node_surrogates,
                &pairs,
                &mut metrics,
            )?;
        }
        Ok((
            requested
                .iter()
                .map(|uuid| resolved.get(uuid).copied().flatten())
                .collect(),
            metrics,
        ))
    }
}

#[cfg(test)]
mod tests;
