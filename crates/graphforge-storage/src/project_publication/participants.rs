//! Participants for atomic project publication.

use super::{
    ATTEMPTS_DIR, Digest, File, GENERATIONS_DIR, GfError, MAX_GRAPH_MANIFEST_SEGMENT_BYTES,
    OpenOptions, Ordering, PARTICIPANTS_DIR, ParticipantPayloads, Path, PathBuf, ProjectCapability,
    ProjectErrorCode, ProjectGenerationRequest, ProjectParticipantEncoding, Read,
    ResolvedProjectGeneration, Serialize, Sha256, StagedParticipant, Write, canonical_line,
    ensure_machine_directory, hex_digest, project_error, project_failpoint, publication_io,
    sync_directory, transaction_conflict,
};

pub(super) fn prepare_generation_directory(
    root: &Path,
    request: &ProjectGenerationRequest,
    request_fingerprint: &str,
    requires_promotion: bool,
) -> Result<PathBuf, GfError> {
    let relative_root = if requires_promotion {
        Path::new(ATTEMPTS_DIR)
            .join(request.transaction_uuid.hyphenated().to_string())
            .join(request_fingerprint)
    } else {
        Path::new(GENERATIONS_DIR).join(request.generation_uuid.hyphenated().to_string())
    };
    let generation_root = root.join(&relative_root);
    if generation_root.exists() {
        return Err(transaction_conflict(request));
    }
    ensure_machine_directory(root, &relative_root.join(PARTICIPANTS_DIR))?;
    Ok(generation_root)
}

pub(super) fn stage_optional_graph_tree(
    participants: &[StagedParticipant],
    parent: &ResolvedProjectGeneration,
    generation_root: &Path,
    graph_tree: Option<&Path>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let files_participant = participants.iter().find(|participant| {
        participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == crate::GRAPH_FILES_FAMILY
    });
    let snapshot_participant = participants.iter().find(|participant| {
        participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == "snapshot"
    });
    if files_participant.is_some() && snapshot_participant.is_some() {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "graph generation cannot declare both snapshot and files participants",
        ));
    }
    let Some(files_participant) = files_participant else {
        if graph_tree.is_some() {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "graph_tree source requires a graph/files inventory participant",
            ));
        }
        return Ok(());
    };
    if files_participant.capability_version != crate::GRAPH_CAPABILITY_VERSION
        || !matches!(
            files_participant.record_version,
            crate::GRAPH_FILES_RECORD_VERSION
                | crate::GRAPH_FILES_V2_RECORD_VERSION
                | crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
                | crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        )
        || files_participant.encoding != ProjectParticipantEncoding::Json.extension()
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "unsupported graph files participant contract",
        ));
    }
    let inventory_path = generation_root
        .join(PARTICIPANTS_DIR)
        .join(&files_participant.relative_path);
    let participant = crate::decode_versioned_graph_files_participant(
        files_participant.record_version,
        &std::fs::read(inventory_path).map_err(publication_io)?,
    )?;
    let inventory = match participant {
        crate::GraphFilesParticipant::V1(inventory) => inventory,
        crate::GraphFilesParticipant::V2(root) => {
            if graph_tree.is_some() {
                return Err(project_error(
                    ProjectErrorCode::PublicationFailed,
                    "graph/files v2 root must reference project objects, not a generation graph tree",
                ));
            }
            verify_compact_graph_root(parent.container_root(), &root)?;
            sync_directory(generation_root)?;
            return Ok(());
        }
    };
    let parent_tree = parent.graph_tree_root();
    let source = match graph_tree {
        Some(path) => path,
        None if parent_tree.exists() => {
            crate::verify_graph_tree(&parent_tree, &inventory)?;
            parent_tree.as_path()
        }
        None => {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "graph/files participant requires a graph_tree source directory",
            ));
        }
    };
    crate::graph_files::stage_graph_tree_with_allocation(
        source,
        generation_root,
        &inventory,
        allocation,
    )?;
    sync_directory(generation_root)?;
    Ok(())
}

pub(super) fn verify_optional_generation_graph_tree(
    generation_root: &Path,
    participants: &[StagedParticipant],
) -> Result<(), GfError> {
    let Some(files) = participants.iter().find(|participant| {
        participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == crate::GRAPH_FILES_FAMILY
    }) else {
        return Ok(());
    };
    let path = generation_root
        .join(PARTICIPANTS_DIR)
        .join(&files.relative_path);
    let bytes = std::fs::read(&path).map_err(publication_io)?;
    match crate::decode_versioned_graph_files_participant(files.record_version, &bytes)? {
        crate::GraphFilesParticipant::V1(inventory) => {
            crate::verify_graph_tree(&crate::graph_tree_root(generation_root), &inventory)
        }
        // Compact payloads live outside this generation. Staging verifies the
        // complete named root once, and publication repeats that verification
        // while holding the CAS lease immediately before CURRENT. Intermediate
        // generation validation must not re-read every immutable payload byte.
        crate::GraphFilesParticipant::V2(_) => Ok(()),
    }
}

pub(super) fn verify_optional_graph_tree_with_lease(
    generation_root: &Path,
    participants: &[StagedParticipant],
    lease: &crate::GraphObjectPublicationLease,
) -> Result<(), GfError> {
    let Some(files) = participants.iter().find(|participant| {
        participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == crate::GRAPH_FILES_FAMILY
    }) else {
        return Ok(());
    };
    let path = generation_root
        .join(PARTICIPANTS_DIR)
        .join(&files.relative_path);
    let bytes = std::fs::read(&path).map_err(publication_io)?;
    match crate::decode_versioned_graph_files_participant(files.record_version, &bytes)? {
        crate::GraphFilesParticipant::V1(inventory) => {
            crate::verify_graph_tree(&crate::graph_tree_root(generation_root), &inventory)
        }
        crate::GraphFilesParticipant::V2(root) => {
            verify_compact_graph_root_with_lease(lease, &root)
        }
    }
}

fn verify_compact_graph_root(
    container_root: &Path,
    root: &crate::GraphFilesRootV2,
) -> Result<(), GfError> {
    let (files, _) =
        crate::resolve_graph_manifest(root, crate::GraphManifestLimits::default(), |digest| {
            crate::read_graph_object_by_digest(
                container_root,
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
            )
        })?;
    crate::route_component::authenticate_manifest_routes(root.format_version, &files, |entry| {
        crate::read_graph_object_by_digest(
            container_root,
            &entry.content_sha256,
            MAX_GRAPH_MANIFEST_SEGMENT_BYTES,
        )
    })?;
    for entry in files {
        crate::verify_graph_object(container_root, &entry.content_sha256, entry.byte_length)?;
    }
    Ok(())
}

fn verify_compact_graph_root_with_lease(
    lease: &crate::GraphObjectPublicationLease,
    root: &crate::GraphFilesRootV2,
) -> Result<(), GfError> {
    let (files, _) =
        crate::resolve_graph_manifest(root, crate::GraphManifestLimits::default(), |digest| {
            crate::graph_object_store::read_graph_object_by_digest_with_lease(
                lease,
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
            )
        })?;
    crate::route_component::authenticate_manifest_routes(root.format_version, &files, |entry| {
        crate::graph_object_store::read_graph_object_by_digest_with_lease(
            lease,
            &entry.content_sha256,
            MAX_GRAPH_MANIFEST_SEGMENT_BYTES,
        )
    })?;
    for entry in files {
        crate::graph_object_store::verify_graph_object_with_lease(
            lease,
            &entry.content_sha256,
            entry.byte_length,
        )?;
    }
    Ok(())
}

pub(super) fn stage_participant_files(
    request: &ProjectGenerationRequest,
    generation_root: &Path,
    participants: &[StagedParticipant],
    payloads: ParticipantPayloads<'_>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    for metadata in participants {
        let (index, input) = request
            .participants
            .iter()
            .enumerate()
            .find(|candidate| {
                candidate.1.capability_id == metadata.capability_id
                    && candidate.1.record_family_id == metadata.record_family_id
            })
            .expect("validated canonical metadata has one source participant");
        let destination = generation_root
            .join(PARTICIPANTS_DIR)
            .join(&metadata.relative_path);
        let parent_dir = destination
            .parent()
            .expect("machine-derived participant path has a parent");
        let relative_parent = parent_dir
            .strip_prefix(generation_root)
            .expect("machine-derived participant parent is contained");
        ensure_machine_directory(generation_root, relative_parent)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(publication_io)?;
        let write_result = (|| -> Result<(), GfError> {
            match payloads {
                ParticipantPayloads::Memory => {
                    file.write_all(&input.bytes).map_err(publication_io)?;
                }
                ParticipantPayloads::Files(files, cancelled, copy_buffer_bytes) => {
                    let source = &files[index];
                    let mut input = crate::project_portable::open_regular_nofollow(&source.source)
                        .map_err(publication_io)?;
                    let mut hash = Sha256::new();
                    let mut copied = 0;
                    let mut buffer = vec![0; copy_buffer_bytes];
                    loop {
                        if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                            return Err(project_error(
                                ProjectErrorCode::PublicationFailed,
                                "portable import cancelled during staging",
                            ));
                        }
                        let count = input.read(&mut buffer).map_err(publication_io)?;
                        if count == 0 {
                            break;
                        }
                        file.write_all(&buffer[..count]).map_err(publication_io)?;
                        hash.update(&buffer[..count]);
                        copied += count as u64;
                    }
                    let digest: [u8; 32] = hash.finalize().into();
                    if copied != source.byte_length || digest != source.content_sha256 {
                        return Err(project_error(
                            ProjectErrorCode::PublicationFailed,
                            "portable participant changed during staging",
                        ));
                    }
                }
            }
            Ok(())
        })();
        let observed = allocation.map_or(Ok(()), |allocation| {
            allocation.replace_file_at(&destination, &file)
        });
        write_result?;
        observed?;
        project_failpoint::hit(
            "project.after_participant_write",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "STAGED",
            false,
        )?;
        file.sync_all().map_err(publication_io)?;
        if let Some(allocation) = allocation {
            allocation.replace_file_at(&destination, &file)?;
        }
        project_failpoint::hit(
            "project.after_participant_fsync",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "STAGED",
            false,
        )?;
        verify_participant_file(&destination, metadata)?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct RequestFingerprint<'a> {
    format: &'static str,
    format_version: u32,
    transaction_uuid: String,
    generation_uuid: String,
    capabilities: &'a [ProjectCapability],
    participants: &'a [StagedParticipant],
}

impl Serialize for StagedParticipant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Ordered<'a> {
            capability_id: &'a str,
            capability_version: u32,
            record_family_id: &'a str,
            record_version: u32,
            relative_path: &'a str,
            encoding: &'a str,
            byte_length: u64,
            row_count: u64,
            schema_fingerprint: &'a str,
            content_sha256: &'a str,
        }
        Ordered {
            capability_id: &self.capability_id,
            capability_version: self.capability_version,
            record_family_id: &self.record_family_id,
            record_version: self.record_version,
            relative_path: &self.relative_path,
            encoding: &self.encoding,
            byte_length: self.byte_length,
            row_count: self.row_count,
            schema_fingerprint: &self.schema_fingerprint,
            content_sha256: &self.content_sha256,
        }
        .serialize(serializer)
    }
}

pub(super) fn validate_request(request: &ProjectGenerationRequest) -> Result<(), GfError> {
    if request.capabilities.is_empty() {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "a generation must declare at least one capability",
        ));
    }
    for capability in &request.capabilities {
        validate_machine_id(&capability.capability_id)?;
        if capability.capability_version == 0 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "capability contract versions must be positive",
            ));
        }
    }
    for participant in &request.participants {
        validate_machine_id(&participant.capability_id)?;
        validate_machine_id(&participant.record_family_id)?;
        if participant.capability_version == 0 || participant.record_version == 0 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "participant contract versions must be positive",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn request_metadata(
    request: &ProjectGenerationRequest,
) -> Result<(Vec<ProjectCapability>, Vec<StagedParticipant>, String), GfError> {
    request_metadata_with_payloads(request, ParticipantPayloads::Memory)
}

#[expect(
    clippy::too_many_lines,
    reason = "canonical metadata validation keeps memory and streamed sources identical"
)]
pub(super) fn request_metadata_with_payloads(
    request: &ProjectGenerationRequest,
    payloads: ParticipantPayloads<'_>,
) -> Result<(Vec<ProjectCapability>, Vec<StagedParticipant>, String), GfError> {
    let mut capabilities = request.capabilities.clone();
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    if capabilities
        .windows(2)
        .any(|pair| pair[0].capability_id == pair[1].capability_id)
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "duplicate capability identity",
        ));
    }
    if capabilities
        .binary_search_by(|entry| entry.capability_id.as_str().cmp("graph"))
        .ok()
        .map(|index| capabilities[index].capability_version)
        != Some(1)
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "every generation must declare graph capability version 1",
        ));
    }
    let mut participants = Vec::with_capacity(request.participants.len());
    for (index, participant) in request.participants.iter().enumerate() {
        let (byte_length, content_sha256) = match payloads {
            ParticipantPayloads::Memory => (
                u64::try_from(participant.bytes.len()).map_err(|_| {
                    project_error(
                        ProjectErrorCode::PublicationFailed,
                        "participant byte length exceeds u64",
                    )
                })?,
                Sha256::digest(&participant.bytes).into(),
            ),
            ParticipantPayloads::Files(files, _, _) => {
                let file = files.get(index).ok_or_else(|| {
                    project_error(
                        ProjectErrorCode::PublicationFailed,
                        "missing participant file",
                    )
                })?;
                if file.participant.capability_id != participant.capability_id
                    || file.participant.record_family_id != participant.record_family_id
                {
                    return Err(project_error(
                        ProjectErrorCode::PublicationFailed,
                        "participant file identity mismatch",
                    ));
                }
                (file.byte_length, file.content_sha256)
            }
        };
        participants.push(StagedParticipant {
            capability_id: participant.capability_id.clone(),
            capability_version: participant.capability_version,
            record_family_id: participant.record_family_id.clone(),
            record_version: participant.record_version,
            relative_path: format!(
                "{}/{}.{}",
                participant.capability_id,
                participant.record_family_id,
                participant.encoding.extension()
            ),
            encoding: participant.encoding.extension().into(),
            byte_length,
            row_count: participant.row_count,
            schema_fingerprint: hex_digest(participant.schema_fingerprint),
            content_sha256: hex_digest(content_sha256),
        });
    }
    participants.sort_by(|left, right| {
        (
            &left.capability_id,
            &left.record_family_id,
            &left.relative_path,
        )
            .cmp(&(
                &right.capability_id,
                &right.record_family_id,
                &right.relative_path,
            ))
    });
    if participants.windows(2).any(|pair| {
        pair[0].capability_id == pair[1].capability_id
            && pair[0].record_family_id == pair[1].record_family_id
    }) {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "duplicate participant identity",
        ));
    }
    for participant in &participants {
        let capability = capabilities
            .binary_search_by(|entry| entry.capability_id.cmp(&participant.capability_id))
            .ok()
            .map(|index| &capabilities[index])
            .ok_or_else(|| {
                project_error(
                    ProjectErrorCode::PublicationFailed,
                    "participant capability is not declared",
                )
            })?;
        if capability.capability_version != participant.capability_version {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "participant capability version conflicts with declaration",
            ));
        }
    }
    let fingerprint_input = RequestFingerprint {
        format: "graphforge-publication-request",
        format_version: 1,
        transaction_uuid: request.transaction_uuid.hyphenated().to_string(),
        generation_uuid: request.generation_uuid.hyphenated().to_string(),
        capabilities: &capabilities,
        participants: &participants,
    };
    let bytes = canonical_line(&fingerprint_input)?;
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    Ok((capabilities, participants, hex_digest(digest)))
}

fn validate_machine_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "machine ID must be 1-64 lowercase ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

pub(super) fn verify_participant_file(
    path: &Path,
    expected: &StagedParticipant,
) -> Result<(), GfError> {
    let metadata = std::fs::symlink_metadata(path).map_err(publication_io)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant is not a regular non-link file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "staged participant is hard-linked",
            ));
        }
    }
    if metadata.len() != expected.byte_length {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant byte length changed",
        ));
    }
    let mut file = File::open(path).map_err(publication_io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer).map_err(publication_io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if hex_digest(actual) != expected.content_sha256 {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant digest changed",
        ));
    }
    Ok(())
}

pub(super) fn sync_participant_directories(
    participants_root: &Path,
    participants: &[StagedParticipant],
) -> Result<(), GfError> {
    let mut directories: Vec<PathBuf> = participants
        .iter()
        .filter_map(|participant| {
            participants_root
                .join(&participant.relative_path)
                .parent()
                .map(Path::to_owned)
        })
        .collect();
    directories.sort();
    directories.dedup();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    sync_directory(participants_root)
}

#[cfg(test)]
mod tests;
