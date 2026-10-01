//! Forward actual portable copy captures into graph installation.
use super::*;
use graphforge_filesystem::ObservedSync as _;
use std::io::Read as _;

fn capture_graph_sources(
    graph_tree: &Path,
    paths: &[PathBuf],
    captures: Option<
        &std::collections::BTreeMap<PathBuf, &crate::project_portable_v2::MaterializedCapture>,
    >,
) -> Result<Vec<crate::graph_object_store::AuthenticatedGraphFile>, PortableV2Error> {
    let mut authenticated = Vec::with_capacity(paths.len());
    for relative in paths {
        let source = graph_tree.join(relative);
        let file = if let Some(capture) = captures.and_then(|captures| captures.get(&source)) {
            capture.graph_file(&source, relative.clone())?
        } else {
            // Newly derived adjacency is not an authenticated archive member.
            // Generic naming remains genuine for these newly produced artifacts.
            let length = fs::metadata(&source)
                .map_err(|error| storage(&error))?
                .len();
            let input = crate::project_portable_v2_export::open_source_no_follow(&source)?;
            let bound = length.checked_add(1).ok_or_else(|| {
                PortableV2Error::new(
                    PortableV2ErrorCode::LimitExceeded,
                    "derived artifact length overflow",
                )
            })?;
            let mut input = input.take(bound);
            let mut digest = Sha256::new();
            let mut buffer = [0; 64 * 1024];
            loop {
                let count = input.read(&mut buffer).map_err(|error| storage(&error))?;
                if count == 0 {
                    break;
                }
                digest.update(&buffer[..count]);
            }
            crate::graph_object_store::AuthenticatedGraphFile {
                relative_path: relative.clone(),
                byte_length: length,
                content_sha256: hex(digest.finalize().into()),
            }
        };
        authenticated.push(file);
    }
    Ok(authenticated)
}

pub(super) fn prepare_compact_import_graph_with_allocation(
    target: &Path,
    package_graph_tree: Option<&Path>,
    participants: &mut [ProjectFileParticipant],
    entry_count: usize,
    allocation: Option<&crate::StorageAllocationOperation>,
    captures: Option<
        &std::collections::BTreeMap<PathBuf, &crate::project_portable_v2::MaterializedCapture>,
    >,
) -> Result<Option<crate::GraphObjectPublicationLease>, PortableV2Error> {
    let Some(graph_tree) = package_graph_tree else {
        return Ok(None);
    };
    let Some(participant) = participants.iter_mut().find(|participant| {
        participant.participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.participant.record_family_id == crate::GRAPH_FILES_FAMILY
    }) else {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "graph tree requires a graph/files participant",
        ));
    };
    if !matches!(
        participant.participant.record_version,
        crate::graph_files::GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION
            | crate::graph_files::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
    ) {
        return Ok(None);
    }
    let mut lease =
        crate::begin_graph_object_publication(target).map_err(|error| storage(&error))?;
    lease
        .set_import_allocation_operation(allocation.cloned())
        .map_err(|error| storage(&error))?;
    let directory = graphforge_filesystem::StableDirectory::open(graph_tree).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot authenticate portable graph tree",
        )
    })?;
    let mut paths = Vec::new();
    let mut remaining = entry_count.saturating_mul(2).saturating_add(1024);
    collect_portable_graph_paths(&directory, Path::new(""), &mut paths, &mut remaining)?;
    paths.sort();
    let authenticated = capture_graph_sources(graph_tree, &paths, captures)?;
    let (root, _) = if crate::graph_files::root_is_mapped(participant.participant.record_version) {
        let routes = crate::route_component::owned::read_owned_layout_table(&directory)
            .map_err(|error| storage(&error))?
            .ok_or_else(|| {
                PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "mapped compact import requires route authority",
                )
            })?;
        crate::graph_object_store::append_authenticated_mapped_graph_files(
            &lease,
            graph_tree,
            &mut crate::graph_object_store::GraphManifestState::empty(),
            &authenticated,
            &[],
            &routes,
        )
    } else {
        crate::graph_object_store::append_authenticated_graph_files_v2(
            &lease,
            graph_tree,
            &mut crate::graph_object_store::GraphManifestState::empty(),
            &authenticated,
            &[],
        )
    }
    .map_err(|error| storage(&error))?;
    let published =
        crate::graph_files::graph_files_root_participant(&root).map_err(|error| storage(&error))?;
    let bytes = &published.bytes;
    crate::project_publication::publish_atomic_bytes_with_allocation(
        &participant.source,
        bytes,
        || Ok(()),
        || Ok(()),
        || Ok(()),
        allocation,
    )
    .map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot publish imported compact graph root",
        )
    })?;
    #[cfg(not(windows))]
    let file = fs::File::open(&participant.source);
    #[cfg(windows)]
    let file = OpenOptions::new().write(true).open(&participant.source);
    let file = file.map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot reopen imported compact graph root",
        )
    })?;
    participant.byte_length = bytes.len() as u64;
    participant.content_sha256 =
        graphforge_core::hash_observation::ControlSha256::digest(bytes).into();
    participant.participant = published;
    file.observed_sync_all().map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot sync imported compact graph root",
        )
    })?;
    Ok(Some(lease))
}
