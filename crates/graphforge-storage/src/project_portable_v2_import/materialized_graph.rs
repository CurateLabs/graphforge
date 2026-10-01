//! Forward actual portable copy captures into graph installation.
use super::*;
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use sha2::Digest;

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
            let source = capture.open_source(&source)?;
            crate::graph_object_store::AuthenticatedGraphFile {
                relative_path: relative.clone(),
                byte_length: source.bytes(),
                content_sha256: source.content_sha256().to_owned(),
            }
        } else {
            if captures.is_some() {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::ConcurrentMutation,
                    "portable graph file lacks private byte capture",
                ));
            }
            // Private test helpers can provide an untrusted tree without a
            // scanner/writer capture. Its generic installer keeps genuine SHA.
            let length = fs::metadata(&source)
                .map_err(|_| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Io,
                        "cannot inspect untrusted graph file",
                    )
                })?
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
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let count = input.read(&mut buffer).map_err(|_| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Io,
                        "cannot read untrusted graph file",
                    )
                })?;
                if count == 0 {
                    break;
                }
                digest.update(&buffer[..count]);
            }
            crate::graph_object_store::AuthenticatedGraphFile {
                relative_path: relative.clone(),
                byte_length: length,
                content_sha256: digest.finalize().iter().fold(
                    String::with_capacity(64),
                    |mut text, byte| {
                        use std::fmt::Write as _;
                        write!(text, "{byte:02x}").expect("writing a digest to String succeeds");
                        text
                    },
                ),
            }
        };
        authenticated.push(file);
    }
    Ok(authenticated)
}

pub(super) struct PreparedCompactImport {
    pub(super) lease: crate::GraphObjectPublicationLease,
    pub(super) root_path: PathBuf,
    pub(super) root_capture: crate::project_portable_v2::MaterializedCapture,
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
) -> Result<Option<PreparedCompactImport>, PortableV2Error> {
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
    let routes = if crate::graph_files::root_is_mapped(participant.participant.record_version) {
        Some(
            crate::route_component::owned::read_owned_layout_table(&directory)
                .map_err(|error| storage(&error))?
                .ok_or_else(|| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::InvalidStructure,
                        "mapped compact import requires route authority",
                    )
                })?,
        )
    } else {
        None
    };
    let empty_captures = std::collections::BTreeMap::new();
    let (root, _) = crate::graph_object_store::append_captured_portable_graph_files(
        &lease,
        graph_tree,
        &mut crate::graph_object_store::GraphManifestState::empty(),
        &authenticated,
        routes.as_ref(),
        captures.unwrap_or(&empty_captures),
    )
    .map_err(|error| storage(&error))?;
    let root_capture =
        crate::project_portable_v2::publish_compact_import_root(participant, &root, allocation)?;
    Ok(Some(PreparedCompactImport {
        lease,
        root_path: participant.source.clone(),
        root_capture,
    }))
}
