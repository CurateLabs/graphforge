//! Capture the actual bounded compact-root bytes emitted during import.
use super::{fs, MaterializedCapture, PortableV2Error, PortableV2ErrorCode};
use graphforge_core::hash_observation::ControlSha256;
use sha2::Digest;

/// Encode and publish the root before minting authority over its exact bytes.
/// Callers provide a semantic root, never a digest/checksum tuple to bless.
pub(crate) fn publish_compact_import_root(
    participant: &mut crate::project_publication::ProjectFileParticipant,
    root: &crate::GraphFilesRootV2,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<MaterializedCapture, PortableV2Error> {
    let published = crate::graph_files::graph_files_root_participant(root)
        .map_err(|_| failed("cannot encode imported compact graph root"))?;
    let bytes = &published.bytes;
    let digest = ControlSha256::digest(bytes).into();
    let checksum = crate::corruption_checksum::checksum(bytes);
    crate::project_publication::publish_atomic_bytes_with_allocation(
        &participant.source,
        bytes,
        || Ok(()),
        || Ok(()),
        || Ok(()),
        allocation,
    )
    .map_err(|_| failed("cannot publish imported compact graph root"))?;
    #[cfg(not(windows))]
    let file = fs::File::open(&participant.source);
    #[cfg(windows)]
    let file = fs::OpenOptions::new().write(true).open(&participant.source);
    let file = file.map_err(|_| failed("cannot reopen imported compact graph root"))?;
    crate::durable_commit::seal_file(&file)
        .map_err(|_| failed("cannot sync imported compact graph root"))?;
    let capture = MaterializedCapture {
        identity: graphforge_filesystem::file_identity(&file)
            .map_err(|_| failed("cannot identify imported compact graph root"))?,
        length: bytes.len() as u64,
        digest,
        checksum,
        allocated_bytes: graphforge_filesystem::file_space_usage(&file)
            .map_err(|_| failed("cannot inspect imported compact graph root allocation"))?
            .allocated_bytes,
    };
    // Release the publication writer before reopening the retained source,
    // including Windows handles that do not permit concurrent access.
    drop(file);
    capture.authenticate(&participant.source, None)?;
    capture.open_source(&participant.source)?;
    participant.byte_length = bytes.len() as u64;
    participant.content_sha256 = digest;
    participant.participant = published;
    Ok(capture)
}

fn failed(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewritten_compact_root_replaces_stale_capture_and_refuses_changed_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("graph-files.json");
        let mut root = crate::GraphFilesRootV2 {
            format: "graphforge-graph-files-root".into(),
            format_version: crate::GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION,
            root_node_sha256: "0".repeat(64),
            logical_file_count: 0,
            logical_byte_length: 0,
        };
        let published = crate::graph_files::graph_files_root_participant(&root).unwrap();
        let mut participant = crate::project_publication::ProjectFileParticipant {
            source: path.clone(),
            byte_length: published.bytes.len() as u64,
            content_sha256: ControlSha256::digest(&published.bytes).into(),
            participant: published,
        };
        let old = publish_compact_import_root(&mut participant, &root, None).unwrap();
        root.root_node_sha256 = "1".repeat(64);
        let observed = graphforge_core::hash_observation::operation::Capture::start();
        let fresh = publish_compact_import_root(&mut participant, &root, None).unwrap();
        let work = observed.snapshot();
        drop(observed);
        let file = fs::File::open(&path).unwrap();
        let checksum = crate::corruption_checksum::checksum(&participant.participant.bytes);
        assert!(!old.matches_file(
            &file,
            participant.byte_length,
            participant.content_sha256,
            checksum,
        ));
        assert!(fresh.matches_file(
            &file,
            participant.byte_length,
            participant.content_sha256,
            checksum,
        ));
        assert_eq!(work.artifact_payload_sha256_bytes, 0);
        assert_eq!(work.portable_authentication_sha256_bytes, 0);
        assert_eq!(work.unclassified_sha256_bytes, 0);
        assert!(work.checksum_bytes >= participant.byte_length * 2);
        assert_eq!(fs::read(&path).unwrap(), participant.participant.bytes);
        drop(file);
        let mut changed = participant.participant.bytes.clone();
        changed[0] ^= 1;
        fs::write(&path, changed).unwrap();
        assert_eq!(
            fresh.authenticate(&path, None).unwrap_err().code,
            PortableV2ErrorCode::ConcurrentMutation,
        );
    }
}
