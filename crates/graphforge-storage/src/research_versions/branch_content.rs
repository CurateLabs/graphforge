//! Transfer a domain-owner prepared effective Branch into the owning Project CAS.
use super::{
    Digest, GfError, Path, ResearchEvidenceReference, ResearchVersionRecord,
    ResolvedProjectGeneration, Sha256, cancelled, commitments, hex, invalid, retained_content,
};
use std::sync::atomic::AtomicBool;

/// Authenticated immutable content protected from cleanup until publication.
/// Keep this value alive through the owning Project's CURRENT transition.
pub struct PreparedBranchContent {
    /// Canonical effective Branch Version ready for one publication.
    pub version: ResearchVersionRecord,
    pub(super) _lease: crate::GraphObjectPublicationLease,
}

/// Install a selected, domain-validated private preparation into Project CAS.
/// This does not publish authority. The caller must use `PublishBranch` while
/// holding the returned content lease. No private generation is retained.
pub fn prepare_branch_content(
    root: &Path,
    source: &ResolvedProjectGeneration,
    mut version: ResearchVersionRecord,
    cancellation: &AtomicBool,
) -> Result<PreparedBranchContent, GfError> {
    cancelled(cancellation)?;
    if version.content.source_version.is_none() {
        return Err(invalid(
            "prepared Branch content requires exact origin lineage",
        ));
    }
    source.validate_complete_participant_inventory()?;
    let lease = crate::begin_graph_object_publication(root)?;
    let source_root = source.container_root();
    let mut participants = commitments(source)?;
    let mut total = 0_u64;
    for commitment in &mut participants {
        cancelled(cancellation)?;
        // Check the sealed participant size before allocating its snapshot.
        let path = source.participant_path(&commitment.key.capability, &commitment.key.family)?;
        let length = std::fs::metadata(path)
            .map_err(|_| invalid("prepared Branch participant is unavailable"))?
            .len();
        total = total
            .checked_add(length)
            .ok_or_else(|| invalid("prepared Branch byte count overflow"))?;
        if total > 256 * 1024 * 1024 {
            return Err(super::error(
                super::ProjectErrorCode::ResourceLimit,
                "prepared Branch participants exceed the byte limit",
            ));
        }
        let mut snapshot = source
            .participant_snapshot(&commitment.key.capability, &commitment.key.family)?
            .ok_or_else(|| invalid("prepared Branch participant is unavailable"))?;
        if commitment.key.capability == "graph" && commitment.key.family == "files" {
            install_graph(
                root,
                source,
                &lease,
                commitment,
                &mut snapshot,
                cancellation,
            )?;
        }
        crate::graph_object_store::install_graph_object_bytes(root, &snapshot.bytes)?;
    }
    for reference in &version.content.evidence {
        cancelled(cancellation)?;
        if let ResearchEvidenceReference::Local {
            sha256,
            byte_length,
            ..
        } = reference
        {
            let digest = hex(sha256);
            crate::graph_object_store::install_graph_object_file(
                root,
                &crate::graph_object_path(source_root, &digest)?,
                &digest,
                *byte_length,
            )?;
        }
    }
    version.content.participants = participants;
    version.content.graph_projection = None;
    version.content.producer = super::PRODUCER.into();
    retained_content::inspect(root, &version, None)?;
    cancelled(cancellation)?;
    Ok(PreparedBranchContent {
        version,
        _lease: lease,
    })
}

fn install_graph(
    root: &Path,
    source: &ResolvedProjectGeneration,
    lease: &crate::GraphObjectPublicationLease,
    commitment: &mut super::ResearchParticipantCommitment,
    snapshot: &mut crate::ProjectParticipantSnapshot,
    cancellation: &AtomicBool,
) -> Result<(), GfError> {
    let source_root = source.container_root();
    match source.declared_graph_files_participant()? {
        Some(crate::GraphFilesParticipant::V1(inventory)) => {
            let graph =
                retained_content::install_graph(lease, &source.graph_tree_root(), &inventory)?;
            let replacement = crate::graph_files::graph_files_root_participant(&graph)?;
            snapshot.bytes = replacement.bytes;
            commitment.record_version = replacement.record_version;
            commitment.schema_sha256 = replacement.schema_fingerprint;
            commitment.row_count = replacement.row_count;
            commitment.content_sha256 = Sha256::digest(&snapshot.bytes).into();
        }
        Some(crate::GraphFilesParticipant::V2(_)) => {
            let (files, nodes) = retained_content::graph_closure(
                source_root,
                snapshot.record_version,
                &snapshot.bytes,
                None,
            )?;
            for node in nodes {
                cancelled(cancellation)?;
                let bytes = crate::read_graph_object_by_digest(
                    source_root,
                    &node,
                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                )?;
                crate::graph_object_store::install_graph_object_bytes(root, &bytes)?;
            }
            for file in files {
                cancelled(cancellation)?;
                crate::graph_object_store::install_graph_object_file(
                    root,
                    &crate::graph_object_path(source_root, &file.content_sha256)?,
                    &file.content_sha256,
                    file.byte_length,
                )?;
            }
        }
        None => return Err(invalid("prepared Branch graph inventory is unavailable")),
    }
    Ok(())
}

/// Open authenticated prepared content in an empty private ephemeral container.
/// The preparation lease stays alive; no source Project head is published.
pub fn materialize_prepared_branch(
    root: &Path,
    prepared: &PreparedBranchContent,
    target: &Path,
) -> Result<ResolvedProjectGeneration, GfError> {
    super::project_restore::materialize(root, &prepared.version, target, true)
}
