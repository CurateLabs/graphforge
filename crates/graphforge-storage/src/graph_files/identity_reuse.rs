//! Reuse of already-authenticated graph file identities during capture.

use super::{
    ARTIFACT_IDENTITY, GfError, GraphFileEntry, GraphFilesInventory, GraphFilesParticipant, Path,
    ProjectParticipant, capture_graph_files_reusing_digests,
};

/// An already-authenticated graph file identity that a later capture may reuse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KnownGraphFile {
    pub(crate) byte_length: u64,
    pub(crate) content_sha256: String,
    pub(crate) content_xxh64: u64,
}

impl From<&GraphFileEntry> for KnownGraphFile {
    fn from(entry: &GraphFileEntry) -> Self {
        Self {
            byte_length: entry.byte_length,
            content_sha256: entry.content_sha256.clone(),
            content_xxh64: entry.content_xxh64,
        }
    }
}

/// Capture a private workspace for publication over `parent`, reusing the
/// parent's authenticated declared SHA-256 for every file whose path, exact
/// length and freshly computed XXH64 all match the parent inventory. Changed and new
/// files are hashed once. Only newly written bytes pay for a new identity.
///
/// # Errors
/// Rejects links, special files, unsafe relative paths, duplicates, and
/// inventory size overflow, and propagates parent inventory admission errors.
pub fn capture_graph_files_over_parent(
    source_root: &Path,
    parent: &crate::ResolvedProjectGeneration,
) -> Result<(GraphFilesInventory, ProjectParticipant), GfError> {
    // Reuse needs only the parent's authenticated declared identities: every
    // reuse is gated by a fresh checksum of the new bytes, so the parent's
    // payload files are never read here.
    let entries = match parent.declared_graph_files_participant()? {
        Some(GraphFilesParticipant::V1(inventory)) => inventory.files,
        Some(GraphFilesParticipant::V2(root)) => {
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                crate::graph_object_store::read_graph_control_object_by_digest(
                    parent.container_root(),
                    digest,
                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                )
            })?
            .0
        }
        None => Vec::new(),
    };
    let known = entries
        .iter()
        .map(|entry| (entry.relative_path.clone(), KnownGraphFile::from(entry)))
        .collect::<std::collections::HashMap<_, _>>();
    capture_graph_files_reusing_digests(source_root, &known, ARTIFACT_IDENTITY)
}
