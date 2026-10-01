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
    /// Native identity of the content-store object this entry names. A workspace
    /// file with this identity is that object (hydration hard-links it), so its
    /// bytes carry the declared identity without another read.
    pub(crate) object_identity: Option<graphforge_filesystem::FileIdentity>,
}

impl From<&GraphFileEntry> for KnownGraphFile {
    fn from(entry: &GraphFileEntry) -> Self {
        Self {
            byte_length: entry.byte_length,
            content_sha256: entry.content_sha256.clone(),
            content_xxh64: entry.content_xxh64,
            object_identity: None,
        }
    }
}

/// Capture a private workspace for publication over `parent`, reusing the
/// parent's authenticated declared SHA-256 for every file whose path and exact
/// length match the parent inventory and which either is the parent's own
/// content-store object (same native identity, so unchanged and already
/// admitted) or carries its freshly computed XXH64. Changed and new files are
/// hashed once. Only newly written bytes pay for a new identity.
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
    let compact = matches!(
        parent.declared_graph_files_participant()?,
        Some(GraphFilesParticipant::V2(_))
    );
    let known = entries
        .iter()
        .map(|entry| {
            let mut known = KnownGraphFile::from(entry);
            if compact {
                known.object_identity =
                    crate::graph_object_path(parent.container_root(), &entry.content_sha256)
                        .ok()
                        .and_then(|object| graphforge_filesystem::path_identity(&object).ok());
            }
            (entry.relative_path.clone(), known)
        })
        .collect::<std::collections::HashMap<_, _>>();
    capture_graph_files_reusing_digests(source_root, &known, ARTIFACT_IDENTITY)
}
