//! Reuse of already-authenticated graph file identities during capture.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

use super::{
    ARTIFACT_IDENTITY, GfError, GraphFileEntry, GraphFilesInventory, GraphFilesParticipant, Path,
    ProjectParticipant, build_inventory_for_owned_layout, capture_graph_files_reusing_digests,
};

/// How many freshly hashed files a capture keeps open at once. Beyond it a
/// file is simply hashed again as it is installed, so a commit that rewrites
/// thousands of files never holds thousands of descriptors.
pub(crate) const MAX_RETAINED_CAPTURES: usize = 128;

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
    let known = known_files(parent)?;
    capture_graph_files_reusing_digests(source_root, &known, ARTIFACT_IDENTITY)
}

/// The parent's authenticated declared identities, keyed by path. Reuse needs
/// nothing more: every reuse is gated by a fresh checksum of the new bytes (or
/// by the file being the parent's own content-store object), so the parent's
/// payload files are never read here.
fn known_files(
    parent: &crate::ResolvedProjectGeneration,
) -> Result<std::collections::HashMap<String, KnownGraphFile>, GfError> {
    let participant = parent.declared_graph_files_participant()?;
    let compact = matches!(participant, Some(GraphFilesParticipant::V2(_)));
    let entries = match participant {
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
    Ok(known)
}

/// A workspace file hashed by a capture, kept open so that installing it into
/// the content store checks the copied bytes against the checksum taken while
/// hashing instead of hashing the same bytes with SHA-256 a second time.
///
/// Only [`capture_workspace_over_parent`] mints one, from the handle it hashed.
pub(crate) struct CapturedWorkspaceFile {
    file: File,
    path: PathBuf,
    identity: Option<graphforge_filesystem::FileIdentity>,
    byte_length: u64,
    content_sha256: String,
    content_xxh64: u64,
}

impl CapturedWorkspaceFile {
    pub(super) fn new(
        file: File,
        path: PathBuf,
        byte_length: u64,
        content_sha256: String,
        content_xxh64: u64,
    ) -> Self {
        let identity = graphforge_filesystem::file_identity(&file).ok();
        Self {
            file,
            path,
            identity,
            byte_length,
            content_sha256,
            content_xxh64,
        }
    }

    pub(crate) fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    pub(crate) const fn bytes(&self) -> u64 {
        self.byte_length
    }

    pub(crate) const fn checksum(&self) -> u64 {
        self.content_xxh64
    }

    pub(crate) const fn source(&self) -> &File {
        &self.file
    }

    /// The captured handle and the workspace name still denote one file of the
    /// captured length. A change in between is refused, never installed.
    pub(crate) fn revalidate(&self) -> Result<(), GfError> {
        let unchanged = self.identity.is_some_and(|identity| {
            self.file
                .metadata()
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() == self.byte_length)
                && graphforge_filesystem::file_identity(&self.file)
                    .is_ok_and(|current| current == identity)
                && graphforge_filesystem::path_identity(&self.path)
                    .is_ok_and(|named| named == identity)
        });
        if unchanged {
            Ok(())
        } else {
            Err(GfError::Validation(
                "captured workspace file identity or length changed before install".into(),
            ))
        }
    }
}

/// An inventory of a private workspace over its parent, with the handles of the
/// files that must be installed.
pub(crate) struct WorkspaceCapture {
    pub(crate) inventory: GraphFilesInventory,
    pub(crate) captured: BTreeMap<String, CapturedWorkspaceFile>,
}

/// Capture a private workspace for compact publication over `parent`: the
/// parent's identities are reused exactly as in [`capture_graph_files_over_parent`],
/// and every file that had to be hashed is retained for installation.
///
/// # Errors
/// Rejects links, special files, unsafe relative paths, duplicates, and
/// inventory size overflow, and propagates parent inventory admission errors.
pub(crate) fn capture_workspace_over_parent(
    source_root: &Path,
    parent: &crate::ResolvedProjectGeneration,
) -> Result<WorkspaceCapture, GfError> {
    let known = known_files(parent)?;
    let mut captured = BTreeMap::new();
    let (inventory, _) = build_inventory_for_owned_layout(
        source_root,
        false,
        Some(&known),
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        Some(&mut captured),
    )?;
    Ok(WorkspaceCapture {
        inventory,
        captured,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_for(
        path: &Path,
        object_identity: Option<graphforge_filesystem::FileIdentity>,
    ) -> KnownGraphFile {
        KnownGraphFile {
            byte_length: std::fs::metadata(path).unwrap().len(),
            content_sha256: "ab".repeat(32),
            // A checksum the bytes cannot have: only an identity match can reuse it.
            content_xxh64: 0,
            object_identity,
        }
    }

    /// The workspace file that is the parent's own content-store object carries
    /// that object's identity: nothing was written, so nothing is read or hashed.
    /// A copy of the same bytes is a different file and is captured afresh.
    #[test]
    fn a_workspace_file_that_is_the_parents_object_is_reused_without_a_read() {
        let directory = tempfile::tempdir().unwrap();
        let object = directory.path().join("object");
        let linked = directory.path().join("linked");
        let copied = directory.path().join("copied");
        std::fs::write(&object, b"immutable payload").unwrap();
        std::fs::hard_link(&object, &linked).unwrap();
        std::fs::copy(&object, &copied).unwrap();
        let identity = graphforge_filesystem::path_identity(&object).unwrap();
        let known = known_for(&object, Some(identity));

        let (digest, checksum, calls, hashed) =
            super::super::capture_payload_identity(&linked, Some(&known), ARTIFACT_IDENTITY)
                .unwrap();
        assert_eq!(digest, known.content_sha256, "the declared digest is reused");
        assert_eq!(checksum, known.content_xxh64);
        assert_eq!(calls, 0, "no byte of the object is read");
        assert!(hashed.is_none(), "nothing is retained for installation");

        let (digest, checksum, calls, hashed) =
            super::super::capture_payload_identity(&copied, Some(&known), ARTIFACT_IDENTITY)
                .unwrap();
        assert_ne!(digest, known.content_sha256, "a copy is named by its own bytes");
        assert_eq!(
            checksum,
            crate::corruption_checksum::checksum(b"immutable payload")
        );
        assert!(calls > 0);
        assert!(hashed.is_some(), "a freshly hashed file is retained to install");
    }
}
