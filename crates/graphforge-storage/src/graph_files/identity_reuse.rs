//! Reuse of already-authenticated graph file identities during capture.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Seek;
use std::path::PathBuf;

use super::{
    ARTIFACT_IDENTITY, GfError, GraphFileEntry, GraphFilesInventory, GraphFilesParticipant, Path,
    build_inventory_for_owned_layout,
};

/// How many freshly hashed files a capture keeps open at once. Beyond it a
/// file is simply hashed again as it is installed (SHA-256 twice for that file),
/// so a commit that rewrites thousands of files never holds thousands of
/// descriptors. A commit changes a handful of files, so the bound is not met in
/// ordinary use.
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

/// The digest, checksum and read calls for one workspace file, plus the handle
/// that was hashed when the file was not reused from the parent's identity.
pub(crate) fn capture_payload_identity(
    path: &Path,
    reused: Option<&KnownGraphFile>,
    domain: graphforge_core::hash_observation::HashDomain,
) -> Result<(String, u64, u64, Option<File>), GfError> {
    let mut file =
        File::open(path).map_err(|error| super::storage("open graph file", path, error))?;
    // A hydrated payload nothing has read yet is admitted before its bytes can
    // name a new identity: a corrupted hard-linked object must be refused, not
    // republished under a fresh digest (#1388).
    crate::graph_admission::admit_file(&file)?;
    // Windows admission uses seek_read, which advances this handle's cursor.
    file.rewind()
        .map_err(|error| super::storage("rewind admitted graph file", path, error))?;
    let mut prior_calls = 0;
    if let Some(known) = reused {
        // The workspace file is the parent's own content-store object: nothing
        // was written, so there are no new bytes to name and none to read. The
        // reuse references the existing object by its existing digest, never a
        // fresh one, so a corrupted object stays corrupted and is refused by
        // its reader (admitted above where it is first-touch).
        if let Some(object) = known.object_identity
            && graphforge_filesystem::file_identity(&file).is_ok_and(|identity| identity == object)
        {
            return Ok((known.content_sha256.clone(), known.content_xxh64, 0, None));
        }
        // Reuse the authenticated identity only when these exact bytes still
        // carry its checksum. A mismatch is a change, never a stale reuse.
        let (checksum, calls) = super::checksum_reader(&mut file, path)?;
        if checksum == known.content_xxh64 {
            return Ok((known.content_sha256.clone(), checksum, calls, None));
        }
        prior_calls = calls;
        file =
            File::open(path).map_err(|error| super::storage("reopen graph file", path, error))?;
    }
    let (digest, checksum, calls) = super::hash_reader_with_checksum(&mut file, path, domain)?;
    let calls = calls
        .checked_add(prior_calls)
        .ok_or_else(|| super::resource_limit("graph file authentication read calls overflow"))?;
    Ok((super::hex_digest(digest), checksum, calls, Some(file)))
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
    /// Payload read calls the capture made: none for a file that is its parent's
    /// own object, one pass for every other.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "observed by the capture's tests")
    )]
    pub(crate) read_calls: u64,
}

/// Capture a private workspace for compact publication over `parent`, reusing
/// the parent's authenticated declared SHA-256 for every file whose path and
/// exact length match the parent inventory and which either is the parent's own
/// content-store object (same native identity, so unchanged and already
/// admitted) or carries its freshly computed XXH64. Changed and new files are
/// hashed once, and each is retained for installation (at most
/// [`MAX_RETAINED_CAPTURES`]; a commit that changes more files hashes the rest a
/// second time as it installs them).
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
    let (inventory, read_calls) = build_inventory_for_owned_layout(
        source_root,
        false,
        Some(&known),
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        Some(&mut captured),
        None,
    )?;
    Ok(WorkspaceCapture {
        inventory,
        captured,
        read_calls,
    })
}

/// Capture rebuilt adjacency files afresh even when their bytes match the
/// parent's inventory. Explicit repair needs retained SHA-authenticated source
/// handles so compact publication can replace a corrupt object at that digest.
pub(crate) fn capture_workspace_over_parent_repairing_adjacency(
    source_root: &Path,
    parent: &crate::ResolvedProjectGeneration,
) -> Result<WorkspaceCapture, GfError> {
    let known = known_files(parent)?;
    let mut captured = BTreeMap::new();
    let (inventory, read_calls) = build_inventory_for_owned_layout(
        source_root,
        false,
        Some(&known),
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        Some(&mut captured),
        Some("indexes/adjacency/"),
    )?;
    Ok(WorkspaceCapture {
        inventory,
        captured,
        read_calls,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_touch_capture_hashes_the_complete_payload() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("topology")).unwrap();
        let path = root.path().join("topology/nodes.parquet");
        std::fs::write(&path, b"capture this nonempty payload after admission").unwrap();
        let expected = crate::capture_graph_files(root.path()).unwrap().0;
        let entry = &expected.files[0];
        crate::graph_admission::AdmissionBatch::begin().register(
            graphforge_filesystem::path_identity(&path).unwrap(),
            entry,
            path.clone(),
            root.path(),
            path.clone(),
        );

        let actual = crate::capture_graph_files(root.path()).unwrap().0;
        assert_eq!(actual, expected);
        crate::graph_files::resolve_v1_inventory_entry_retained(root.path(), &actual.files[0])
            .unwrap();
    }

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

    fn publish_compact(root: &Path, workspace: &Path) -> crate::ResolvedProjectGeneration {
        use crate::{
            GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, ProjectCapability,
            ProjectGenerationRequest, ProjectStageOutcome, empty_workspace_participants,
        };
        {
            let parent = crate::resolve_project_generation(root).unwrap();
            let (participant, lease) =
                crate::prepare_compact_graph_publication(&parent, workspace).unwrap();
            let mut participants = empty_workspace_participants().unwrap();
            participants.insert(0, participant);
            let request = ProjectGenerationRequest {
                transaction_uuid: uuid::Uuid::now_v7(),
                generation_uuid: uuid::Uuid::now_v7(),
                capabilities: vec![
                    ProjectCapability {
                        capability_id: GRAPH_CAPABILITY_ID.into(),
                        capability_version: GRAPH_CAPABILITY_VERSION,
                    },
                    ProjectCapability {
                        capability_id: "workspace".into(),
                        capability_version: 1,
                    },
                ],
                participants,
            };
            let ProjectStageOutcome::Staged(staged) =
                crate::stage_project_generation_with_graph_tree(root, &request, None).unwrap()
            else {
                panic!("fresh publication replayed");
            };
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .unwrap()
                .publish_with_graph_objects(&lease)
                .unwrap();
            crate::resolve_project_generation(root).unwrap()
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
            capture_payload_identity(&linked, Some(&known), ARTIFACT_IDENTITY).unwrap();
        assert_eq!(
            digest, known.content_sha256,
            "the declared digest is reused"
        );
        assert_eq!(checksum, known.content_xxh64);
        assert_eq!(calls, 0, "no byte of the object is read");
        assert!(hashed.is_none(), "nothing is retained for installation");

        let (digest, checksum, calls, hashed) =
            capture_payload_identity(&copied, Some(&known), ARTIFACT_IDENTITY).unwrap();
        assert_ne!(
            digest, known.content_sha256,
            "a copy is named by its own bytes"
        );
        assert_eq!(
            checksum,
            crate::corruption_checksum::checksum(b"immutable payload")
        );
        assert!(calls > 0);
        assert!(
            hashed.is_some(),
            "a freshly hashed file is retained to install"
        );
    }
    /// Publishing a workspace over a compact parent installs what changed and
    /// reads nothing else: hydrated files are the parent's own objects, so a
    /// capture of them reads no byte; a new file is read once and retained.
    #[test]
    fn a_capture_over_a_compact_parent_reads_only_what_changed() {
        let root = tempfile::tempdir().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let workspace = tempfile::tempdir_in(root.path()).unwrap();
        std::fs::create_dir_all(workspace.path().join("topology")).unwrap();
        std::fs::write(
            workspace.path().join("topology/nodes.parquet"),
            vec![7_u8; 8 * 64 * 1024],
        )
        .unwrap();
        std::fs::write(workspace.path().join("topology/generation.json"), b"{}\n").unwrap();

        // The first commit has no graph parent and still publishes compact.
        let first = publish_compact(root.path(), workspace.path());
        assert!(first.declared_graph_files_inventory().unwrap().is_none());
        let inventory = first.graph_files_inventory().unwrap().unwrap();

        // Hydrate it as an open does, then capture the untouched workspace.
        let hydrated = tempfile::tempdir_in(root.path()).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, hydrated.path()).unwrap();
        let untouched = capture_workspace_over_parent(hydrated.path(), &first).unwrap();
        // Hydration writes the route table afresh as a private single-link file, so
        // it alone is read; the payload files are the parent's own objects.
        assert!(
            untouched
                .captured
                .keys()
                .all(|path| path == "semantic-routes.json"),
            "only the private route table is new: {:?}",
            untouched.captured.keys().collect::<Vec<_>>()
        );
        assert!(
            untouched.read_calls <= 2,
            "the 512 KiB payload is not read to capture it: {} read calls",
            untouched.read_calls
        );
        let payload = |inventory: &GraphFilesInventory| {
            inventory
                .files
                .iter()
                .filter(|entry| entry.relative_path != "semantic-routes.json")
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(payload(&untouched.inventory), payload(&inventory));

        // A new file is read once and kept; the payload still reads nothing.
        std::fs::write(hydrated.path().join("topology/added.bin"), vec![9_u8; 2048]).unwrap();
        let changed = capture_workspace_over_parent(hydrated.path(), &first).unwrap();
        assert!(changed.captured.contains_key("topology/added.bin"));
        assert!(!changed.captured.contains_key("topology/nodes.parquet"));
        assert_eq!(
            changed.read_calls,
            untouched.read_calls + 1,
            "one pass over the one new file"
        );

        // Publishing that workspace installs the one object and keeps the rest.
        let second = publish_compact(root.path(), hydrated.path());
        let after = second.graph_files_inventory().unwrap().unwrap();
        assert!(
            after
                .files
                .iter()
                .any(|entry| entry.relative_path == "topology/added.bin")
        );
        for entry in payload(&inventory) {
            assert!(
                after.files.contains(&entry),
                "{} is carried over",
                entry.relative_path
            );
        }
    }
    /// A tree hydrated from a compact generation is made of hard links to sealed
    /// (read-only) objects. Removing it, as every private view does when it is
    /// dropped, must work on every platform (Windows refuses to delete a
    /// read-only file) and must leave the sealed objects themselves intact.
    #[test]
    fn a_hydrated_tree_of_sealed_objects_can_be_removed_and_leaves_the_objects() {
        let root = tempfile::tempdir().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let workspace = tempfile::tempdir_in(root.path()).unwrap();
        std::fs::create_dir_all(workspace.path().join("topology")).unwrap();
        std::fs::write(workspace.path().join("topology/nodes.parquet"), b"payload").unwrap();
        let generation = publish_compact(root.path(), workspace.path());
        let inventory = generation.graph_files_inventory().unwrap().unwrap();
        let hydrated = tempfile::tempdir_in(root.path()).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, hydrated.path()).unwrap();
        let linked = hydrated.path().join("topology/nodes.parquet");
        assert!(
            std::fs::metadata(&linked).unwrap().permissions().readonly(),
            "the hydrated file is the sealed object"
        );
        let hydrated_path = hydrated.path().to_path_buf();
        drop(hydrated);
        assert!(!hydrated_path.exists(), "the private tree is removable");
        for entry in &inventory.files {
            let object = crate::graph_object_path(root.path(), &entry.content_sha256).unwrap();
            assert!(
                object.is_file(),
                "{} survives its view",
                entry.relative_path
            );
        }
    }
}
