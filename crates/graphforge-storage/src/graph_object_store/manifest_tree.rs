//! manifest tree ownership for immutable graph objects.

use super::AuthenticatedGraphFile;
use super::BTreeMap;
use super::BTreeSet;
use super::Digest;
use super::GRAPH_FILES_V2_FORMAT;
use super::GRAPH_MANIFEST_NODE_FORMAT;
use super::GRAPH_MANIFEST_NODE_VERSION;
use super::GRAPH_RADIX_DEPTH;
use super::GfError;
use super::GraphFilesAppendEvidence;
use super::GraphFilesInventory;
use super::GraphFilesMigrationEvidence;
use super::GraphFilesRootV2;
use super::GraphManifestNode;
use super::GraphManifestNodeKind;
use super::GraphObjectPublicationLease;
use super::GraphPublicationIo;
use super::Path;
use super::PathBuf;
use super::ReadIoEvidence;
use super::fs;
use super::hash_regular_file;
use super::hex_digest;
use super::install_graph_manifest_node_with_lease;
use super::install_graph_object_file_with_lease;
use super::read_graph_object_by_digest_file_counted_in_domain;
use super::returned_error_boundary;
use super::storage;
use super::validate_digest;
use super::validate_logical_path;
use super::validate_publication_identity;
use super::validation;
use graphforge_core::hash_observation::ControlSha256 as Sha256;

#[derive(Clone, Copy)]
enum CapturedGraphInventory<'a, 'b> {
    Encoded(&'a crate::graph_construction::CapturedEncodedInventory<'b>),
    Portable(&'a BTreeMap<PathBuf, &'b crate::project_portable_v2::MaterializedCapture>),
    Workspace(&'a BTreeMap<String, crate::graph_files::CapturedWorkspaceFile>),
}

/// Only the import owner's privately authenticated copies can choose this path.
pub(crate) fn append_captured_portable_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    files: &[AuthenticatedGraphFile],
    routes: Option<&crate::route_component::RouteTable>,
    captures: &BTreeMap<PathBuf, &crate::project_portable_v2::MaterializedCapture>,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let paths = files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<_>>();
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        &paths,
        Some(files),
        &[],
        routes,
        Some(CapturedGraphInventory::Portable(captures)),
        &mut || false,
    )
}

/// Storage-owned, root-bound state for a sequence of path-copy publications.
///
/// Opening an existing root authenticates its inventory exactly once. Callers
/// cannot replace the cached inventory independently of the root.
#[derive(Debug, Clone, Default)]
pub struct GraphManifestState {
    project_identity: Option<graphforge_filesystem::FileIdentity>,
    root: Option<GraphFilesRootV2>,
    entries: BTreeMap<String, crate::GraphFileEntry>,
}

impl GraphManifestState {
    /// Start an empty authenticated publication sequence.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            project_identity: None,
            root: None,
            entries: BTreeMap::new(),
        }
    }

    /// Authenticate an existing root once, then retain its exact inventory.
    pub fn open(
        lease: &GraphObjectPublicationLease,
        root: GraphFilesRootV2,
        limits: crate::GraphManifestLimits,
    ) -> Result<(Self, crate::GraphManifestResolveEvidence), GfError> {
        validate_publication_identity(lease)?;
        let mut read_calls = 0_u64;
        let (entries, mut evidence) = crate::resolve_graph_manifest(&root, limits, |digest| {
            let (bytes, io) = read_graph_object_by_digest_file_counted_in_domain(
                lease.cas.open_digest(digest)?,
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                &lease.cas.diagnostic_root,
                graphforge_core::hash_observation::HashDomain::ControlAuthentication,
            )?;
            read_calls = read_calls
                .checked_add(io.calls)
                .ok_or_else(|| validation("manifest open read calls overflow"))?;
            Ok(bytes)
        })?;
        let mut authority_read_bytes = 0_u64;
        crate::route_component::authenticate_manifest_routes(
            root.format_version,
            &entries,
            |entry| {
                let (bytes, io) = read_graph_object_by_digest_file_counted_in_domain(
                    lease.cas.open_digest(&entry.content_sha256)?,
                    &entry.content_sha256,
                    64 * 1024 * 1024,
                    &lease.cas.diagnostic_root,
                    graphforge_core::hash_observation::HashDomain::ControlAuthentication,
                )?;
                authority_read_bytes = authority_read_bytes
                    .checked_add(io.bytes)
                    .ok_or_else(|| validation("route authority read bytes overflow"))?;
                read_calls = read_calls
                    .checked_add(io.calls)
                    .ok_or_else(|| validation("route authority read count overflow"))?;
                Ok(bytes)
            },
        )?;
        evidence.authority_read_bytes = authority_read_bytes;
        evidence.application_read_calls = read_calls;
        Ok((
            Self {
                project_identity: Some(lease.cas.project.identity()),
                root: Some(root),
                entries: entries
                    .into_iter()
                    .map(|entry| (entry.relative_path.clone(), entry))
                    .collect(),
            },
            evidence,
        ))
    }

    /// Current authenticated compact root, if one has been published.
    #[must_use]
    pub const fn root(&self) -> Option<&GraphFilesRootV2> {
        self.root.as_ref()
    }

    pub(crate) fn entry(&self, path: &str) -> Option<&crate::GraphFileEntry> {
        self.entries.get(path)
    }

    /// Current entries in canonical logical-path order.
    #[must_use]
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &crate::GraphFileEntry> {
        self.entries.values()
    }
}
/// Seal only changed logical files into a structurally shared radix manifest.
#[allow(clippy::too_many_lines)]
pub fn append_graph_files_v2(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_paths: &[PathBuf],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        sealed_paths,
        None,
        tombstones,
        None,
        None,
        &mut || false,
    )
}

/// Prepare a compact (V2) root for a private candidate over any parent.
///
/// Every parent shape publishes a compact root, so a mutating commit never
/// leaves an expanded inventory behind: a compact parent reuses its unchanged
/// objects and installs only changed files and tombstones; an expanded or
/// absent parent (the first commit on an empty project, or a pre-existing
/// expanded generation converting on its next commit) installs the candidate
/// once into an empty state. Keep the returned lease through CURRENT and stage
/// without a graph tree.
///
/// # Errors
/// Rejects invalid inventories, route authority, or object publication failures.
pub fn prepare_graph_files_replacement(
    parent: &crate::ResolvedProjectGeneration,
    workspace: &Path,
    inventory: &GraphFilesInventory,
) -> Result<
    (
        crate::ProjectParticipant,
        crate::GraphObjectPublicationLease,
    ),
    GfError,
> {
    prepare_compact_root(parent, workspace, inventory, None, false)
}

/// Capture a private workspace over `parent` and publish it as a compact root:
/// the commit path of every graph mutation. Unchanged files install nothing and
/// read nothing beyond the parent's declared identities; each changed file is
/// hashed once, while it is captured, and installed against that capture.
/// Keep the returned lease through CURRENT and stage without a graph tree.
///
/// # Errors
/// Rejects unsafe workspaces, corrupted payloads, invalid route authority, or
/// object publication failures.
pub fn prepare_compact_graph_publication(
    parent: &crate::ResolvedProjectGeneration,
    workspace: &Path,
) -> Result<
    (
        crate::ProjectParticipant,
        crate::GraphObjectPublicationLease,
    ),
    GfError,
> {
    let capture = crate::graph_files::capture_workspace_over_parent(workspace, parent)?;
    prepare_compact_root(
        parent,
        workspace,
        &capture.inventory,
        Some(&capture.captured),
        false,
    )
}

/// Capture a compact publication for the explicit adjacency repair action.
/// The resulting lease can replace an existing corrupt CAS object only when
/// the changed logical path is an adjacency index and the staged source hashes
/// to that object's declared digest.
pub fn prepare_compact_graph_publication_repairing_adjacency(
    parent: &crate::ResolvedProjectGeneration,
    workspace: &Path,
) -> Result<
    (
        crate::ProjectParticipant,
        crate::GraphObjectPublicationLease,
    ),
    GfError,
> {
    let capture =
        crate::graph_files::capture_workspace_over_parent_repairing_adjacency(workspace, parent)?;
    let (participant, lease) = prepare_compact_root(
        parent,
        workspace,
        &capture.inventory,
        Some(&capture.captured),
        true,
    )?;
    Ok((participant, lease))
}

fn prepare_compact_root(
    parent: &crate::ResolvedProjectGeneration,
    workspace: &Path,
    inventory: &GraphFilesInventory,
    captured: Option<&BTreeMap<String, crate::graph_files::CapturedWorkspaceFile>>,
    repair_corrupt_adjacency: bool,
) -> Result<
    (
        crate::ProjectParticipant,
        crate::GraphObjectPublicationLease,
    ),
    GfError,
> {
    // Validate the complete expanded contract before installing one object.
    crate::graph_files::encode_inventory(inventory)?;
    let mut lease = crate::begin_graph_object_publication(parent.container_root())?;
    lease.repair_corrupt_adjacency = repair_corrupt_adjacency;
    let mut state = match parent.declared_graph_files_participant()? {
        Some(crate::GraphFilesParticipant::V2(root)) => {
            crate::graph_object_store::GraphManifestState::open(
                &lease,
                root,
                crate::GraphManifestLimits::default(),
            )?
            .0
        }
        Some(crate::GraphFilesParticipant::V1(_)) | None => {
            crate::graph_object_store::GraphManifestState::empty()
        }
    };
    let (root, _) =
        replace_replayed_graph_files(&lease, workspace, &mut state, inventory, captured)?;
    Ok((
        crate::graph_files::graph_files_root_participant(&root)?,
        lease,
    ))
}

/// Publish a private replay candidate using its authenticated route contract.
fn replace_replayed_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    inventory: &GraphFilesInventory,
    captured: Option<&BTreeMap<String, crate::graph_files::CapturedWorkspaceFile>>,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let changed = inventory
        .files
        .iter()
        .filter(|entry| {
            (lease.repair_corrupt_adjacency
                && entry.relative_path.starts_with("indexes/adjacency/"))
                || state.entries.get(&entry.relative_path).is_none_or(|old| {
                    old.content_sha256 != entry.content_sha256
                        || old.byte_length != entry.byte_length
                })
        })
        .map(|entry| PathBuf::from(&entry.relative_path))
        .collect::<Vec<_>>();
    let tombstones = state
        .entries
        .keys()
        .filter(|path| {
            inventory
                .files
                .binary_search_by(|entry| entry.relative_path.cmp(path))
                .is_err()
        })
        .cloned()
        .collect::<Vec<_>>();
    append_replayed_graph_files(
        lease,
        workspace,
        state,
        inventory,
        &changed,
        &tombstones,
        captured,
    )
}

/// Append selected replay outputs without changing their authenticated route contract.
pub(crate) fn append_replayed_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    inventory: &GraphFilesInventory,
    sealed_paths: &[PathBuf],
    tombstones: &[String],
    captured: Option<&BTreeMap<String, crate::graph_files::CapturedWorkspaceFile>>,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let routes = crate::route_component::authenticate_manifest_routes(
        inventory.format_version,
        &inventory.files,
        |entry| {
            crate::graph_files::read_route_table_counted(workspace, entry).map(|(bytes, _)| bytes)
        },
    )?;
    // The inventory already holds each changed file's digest and length, from
    // the capture that read these exact bytes. Installing authenticates them
    // again as it copies, so a stale claim fails there; passing them only spares
    // a redundant standalone pre-hash of every changed payload.
    let authenticated = sealed_paths
        .iter()
        .map(|path| {
            let name = path
                .to_str()
                .ok_or_else(|| validation("sealed graph path is not UTF-8"))?;
            let index = inventory
                .files
                .binary_search_by(|entry| entry.relative_path.as_str().cmp(name))
                .map_err(|_| validation("sealed graph path is absent from its inventory"))?;
            let entry = &inventory.files[index];
            Ok(AuthenticatedGraphFile {
                relative_path: path.clone(),
                byte_length: entry.byte_length,
                content_sha256: entry.content_sha256.clone(),
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        sealed_paths,
        Some(&authenticated),
        tombstones,
        routes.as_ref(),
        captured.map(CapturedGraphInventory::Workspace),
        &mut || false,
    )
}

/// Seal a verified mapped import into a fresh CAS root, checking exact table closure.
pub(crate) fn append_mapped_import_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    paths: &[PathBuf],
    routes: &crate::route_component::RouteTable,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    append_graph_files_v2_inner(
        lease,
        workspace,
        &mut GraphManifestState::empty(),
        paths,
        None,
        &[],
        Some(routes),
        None,
        &mut || false,
    )
}

/// Publish writer-authenticated files with one copy-and-hash authentication
/// pass. The expected digest is never trusted without that install-time pass.
#[cfg(test)]
pub(crate) fn append_authenticated_graph_files_v2(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_files: &[AuthenticatedGraphFile],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let paths = sealed_files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<_>>();
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        &paths,
        Some(sealed_files),
        tombstones,
        None,
        None,
        &mut || false,
    )
}

/// Append only sources admitted by the owning checkpoint publication boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_captured_mapped_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    artifacts: &[crate::graph_construction_encoding::ConstructionEncodedArtifact],
    captured: &crate::graph_construction::CapturedEncodedInventory<'_>,
    cancelled: &mut impl FnMut() -> bool,
    tombstones: &[String],
    routes: &crate::route_component::RouteTable,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    validate_publication_identity(lease)?;
    if state
        .project_identity
        .is_some_and(|identity| identity != lease.cas.project.identity())
    {
        return Err(validation(
            "graph manifest state belongs to a different project",
        ));
    }
    let mut staged = state.clone();
    let mut migration_io = GraphPublicationIo::default();
    let mut changed = 0_u64;
    if staged
        .root
        .as_ref()
        .is_some_and(|root| !crate::graph_files::root_is_mapped(root.format_version))
    {
        let mut rebuilt = BTreeMap::new();
        let mut expected = crate::route_component::RouteTable::default();
        let mut digest = install_manifest_node(lease, &empty_branch(0), &mut migration_io)?;
        for old in staged.entries.values() {
            let logical = crate::graph_files::legacy_inventory_logical_text(&old.relative_path)?;
            let path = crate::route_component::encode_relative_route(
                &logical,
                &mut expected,
                64 * 1024 * 1024,
                100_000,
            )?;
            if let Some(component) = crate::route_component::route_position(&path)?
                && routes.route(component)? != expected.route(component)?
            {
                return Err(validation(
                    "mapped parent route differs from encoding authority",
                ));
            }
            let mut entry = old.clone();
            entry.relative_path.clone_from(&path);
            if rebuilt.insert(path.clone(), entry.clone()).is_some() {
                return Err(validation("mapped parent paths collide"));
            }
            digest = update_manifest_path(
                lease,
                Some(&digest),
                0,
                &path,
                Some(entry),
                &mut migration_io,
            )?
            .ok_or_else(|| validation("mapped parent root disappeared"))?;
            changed = changed
                .checked_add(1)
                .ok_or_else(|| validation("mapped parent count overflow"))?;
        }
        staged.entries = rebuilt;
        let root = staged.root.as_mut().expect("legacy parent root exists");
        root.root_node_sha256 = digest;
        root.format_version = crate::graph_files::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION;
    }
    let paths = artifacts
        .iter()
        .map(|file| PathBuf::from(&file.path))
        .collect::<Vec<_>>();
    let (root, mut evidence) = append_graph_files_v2_inner(
        lease,
        workspace,
        &mut staged,
        &paths,
        None,
        tombstones,
        Some(routes),
        Some(CapturedGraphInventory::Encoded(captured)),
        cancelled,
    )?;
    evidence.publication_io.checked_add_assign(&migration_io)?;
    let totals = evidence.publication_io.totals()?;
    evidence.bytes_installed = totals.installed_bytes;
    evidence.read_calls = totals.read_calls;
    evidence.write_calls = totals.write_calls;
    evidence.write_bytes = totals.write_bytes;
    evidence.fsync_calls = totals
        .file_fsync_calls
        .checked_add(totals.directory_fsync_calls)
        .ok_or_else(|| validation("mapped synchronization count overflow"))?;
    evidence.changed_entries_examined = evidence
        .changed_entries_examined
        .checked_add(changed)
        .ok_or_else(|| validation("mapped changed entry count overflow"))?;
    *state = staged;
    Ok((root, evidence))
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn append_graph_files_v2_inner(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_paths: &[PathBuf],
    authenticated: Option<&[AuthenticatedGraphFile]>,
    tombstones: &[String],
    mapped_routes: Option<&crate::route_component::RouteTable>,
    captured: Option<CapturedGraphInventory<'_, '_>>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    validate_publication_identity(lease)?;
    if state
        .project_identity
        .is_some_and(|identity| identity != lease.cas.project.identity())
    {
        return Err(validation(
            "graph manifest state belongs to a different project",
        ));
    }
    let mut additions = Vec::with_capacity(sealed_paths.len());
    let mut evidence = GraphFilesAppendEvidence::default();
    let mut sealed_names = BTreeSet::new();
    for relative in sealed_paths {
        validate_logical_path(relative)?;
        let name = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?;
        if !sealed_names.insert(name.to_owned()) {
            return Err(validation("sealed graph paths contain a duplicate"));
        }
    }
    let mut tombstone_names = BTreeSet::new();
    for path in tombstones {
        validate_logical_path(Path::new(path))?;
        if !tombstone_names.insert(path.clone()) {
            return Err(validation("graph tombstones contain a duplicate"));
        }
        if sealed_names.contains(path) {
            return Err(validation("graph path is both sealed and tombstoned"));
        }
    }
    let mut logical_byte_length = state
        .root
        .as_ref()
        .map_or(0, |root| root.logical_byte_length);
    for (index, relative) in sealed_paths.iter().enumerate() {
        let source = workspace.join(relative);
        let portable = match captured {
            Some(CapturedGraphInventory::Portable(authorities)) => {
                authorities.get(&source).copied()
            }
            _ => None,
        };
        let workspace_capture = match captured {
            Some(CapturedGraphInventory::Workspace(files)) => {
                relative.to_str().and_then(|name| files.get(name))
            }
            _ => None,
        };
        let (digest, expected_length, prehash_io) =
            if let Some(CapturedGraphInventory::Encoded(captured)) = captured {
                let source = captured.open(relative)?;
                (
                    source.content_sha256().to_owned(),
                    source.bytes(),
                    ReadIoEvidence::default(),
                )
            } else if let Some(capture) = workspace_capture {
                (
                    capture.content_sha256().to_owned(),
                    capture.bytes(),
                    ReadIoEvidence::default(),
                )
            } else if let Some(files) = authenticated {
                let expected = files
                    .get(index)
                    .ok_or_else(|| validation("authenticated graph inventory is incomplete"))?;
                if expected.relative_path != *relative {
                    return Err(validation("authenticated graph file metadata changed"));
                }
                validate_digest(&expected.content_sha256)?;
                (
                    expected.content_sha256.clone(),
                    expected.byte_length,
                    ReadIoEvidence::default(),
                )
            } else {
                let metadata = fs::symlink_metadata(&source)
                    .map_err(|error| storage("inspect sealed graph file", &source, error))?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(validation("sealed graph path is not a regular file"));
                }
                let (digest, io) = hash_regular_file(&source)?;
                (hex_digest(digest), metadata.len(), io)
            };
        let installed = if let Some(CapturedGraphInventory::Encoded(captured)) = captured {
            let source = captured.open(relative)?;
            super::install_captured_encoded_artifact_with_lease(lease, &source, cancelled)?
        } else if let Some(capture) = portable {
            let source = capture
                .open_source(&source)
                .map_err(|error| validation(error.to_string()))?;
            if source.content_sha256() != digest || source.bytes() != expected_length {
                return Err(validation(
                    "portable graph capture disagrees with file metadata",
                ));
            }
            super::install_captured_portable_source_with_lease(lease, &source, cancelled)?
        } else if let Some(capture) = workspace_capture {
            let repair_corrupt_adjacency = lease.repair_corrupt_adjacency
                && relative
                    .to_str()
                    .is_some_and(|path| path.starts_with("indexes/adjacency/"));
            super::install_captured_workspace_file_with_lease(
                lease,
                capture,
                repair_corrupt_adjacency,
                cancelled,
            )?
        } else {
            install_graph_object_file_with_lease(lease, &source, &digest, expected_length)?
        };
        evidence.payload_bytes_hashed = evidence
            .payload_bytes_hashed
            .checked_add(installed.bytes_hashed)
            .and_then(|bytes| bytes.checked_add(prehash_io.bytes))
            .ok_or_else(|| validation("graph payload SHA bytes overflow"))?;
        evidence.publication_io.payload.add_install(&installed)?;
        evidence.publication_io.payload.add_read(prehash_io)?;
        let relative_path = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?
            .to_owned();
        let entry = crate::GraphFileEntry {
            content_xxh64: installed.content_xxh64.ok_or_else(|| {
                validation("graph object installation omitted its payload checksum")
            })?,
            relative_path: relative_path.clone(),
            byte_length: expected_length,
            content_sha256: digest,
            role: crate::graph_files::infer_role(relative),
        };
        if let Some(previous) = state.entries.get(&relative_path) {
            logical_byte_length = logical_byte_length
                .checked_sub(previous.byte_length)
                .ok_or_else(|| validation("graph files v2 logical byte total underflows"))?;
        }
        logical_byte_length = logical_byte_length
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph files v2 byte total overflow"))?;
        additions.push(entry);
    }
    additions.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut tombstones = tombstones.to_vec();
    tombstones.sort();
    for path in &tombstones {
        if let Some(previous) = state.entries.get(path) {
            logical_byte_length = logical_byte_length
                .checked_sub(previous.byte_length)
                .ok_or_else(|| validation("graph files v2 logical byte total underflows"))?;
        }
    }
    if let Some(routes) = mapped_routes {
        let mut final_entries = state.entries.clone();
        for entry in &additions {
            final_entries.insert(entry.relative_path.clone(), entry.clone());
        }
        for path in &tombstones {
            final_entries.remove(path);
        }
        routes.validate_paths(final_entries.keys().map(String::as_str))?;
        let table = routes.encode(64 * 1024 * 1024)?;
        let authority = final_entries
            .get(crate::route_component::TABLE_FILE)
            .ok_or_else(|| validation("mapped publication lacks route authority"))?;
        if authority.byte_length != table.len() as u64
            || authority.content_sha256 != hex_digest(Sha256::digest(&table).into())
        {
            return Err(validation(
                "mapped publication route authority differs from sealed table",
            ));
        }
    }
    // All payload objects are authenticated before any manifest node can
    // reference them. A returned error here therefore leaves only unreferenced,
    // retry-safe CAS state.
    returned_error_boundary("append:before-manifest-reference")?;
    evidence.changed_entries_examined = u64::try_from(
        additions
            .len()
            .checked_add(tombstones.len())
            .ok_or_else(|| validation("graph files v2 changed-entry count overflow"))?,
    )
    .map_err(|_| validation("graph files v2 changed-entry count exceeds u64"))?;
    evidence.publication_io.publications = 1;
    evidence.publication_io.initial_entries = u64::try_from(state.entries.len())
        .map_err(|_| validation("graph files v2 prior-entry count exceeds u64"))?;
    evidence.publication_io.changed_paths = evidence.changed_entries_examined;
    let mut root_digest = match state.root.as_ref() {
        Some(previous) => previous.root_node_sha256.clone(),
        _ => install_manifest_node(lease, &empty_branch(0), &mut evidence.publication_io)?,
    };
    for entry in &additions {
        let relative_path = entry.relative_path.clone();
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &relative_path,
            Some(entry.clone()),
            &mut evidence.publication_io,
        )?
        .ok_or_else(|| validation("radix update unexpectedly removed the root"))?;
    }
    for path in &tombstones {
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            path,
            None,
            &mut evidence.publication_io,
        )?
        .unwrap_or(install_manifest_node(
            lease,
            &empty_branch(0),
            &mut evidence.publication_io,
        )?);
    }
    let added_new = additions
        .iter()
        .filter(|entry| !state.entries.contains_key(&entry.relative_path))
        .count();
    let removed_existing = tombstones
        .iter()
        .filter(|path| state.entries.contains_key(path.as_str()))
        .count();
    let logical_file_count = state
        .entries
        .len()
        .checked_add(added_new)
        .and_then(|count| count.checked_sub(removed_existing))
        .ok_or_else(|| validation("graph files v2 file total overflow"))?;
    let root = GraphFilesRootV2 {
        format: GRAPH_FILES_V2_FORMAT.into(),
        format_version: if mapped_routes.is_some() {
            crate::graph_files::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
        } else {
            crate::graph_files::GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION
        },
        root_node_sha256: root_digest,
        logical_file_count: u64::try_from(logical_file_count)
            .map_err(|_| validation("graph files v2 logical file count exceeds u64"))?,
        logical_byte_length,
    };
    let totals = evidence.publication_io.totals()?;
    evidence.bytes_installed = totals.installed_bytes;
    evidence.read_calls = totals.read_calls;
    evidence.write_calls = totals.write_calls;
    evidence.write_bytes = totals.write_bytes;
    evidence.fsync_calls = totals
        .file_fsync_calls
        .checked_add(totals.directory_fsync_calls)
        .ok_or_else(|| validation("CAS synchronization total overflows"))?;
    // Commit the root-bound cache only after every payload and Patricia node
    // operation succeeded. An error above leaves both fields unchanged.
    for entry in additions {
        state.entries.insert(entry.relative_path.clone(), entry);
    }
    for path in tombstones {
        state.entries.remove(&path);
    }
    state.root = Some(root.clone());
    state.project_identity = Some(lease.cas.project.identity());
    Ok((root, evidence))
}

/// Convert a current expanded graph inventory into a self-contained compact radix root.
pub fn compact_graph_files(
    lease: &GraphObjectPublicationLease,
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<(GraphFilesRootV2, GraphFilesMigrationEvidence), GfError> {
    // Authenticate the complete expanded contract before installing even one
    // payload object; malformed caller-owned structs cannot create partial CAS.
    crate::encode_inventory(inventory)?;
    validate_publication_identity(lease)?;
    let mut evidence = GraphFilesMigrationEvidence::default();
    let mut installed_checksums = BTreeMap::new();
    for entry in &inventory.files {
        let source = crate::graph_files::resolve_v1_inventory_entry(graph_root, entry)?;
        let installed = install_graph_object_file_with_lease(
            lease,
            &source,
            &entry.content_sha256,
            entry.byte_length,
        )?;
        installed_checksums.insert(
            entry.relative_path.clone(),
            installed.content_xxh64.ok_or_else(|| {
                validation("graph object installation omitted its payload checksum")
            })?,
        );
        evidence.payload_objects = evidence
            .payload_objects
            .checked_add(1)
            .ok_or_else(|| validation("CAS payload object count overflows"))?;
        evidence.payload_bytes_hashed = evidence
            .payload_bytes_hashed
            .checked_add(installed.bytes_hashed)
            .ok_or_else(|| validation("CAS payload byte count overflows"))?;
        evidence.bytes_installed = evidence
            .bytes_installed
            .checked_add(installed.bytes_installed)
            .ok_or_else(|| validation("CAS installed byte count overflows"))?;
    }
    let mut publication_io = GraphPublicationIo::default();
    let mut root_digest = install_manifest_node(lease, &empty_branch(0), &mut publication_io)?;
    for entry in &inventory.files {
        let mut canonical_entry = entry.clone();
        canonical_entry.content_xxh64 = installed_checksums[&entry.relative_path];
        canonical_entry.relative_path =
            crate::graph_files::canonical_inventory_relative_text(&entry.relative_path)?;
        let canonical_path = canonical_entry.relative_path.clone();
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &canonical_path,
            Some(canonical_entry),
            &mut publication_io,
        )?
        .ok_or_else(|| validation("radix migration unexpectedly removed the root"))?;
    }
    evidence.bytes_installed = evidence
        .bytes_installed
        .checked_add(publication_io.manifest.installed_bytes)
        .ok_or_else(|| validation("migration manifest bytes overflow"))?;
    Ok((
        GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: if crate::graph_files::inventory_is_mapped(inventory.format_version) {
                crate::graph_files::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
            } else {
                crate::graph_files::GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION
            },
            root_node_sha256: root_digest,
            logical_file_count: inventory.file_count,
            logical_byte_length: inventory.total_byte_length,
        },
        evidence,
    ))
}

fn empty_branch(depth: u8) -> GraphManifestNode {
    GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: GRAPH_MANIFEST_NODE_VERSION,
        depth,
        prefix: String::new(),
        kind: GraphManifestNodeKind::Branch {
            children: BTreeMap::new(),
        },
    }
}

fn install_manifest_node(
    lease: &GraphObjectPublicationLease,
    node: &GraphManifestNode,
    publication_io: &mut GraphPublicationIo,
) -> Result<String, GfError> {
    let (digest, evidence) = install_graph_manifest_node_with_lease(lease, node)?;
    publication_io.manifest.add_install(&evidence)?;
    Ok(digest)
}

fn load_manifest_node(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    expected_depth: u8,
    publication_io: &mut GraphPublicationIo,
) -> Result<GraphManifestNode, GfError> {
    let (bytes, io) = read_graph_object_by_digest_file_counted_in_domain(
        lease.cas.open_digest(digest)?,
        digest,
        crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
        &lease.cas.diagnostic_root,
        graphforge_core::hash_observation::HashDomain::ControlAuthentication,
    )?;
    publication_io.manifest_reads.add_read(io)?;
    let node = crate::decode_graph_manifest_node(&bytes)?;
    if node.depth != expected_depth {
        return Err(validation(
            "graph manifest radix depth mismatch during update",
        ));
    }
    Ok(node)
}

fn update_manifest_path(
    lease: &GraphObjectPublicationLease,
    current_digest: Option<&str>,
    depth: u8,
    path: &str,
    replacement: Option<crate::GraphFileEntry>,
    publication_io: &mut GraphPublicationIo,
) -> Result<Option<String>, GfError> {
    let path_digest = hex_digest(crate::graph_manifest::logical_path_digest(path));
    update_manifest_digest(
        lease,
        current_digest,
        depth,
        path,
        &path_digest,
        replacement,
        publication_io,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn update_manifest_digest(
    lease: &GraphObjectPublicationLease,
    current_digest: Option<&str>,
    depth: u8,
    path: &str,
    path_digest: &str,
    replacement: Option<crate::GraphFileEntry>,
    publication_io: &mut GraphPublicationIo,
) -> Result<Option<String>, GfError> {
    let Some(current_digest) = current_digest else {
        return replacement
            .map(|entry| install_bucket_entries(lease, depth, vec![entry], publication_io))
            .transpose();
    };
    let mut node = load_manifest_node(lease, current_digest, depth, publication_io)?;
    // A small bucket absorbs a divergent path before any prefix split. Both
    // replacement and deletion recompute its maximal compressed prefix.
    if let GraphManifestNodeKind::Bucket { mut entries } = node.kind {
        for entry in &entries {
            if !hex_digest(crate::graph_manifest::logical_path_digest(
                &entry.relative_path,
            ))
            .starts_with(&path_digest[..usize::from(depth)])
            {
                return Err(validation(
                    "manifest bucket ancestral route mismatch during update",
                ));
            }
        }
        match entries.binary_search_by(|entry| entry.relative_path.as_str().cmp(path)) {
            Ok(index) => match replacement {
                Some(entry) => entries[index] = entry,
                None => {
                    entries.remove(index);
                }
            },
            Err(index) => {
                if let Some(entry) = replacement {
                    entries.insert(index, entry);
                } else {
                    return Ok(Some(current_digest.to_owned()));
                }
            }
        }
        return if entries.is_empty() {
            Ok(None)
        } else {
            install_bucket_entries(lease, depth, entries, publication_io).map(Some)
        };
    }
    let start = usize::from(depth);
    let common = node
        .prefix
        .bytes()
        .zip(path_digest.as_bytes()[start..].iter().copied())
        .take_while(|(left, right)| left == right)
        .count();
    if common != node.prefix.len() {
        let Some(entry) = replacement else {
            return Ok(Some(current_digest.to_owned()));
        };
        let split_depth = depth
            .checked_add(u8::try_from(common).map_err(|_| validation("Patricia split overflow"))?)
            .ok_or_else(|| validation("Patricia split overflow"))?;
        let old_edge = node.prefix[common..=common].to_owned();
        node.depth = split_depth + 1;
        node.prefix = node.prefix[common + 1..].to_owned();
        let old_digest = install_manifest_node(lease, &node, publication_io)?;
        let new_edge = path_digest[usize::from(split_depth)..=usize::from(split_depth)].to_owned();
        if old_edge == new_edge {
            return Err(validation("Patricia split did not diverge"));
        }
        let new_digest = install_manifest_node(
            lease,
            &bucket_node(split_depth + 1, vec![entry])?,
            publication_io,
        )?;
        let children = BTreeMap::from([(old_edge, old_digest), (new_edge, new_digest)]);
        return install_manifest_node(
            lease,
            &branch_node(depth, &path_digest[start..start + common], children),
            publication_io,
        )
        .map(Some);
    }
    let payload_depth = depth
        .checked_add(
            u8::try_from(node.prefix.len()).map_err(|_| validation("Patricia depth overflow"))?,
        )
        .ok_or_else(|| validation("Patricia depth overflow"))?;
    match node.kind {
        GraphManifestNodeKind::Bucket { .. } => {
            unreachable!("buckets handled before prefix splitting")
        }
        GraphManifestNodeKind::Branch { mut children } => {
            if payload_depth >= GRAPH_RADIX_DEPTH {
                return Err(validation("Patricia branch exceeds digest route"));
            }
            let edge =
                path_digest[usize::from(payload_depth)..=usize::from(payload_depth)].to_owned();
            let deleting = replacement.is_none();
            let child = update_manifest_digest(
                lease,
                children.get(&edge).map(String::as_str),
                payload_depth + 1,
                path,
                path_digest,
                replacement,
                publication_io,
            )?;
            match child {
                Some(digest) => {
                    children.insert(edge, digest);
                }
                None => {
                    children.remove(&edge);
                }
            }
            match children.len() {
                0 => Ok(None),
                1 => {
                    let (edge, child_digest) = children.into_iter().next().expect("one child");
                    let mut child = load_manifest_node(
                        lease,
                        &child_digest,
                        payload_depth + 1,
                        publication_io,
                    )?;
                    child.depth = depth;
                    child.prefix = format!("{}{}{}", node.prefix, edge, child.prefix);
                    install_manifest_node(lease, &child, publication_io).map(Some)
                }
                _ => {
                    if deleting
                        && let Some(entries) = collect_small_subtree(
                            lease,
                            &children,
                            payload_depth + 1,
                            &path_digest[..usize::from(payload_depth)],
                            publication_io,
                        )?
                    {
                        return install_bucket_entries(lease, depth, entries, publication_io)
                            .map(Some);
                    }
                    install_manifest_node(
                        lease,
                        &branch_node(depth, &node.prefix, children),
                        publication_io,
                    )
                    .map(Some)
                }
            }
        }
    }
}

fn branch_node(depth: u8, prefix: &str, children: BTreeMap<String, String>) -> GraphManifestNode {
    GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: GRAPH_MANIFEST_NODE_VERSION,
        depth,
        prefix: prefix.to_owned(),
        kind: GraphManifestNodeKind::Branch { children },
    }
}

fn bucket_node(
    depth: u8,
    entries: Vec<crate::GraphFileEntry>,
) -> Result<GraphManifestNode, GfError> {
    Ok(GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: crate::graph_manifest::GRAPH_MANIFEST_CHECKSUM_NODE_VERSION,
        depth,
        prefix: crate::graph_manifest::bucket_prefix(depth, &entries)?,
        kind: GraphManifestNodeKind::Bucket { entries },
    })
}

// Only a formerly bounded bucket (at most nine entries after insertion) reaches
// this builder. It never reconstructs the surrounding manifest.
fn install_bucket_entries(
    lease: &GraphObjectPublicationLease,
    depth: u8,
    entries: Vec<crate::GraphFileEntry>,
    publication_io: &mut GraphPublicationIo,
) -> Result<String, GfError> {
    let capacity = crate::graph_manifest::GRAPH_MANIFEST_BUCKET_CAPACITY;
    if entries.is_empty() || entries.len() > capacity + 1 {
        return Err(validation("manifest bucket split input exceeds bound"));
    }
    if entries.len() <= capacity {
        return install_manifest_node(lease, &bucket_node(depth, entries)?, publication_io);
    }
    let prefix = crate::graph_manifest::bucket_prefix(depth, &entries)?;
    let split_depth = usize::from(depth) + prefix.len();
    if split_depth >= usize::from(GRAPH_RADIX_DEPTH) {
        return Err(validation(
            "manifest hash collision exceeds bucket capacity",
        ));
    }
    let mut groups = BTreeMap::<String, Vec<crate::GraphFileEntry>>::new();
    for entry in entries {
        let digest = hex_digest(crate::graph_manifest::logical_path_digest(
            &entry.relative_path,
        ));
        groups
            .entry(digest[split_depth..=split_depth].to_owned())
            .or_default()
            .push(entry);
    }
    let child_depth = u8::try_from(split_depth + 1)
        .map_err(|_| validation("manifest bucket split depth overflow"))?;
    let mut children = BTreeMap::new();
    for (edge, entries) in groups {
        children.insert(
            edge,
            install_bucket_entries(lease, child_depth, entries, publication_io)?,
        );
    }
    install_manifest_node(
        lease,
        &branch_node(depth, &prefix, children),
        publication_io,
    )
}

// Collect a small child forest without assuming that an authenticated branch
// necessarily has more than eight descendants. Each pending nonempty node
// contributes at least one entry. Stop as soon as that lower bound exceeds
// eight; non-unary branches therefore permit at most sixteen node visits.
fn collect_small_subtree(
    lease: &GraphObjectPublicationLease,
    children: &BTreeMap<String, String>,
    depth: u8,
    parent_route: &str,
    publication_io: &mut GraphPublicationIo,
) -> Result<Option<Vec<crate::GraphFileEntry>>, GfError> {
    let capacity = crate::graph_manifest::GRAPH_MANIFEST_BUCKET_CAPACITY;
    if children.len() > capacity {
        return Ok(None);
    }
    let mut pending = children
        .iter()
        .map(|(edge, digest)| (digest.clone(), depth, format!("{parent_route}{edge}")))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(capacity);
    let mut visited = BTreeSet::new();
    while let Some((digest, expected_depth, route)) = pending.pop() {
        if visited.len() >= 2 * capacity || !visited.insert(digest.clone()) {
            return Err(validation(
                "manifest collapse node bound or uniqueness violated",
            ));
        }
        let node = load_manifest_node(lease, &digest, expected_depth, publication_io)?;
        let node_route = format!("{route}{}", node.prefix);
        match node.kind {
            GraphManifestNodeKind::Bucket { entries: bucket } => {
                if entries.len() + pending.len() + bucket.len() > capacity {
                    return Ok(None);
                }
                for entry in bucket {
                    if !hex_digest(crate::graph_manifest::logical_path_digest(
                        &entry.relative_path,
                    ))
                    .starts_with(&node_route)
                    {
                        return Err(validation(
                            "manifest collapse bucket ancestral route mismatch",
                        ));
                    }
                    entries.push(entry);
                }
            }
            GraphManifestNodeKind::Branch { children } => {
                if entries.len() + pending.len() + children.len() > capacity {
                    return Ok(None);
                }
                let child_depth = u8::try_from(node_route.len() + 1)
                    .map_err(|_| validation("manifest collapse depth overflow"))?;
                pending.extend(
                    children
                        .into_iter()
                        .map(|(edge, digest)| (digest, child_depth, format!("{node_route}{edge}"))),
                );
            }
        }
    }
    entries.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(Some(entries))
}

#[cfg(test)]
mod tests;
