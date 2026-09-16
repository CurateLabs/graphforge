//! manifest tree ownership for immutable graph objects.

use super::AuthenticatedGraphFile;
use super::BTreeMap;
use super::BTreeSet;
use super::Digest;
use super::GRAPH_FILES_V2_FORMAT;
use super::GRAPH_FILES_V2_VERSION;
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
use super::Sha256;
use super::fs;
use super::hash_regular_file;
use super::hex_digest;
use super::install_graph_object_bytes_with_lease;
use super::install_graph_object_file_with_lease;
use super::read_graph_object_by_digest_file_counted;
use super::returned_error_boundary;
use super::storage;
use super::validate_digest;
use super::validate_logical_path;
use super::validate_publication_identity;
use super::validation;

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
            let (bytes, io) = read_graph_object_by_digest_file_counted(
                lease.cas.open_digest(digest)?,
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                &lease.cas.diagnostic_root,
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
                let (bytes, io) = read_graph_object_by_digest_file_counted(
                    lease.cas.open_digest(&entry.content_sha256)?,
                    &entry.content_sha256,
                    64 * 1024 * 1024,
                    &lease.cas.diagnostic_root,
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
    )
}

/// Prepare an authenticated canonical candidate while preserving its parent ownership.
/// CAS parents reuse unchanged objects; generation-owned parents retain their
/// existing graph-tree publication path. Keep the returned lease through CURRENT.
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
        Option<crate::GraphObjectPublicationLease>,
    ),
    GfError,
> {
    let mut files_participant = crate::graph_files::inventory_participant(
        crate::graph_files::encode_inventory(inventory)?,
        inventory.file_count,
    )?;
    let publication_lease = match parent.declared_graph_files_participant()? {
        Some(crate::GraphFilesParticipant::V2(root)) => {
            let lease = crate::begin_graph_object_publication(parent.container_root())?;
            let (mut state, _) = crate::graph_object_store::GraphManifestState::open(
                &lease,
                root,
                crate::GraphManifestLimits::default(),
            )?;
            let (root, _) = crate::graph_object_store::replace_replayed_graph_files(
                &lease, workspace, &mut state, inventory,
            )?;
            files_participant = crate::graph_files::graph_files_root_participant(&root)?;
            Some(lease)
        }
        _ => None,
    };
    Ok((files_participant, publication_lease))
}

/// Publish a private replay candidate using its authenticated route contract.
pub(crate) fn replace_replayed_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    inventory: &GraphFilesInventory,
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let changed = inventory
        .files
        .iter()
        .filter(|entry| {
            state.entries.get(&entry.relative_path).is_none_or(|old| {
                old.content_sha256 != entry.content_sha256 || old.byte_length != entry.byte_length
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
    append_replayed_graph_files(lease, workspace, state, inventory, &changed, &tombstones)
}

/// Append selected replay outputs without changing their authenticated route contract.
pub(crate) fn append_replayed_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    inventory: &GraphFilesInventory,
    sealed_paths: &[PathBuf],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let routes = crate::route_component::authenticate_manifest_routes(
        inventory.format_version,
        &inventory.files,
        |entry| {
            crate::graph_files::read_route_table_counted(workspace, entry).map(|(bytes, _)| bytes)
        },
    )?;
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        sealed_paths,
        None,
        tombstones,
        routes.as_ref(),
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
    )
}

/// Publish writer-authenticated files with one copy-and-hash authentication
/// pass. The expected digest is never trusted without that install-time pass.
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
    )
}

/// Append a checkpoint-authorized mapped encoding, retaining legacy payload objects by digest.
pub(crate) fn append_authenticated_mapped_graph_files(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_files: &[AuthenticatedGraphFile],
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
        .is_some_and(|root| root.format_version == GRAPH_FILES_V2_VERSION)
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
        root.format_version = crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION;
    }
    let paths = sealed_files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<_>>();
    let (root, mut evidence) = append_graph_files_v2_inner(
        lease,
        workspace,
        &mut staged,
        &paths,
        Some(sealed_files),
        tombstones,
        Some(routes),
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

#[allow(clippy::too_many_lines)]
fn append_graph_files_v2_inner(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_paths: &[PathBuf],
    authenticated: Option<&[AuthenticatedGraphFile]>,
    tombstones: &[String],
    mapped_routes: Option<&crate::route_component::RouteTable>,
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
        let (digest, expected_length, prehash_io) = if let Some(files) = authenticated {
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
        let installed =
            install_graph_object_file_with_lease(lease, &source, &digest, expected_length)?;
        evidence.publication_io.payload.add_install(&installed)?;
        evidence.publication_io.payload.add_read(prehash_io)?;
        let relative_path = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?
            .to_owned();
        let entry = crate::GraphFileEntry {
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
        None => install_manifest_node(lease, &empty_branch(0), &mut evidence.publication_io)?,
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
            crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        } else {
            GRAPH_FILES_V2_VERSION
        },
        root_node_sha256: root_digest,
        logical_file_count: u64::try_from(logical_file_count)
            .map_err(|_| validation("graph files v2 logical file count exceeds u64"))?,
        logical_byte_length,
    };
    let totals = evidence.publication_io.totals()?;
    evidence.payload_bytes_hashed = evidence.publication_io.payload.read_bytes;
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

/// Import a verified v1 graph tree into a self-contained v2 radix root.
pub fn migrate_graph_files_v1_to_v2(
    lease: &GraphObjectPublicationLease,
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<(GraphFilesRootV2, GraphFilesMigrationEvidence), GfError> {
    // Authenticate the complete expanded contract before installing even one
    // payload object; malformed caller-owned structs cannot create partial CAS.
    crate::encode_inventory(inventory)?;
    validate_publication_identity(lease)?;
    let mut evidence = GraphFilesMigrationEvidence::default();
    for entry in &inventory.files {
        let source = crate::graph_files::resolve_v1_inventory_entry(graph_root, entry)?;
        let installed = install_graph_object_file_with_lease(
            lease,
            &source,
            &entry.content_sha256,
            entry.byte_length,
        )?;
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
            format_version: GRAPH_FILES_V2_VERSION,
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
    let bytes = crate::encode_graph_manifest_node(node)?;
    let (digest, evidence) = install_graph_object_bytes_with_lease(lease, &bytes)?;
    publication_io.manifest.add_install(&evidence)?;
    Ok(digest)
}

fn load_manifest_node(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    expected_depth: u8,
    publication_io: &mut GraphPublicationIo,
) -> Result<GraphManifestNode, GfError> {
    let (bytes, io) = read_graph_object_by_digest_file_counted(
        lease.cas.open_digest(digest)?,
        digest,
        crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
        &lease.cas.diagnostic_root,
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
        format_version: GRAPH_MANIFEST_NODE_VERSION,
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
