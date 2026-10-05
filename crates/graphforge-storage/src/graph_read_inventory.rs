//! Checksum authority for reads and private replay workspaces. This type is not
//! a persisted participant and cannot name a CAS object or publish a generation.

use std::collections::BTreeSet;
use std::io::{Read, Seek};
use std::path::Path;

use graphforge_core::GfError;
use graphforge_core::hash_observation::ControlSha256;
use sha2::Digest;

use crate::{GraphFileEntry, GraphFileRole, GraphFilesInventory};

/// Exact corruption-checking metadata, without a cryptographic payload name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphReadFileEntry {
    /// Canonical relative graph path.
    pub relative_path: String,
    /// Exact payload length.
    pub byte_length: u64,
    /// Seed-zero XXH64 corruption checksum.
    pub content_xxh64: u64,
    /// Logical file role.
    pub role: GraphFileRole,
}

impl From<&GraphFileEntry> for GraphReadFileEntry {
    fn from(entry: &GraphFileEntry) -> Self {
        Self {
            relative_path: entry.relative_path.clone(),
            byte_length: entry.byte_length,
            content_xxh64: entry.content_xxh64,
            role: entry.role,
        }
    }
}

/// Non-persistable read authority derived from a published or private tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphReadInventory {
    /// Raw checksum layout (5) or mapped checksum layout (7).
    pub format_version: u32,
    /// Canonically ordered exact files.
    pub files: Vec<GraphReadFileEntry>,
    /// Canonical paths admitted by enumeration, including unauthenticated
    /// payloads in a route-scoped workspace capture.
    pub(crate) path_names: Vec<String>,
    /// Whether this authority came from a targeted property mutation refresh.
    property_route_scoped: bool,
    /// Number of declared files.
    pub file_count: u64,
    /// Sum of declared file lengths.
    pub total_byte_length: u64,
    route_table_sha256: Option<String>,
    authority_read_calls: u64,
    authority_read_bytes: u64,
}

impl GraphReadInventory {
    /// Project existing authenticated publication metadata into read authority.
    /// No payload bytes are read and CAS names remain in the published inventory.
    ///
    /// # Errors
    /// Refuses unsupported formats and invalid published inventory contracts.
    pub fn from_published(inventory: &GraphFilesInventory) -> Result<Self, GfError> {
        let mut normalized = inventory.clone();
        normalized.format_version = match inventory.format_version {
            5 | 6 => 5,
            7 | 8 => 7,
            _ => return Err(invalid("unsupported graph read inventory format")),
        };
        crate::graph_files::validate_inventory_contract(&normalized)?;
        Ok(Self {
            format_version: normalized.format_version,
            files: inventory
                .files
                .iter()
                .map(GraphReadFileEntry::from)
                .collect(),
            path_names: inventory
                .files
                .iter()
                .map(|entry| entry.relative_path.clone())
                .collect(),
            property_route_scoped: false,
            file_count: inventory.file_count,
            total_byte_length: inventory.total_byte_length,
            route_table_sha256: inventory
                .files
                .iter()
                .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
                .map(|entry| entry.content_sha256.clone()),
            authority_read_calls: 0,
            authority_read_bytes: 0,
        })
    }

    /// Actual nonempty reads made while capturing this private inventory.
    #[must_use]
    pub const fn authority_read_calls(&self) -> u64 {
        self.authority_read_calls
    }

    /// Actual payload bytes read while capturing this authority.
    #[must_use]
    pub const fn authority_read_bytes(&self) -> u64 {
        self.authority_read_bytes
    }

    /// Length of the authenticated semantic route table, if this layout has one.
    #[must_use]
    pub(crate) fn route_table_byte_length(&self) -> u64 {
        self.files
            .iter()
            .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
            .map_or(0, |entry| entry.byte_length)
    }

    pub(crate) fn agrees_with(&self, other: &Self) -> bool {
        self.format_version == other.format_version
            && self.files == other.files
            && self.file_count == other.file_count
            && self.total_byte_length == other.total_byte_length
            && self.route_table_sha256 == other.route_table_sha256
            && self.path_names == other.path_names
    }

    pub(crate) fn authenticate_routes(
        &self,
        root: &Path,
    ) -> Result<Option<crate::route_component::RouteTable>, GfError> {
        let entry = self
            .files
            .iter()
            .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE);
        match (self.format_version, entry) {
            (5, None) => Ok(None),
            (7, Some(entry)) => {
                const LIMIT: u64 = 64 * 1024 * 1024;
                if entry.byte_length > LIMIT {
                    return Err(invalid("semantic route table byte budget exceeded"));
                }
                let mut retained = resolve_entry_retained(root, entry)?;
                retained.file.rewind().map_err(|error| io_error(&error))?;
                let mut bytes = Vec::new();
                let mut read_calls = 0_u64;
                if self.property_route_scoped {
                    let mut buffer = vec![0_u8; 64 * 1024];
                    let mut reader = retained.file.by_ref().take(LIMIT + 1);
                    loop {
                        let read = reader.read(&mut buffer).map_err(|error| io_error(&error))?;
                        if read == 0 {
                            break;
                        }
                        read_calls = read_calls
                            .checked_add(1)
                            .ok_or_else(|| invalid("route control read call count overflow"))?;
                        bytes.extend_from_slice(&buffer[..read]);
                    }
                    crate::lifecycle_io::record_read(
                        crate::StorageIoPhase::PropertyMutationRouteAuthority,
                        bytes.len() as u64,
                        read_calls,
                    );
                } else {
                    retained
                        .file
                        .take(LIMIT + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|error| io_error(&error))?;
                }
                let expected = self
                    .route_table_sha256
                    .as_deref()
                    .ok_or_else(|| invalid("mapped read inventory lacks control authentication"))?;
                if bytes.len() as u64 != entry.byte_length
                    || hex(&ControlSha256::digest(&bytes)) != expected
                {
                    return Err(invalid("semantic route table authentication mismatch"));
                }
                let table = crate::route_component::RouteTable::decode(&bytes, LIMIT, 100_000)?;
                table.validate_paths(self.path_names.iter().map(String::as_str))?;
                Ok(Some(table))
            }
            _ => Err(invalid("graph read inventory route layout mismatch")),
        }
    }
}

/// Capture strict private-workspace paths, lengths, and actual checksums.
/// Only the bounded semantic route control file retains its SHA authentication.
///
/// # Errors
/// Refuses unsafe or ambiguous paths, changed files, excess work, and I/O failures.
pub fn capture_graph_read_inventory(root: &Path) -> Result<GraphReadInventory, GfError> {
    capture_graph_read_inventory_excluding(root, &std::collections::BTreeMap::new(), None)
}

/// Capture read authority for a private tree while a rewrite retains staged
/// temporaries in it. Each excluded path must keep its exact file identity
/// for the whole capture; any other unregistered file is refused as usual.
#[allow(clippy::too_many_lines)]
pub(crate) fn capture_graph_read_inventory_excluding(
    root: &Path,
    excluded: &Exclusions,
    topology: Option<&crate::TopologyFiles>,
) -> Result<GraphReadInventory, GfError> {
    capture_graph_read_inventory_impl(root, excluded, topology, None)
}

/// Capture the complete workspace path set while checksumming only the
/// property routes required by this mutation and the semantic route table.
pub(crate) fn capture_graph_read_inventory_for_property_routes(
    root: &Path,
    topology: &crate::TopologyFiles,
    routes: &std::collections::BTreeSet<(crate::PropertyRouteKind, String)>,
) -> Result<GraphReadInventory, GfError> {
    capture_graph_read_inventory_impl(
        root,
        &std::collections::BTreeMap::new(),
        Some(topology),
        Some(routes),
    )
}

#[allow(clippy::too_many_lines)]
fn capture_graph_read_inventory_impl(
    root: &Path,
    excluded: &Exclusions,
    topology: Option<&crate::TopologyFiles>,
    selected_routes: Option<&std::collections::BTreeSet<(crate::PropertyRouteKind, String)>>,
) -> Result<GraphReadInventory, GfError> {
    let paths = paths_outside_exclusions(root, excluded, topology)?;
    if paths.len() > crate::graph_files::MAX_GRAPH_FILES {
        return Err(invalid("graph read file count exceeds limit"));
    }
    let mapped = paths.iter().any(|path| {
        path.strip_prefix(root).ok() == Some(Path::new(crate::route_component::TABLE_FILE))
    });
    let mut inventory = GraphReadInventory {
        format_version: if mapped { 7 } else { 5 },
        files: Vec::new(),
        path_names: Vec::new(),
        property_route_scoped: selected_routes.is_some(),
        file_count: 0,
        total_byte_length: 0,
        route_table_sha256: None,
        authority_read_calls: 0,
        authority_read_bytes: 0,
    };
    let mut destinations = BTreeSet::new();
    let mut paths = paths;
    if selected_routes.is_some() {
        paths.sort_by_key(|path| {
            path.strip_prefix(root).ok() != Some(Path::new(crate::route_component::TABLE_FILE))
        });
    }
    let mut route_table = None;
    let mut control_seen = false;
    let mut actual_read_bytes = 0_u64;
    let authentication_phase = if selected_routes.is_some() {
        crate::StorageIoPhase::PropertyMutationInventory
    } else {
        crate::StorageIoPhase::HydrationVerification
    };
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| invalid("graph read path escaped root"))?;
        let text = crate::graph_files::owned_inventory_path_text(relative, !mapped)?;
        let destination = if mapped {
            crate::graph_files::wire_relative_path(&text)?
        } else {
            crate::graph_files::legacy_route_destination(&text)?
        };
        if !destinations.insert(crate::graph_files::portable_case_collision_key(
            &destination,
        )?) {
            return Err(invalid("graph read paths have an ambiguous destination"));
        }
        let is_control = text == crate::route_component::TABLE_FILE;
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| io_error(&error))?;
        if !metadata.file_type().is_file() {
            return Err(invalid("graph read payload is not a regular file"));
        }
        let length = metadata.len();
        if is_control && length > 64 * 1024 * 1024 {
            return Err(invalid("semantic route table byte budget exceeded"));
        }
        let hash_payload = match selected_routes {
            None => true,
            Some(_) if is_control => true,
            Some(routes) => selected_property_path(&text, routes, route_table.as_ref())?,
        };
        if selected_routes.is_some() && !control_seen && mapped && !is_control {
            return Err(invalid("semantic route control file is absent"));
        }
        let (content_xxh64, control_bytes) = if hash_payload {
            let mut file = crate::graph_files::open_retained_relative_file(root, relative)
                .map_err(|error| io_error(&error))?;
            // A targeted capture admits and fingerprints every selected payload;
            // a later fragment open verifies this exact same-inode snapshot.
            crate::graph_admission::admit_file(&file)?;
            file.rewind().map_err(|error| io_error(&error))?;
            let identity =
                graphforge_filesystem::file_identity(&file).map_err(|error| io_error(&error))?;
            let mut checksum = crate::corruption_checksum::Checksum::new();
            let mut control = is_control.then(ControlSha256::new);
            let mut control_bytes = is_control.then(Vec::new);
            let mut buffer = vec![0_u8; 64 * 1024];
            let mut read_bytes = 0_u64;
            let limit = length
                .checked_add(1)
                .ok_or_else(|| invalid("graph read length overflow"))?;
            let mut reader = file.by_ref().take(limit);
            loop {
                let read = reader.read(&mut buffer).map_err(|error| io_error(&error))?;
                if read == 0 {
                    break;
                }
                read_bytes = read_bytes
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("graph read length overflow"))?;
                inventory.authority_read_calls = inventory
                    .authority_read_calls
                    .checked_add(1)
                    .ok_or_else(|| invalid("graph read call count overflow"))?;
                actual_read_bytes = actual_read_bytes
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("graph read byte count overflow"))?;
                inventory.authority_read_bytes = inventory
                    .authority_read_bytes
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("graph read byte count overflow"))?;
                let read_phase = if selected_routes.is_some() && is_control {
                    crate::StorageIoPhase::PropertyMutationRouteAuthority
                } else {
                    authentication_phase
                };
                crate::lifecycle_io::record_read(read_phase, read as u64, 1);
                checksum.update(&buffer[..read]);
                if let Some(control) = &mut control {
                    control.update(&buffer[..read]);
                }
                if let Some(bytes) = &mut control_bytes {
                    bytes.extend_from_slice(&buffer[..read]);
                }
            }
            if read_bytes != length
                || file.metadata().map_err(|error| io_error(&error))?.len() != length
                || graphforge_filesystem::path_identity(&path).ok() != Some(identity)
            {
                return Err(invalid("graph read payload changed during capture"));
            }
            if let Some(control) = control {
                inventory.route_table_sha256 = Some(hex(&control.finalize()));
            }
            let object_phase = if selected_routes.is_some() && is_control {
                crate::StorageIoPhase::PropertyMutationRouteAuthority
            } else {
                authentication_phase
            };
            crate::lifecycle_io::record_objects(object_phase, 1);
            (checksum.finish(), control_bytes)
        } else {
            // Keep authority-path checks and any pre-existing hydration ticket
            // even when this payload is outside the selected property routes.
            let file = crate::graph_files::open_retained_relative_file(root, relative)
                .map_err(|error| io_error(&error))?;
            crate::graph_admission::admit_file(&file)?;
            let identity =
                graphforge_filesystem::file_identity(&file).map_err(|error| io_error(&error))?;
            if file.metadata().map_err(|error| io_error(&error))?.len() != length
                || graphforge_filesystem::path_identity(&path).ok() != Some(identity)
            {
                return Err(invalid("graph read payload changed during capture"));
            }
            (0, None)
        };
        if is_control {
            control_seen = true;
            route_table = control_bytes
                .as_deref()
                .map(|bytes| {
                    crate::route_component::RouteTable::decode(bytes, 64 * 1024 * 1024, 100_000)
                })
                .transpose()?;
        }
        inventory.total_byte_length = inventory
            .total_byte_length
            .checked_add(length)
            .ok_or_else(|| invalid("graph read total length overflow"))?;
        inventory.path_names.push(text.clone());
        if hash_payload {
            inventory.files.push(GraphReadFileEntry {
                relative_path: text,
                byte_length: length,
                content_xxh64,
                role: crate::graph_files::infer_role(relative),
            });
        }
    }
    inventory.path_names.sort();
    inventory
        .files
        .sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    inventory.file_count = inventory.path_names.len() as u64;
    if selected_routes.is_some() {
        debug_assert_eq!(inventory.authority_read_bytes, actual_read_bytes);
    }
    verify_exclusions(excluded)?;
    Ok(inventory)
}

fn selected_property_path(
    relative: &str,
    routes: &std::collections::BTreeSet<(crate::PropertyRouteKind, String)>,
    table: Option<&crate::route_component::RouteTable>,
) -> Result<bool, GfError> {
    let semantic = match table {
        Some(table) => table.semantic_relative_path(relative)?,
        None => relative.to_owned(),
    };
    for (kind, route) in routes {
        let directory = match kind {
            crate::PropertyRouteKind::Node => "properties",
            crate::PropertyRouteKind::Edge => "edge_properties",
        };
        if semantic == format!("{directory}/{route}.parquet")
            || semantic
                .strip_prefix(&format!("{directory}/{route}/"))
                .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

type Exclusions =
    std::collections::BTreeMap<std::path::PathBuf, graphforge_filesystem::FileIdentity>;

/// Every file under `root` except identity-pinned exclusions, which must
/// still name their recorded file.
fn paths_outside_exclusions(
    root: &Path,
    excluded: &Exclusions,
    topology: Option<&crate::TopologyFiles>,
) -> Result<Vec<std::path::PathBuf>, GfError> {
    let mut paths = Vec::new();
    crate::graph_files::collect_source_files_with_topology(root, &mut paths, topology)?;
    let mut retained = Vec::with_capacity(paths.len());
    for path in paths {
        match excluded.get(&path) {
            Some(identity)
                if graphforge_filesystem::path_identity(&path).ok() != Some(*identity) =>
            {
                return Err(corrupt_temporary());
            }
            Some(_) => {}
            None => retained.push(path),
        }
    }
    Ok(retained)
}

fn verify_exclusions(excluded: &Exclusions) -> Result<(), GfError> {
    for (path, identity) in excluded {
        if graphforge_filesystem::path_identity(path).ok() != Some(*identity) {
            return Err(corrupt_temporary());
        }
    }
    Ok(())
}

fn corrupt_temporary() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
        message: "rewrite temporary changed during baseline capture".into(),
    }
}

pub(crate) fn resolve_entry_retained(
    root: &Path,
    entry: &GraphReadFileEntry,
) -> Result<crate::graph_files::RetainedV1InventoryEntry, GfError> {
    crate::graph_files::resolve_read_inventory_entry_retained(root, entry)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}
fn io_error(error: &std::io::Error) -> GfError {
    GfError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_route_capture_does_not_read_unrelated_workspace_payloads() {
        fn capture(noise_bytes: usize) -> GraphReadInventory {
            let root = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(root.path().join("properties")).unwrap();
            std::fs::create_dir_all(root.path().join("topology")).unwrap();
            let target = root.path().join("properties/TARGET.parquet");
            let unrelated = root.path().join("properties/UNRELATED.parquet");
            let topology_path = root.path().join("topology/nodes.parquet");
            std::fs::write(&target, b"fixed selected property payload").unwrap();
            std::fs::write(&unrelated, vec![0x5a; noise_bytes]).unwrap();
            std::fs::write(&topology_path, vec![0x37; noise_bytes]).unwrap();
            let topology = crate::TopologyFiles {
                nodes: vec![(topology_path, "topology/nodes.parquet".into())],
                edges: Vec::new(),
            };
            let routes = std::collections::BTreeSet::from([(
                crate::PropertyRouteKind::Node,
                "TARGET".to_owned(),
            )]);
            capture_graph_read_inventory_for_property_routes(root.path(), &topology, &routes)
                .unwrap()
        }

        let small = capture(128);
        let large = capture(16 * 128);
        let target_bytes = b"fixed selected property payload".len() as u64;
        assert_eq!(small.file_count, 3);
        assert_eq!(large.file_count, 3);
        assert_eq!(small.authority_read_bytes(), target_bytes);
        assert_eq!(large.authority_read_bytes(), target_bytes);
        assert_eq!(small.authority_read_calls(), 1);
        assert_eq!(large.authority_read_calls(), 1);
        assert!(large.total_byte_length > small.total_byte_length);
    }

    #[test]
    fn mapped_property_route_capture_authenticates_semantic_target_and_route_table() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("properties")).unwrap();
        std::fs::create_dir_all(root.path().join("topology")).unwrap();
        let semantic = "Target:Mapped";
        let component = crate::route_component::component(semantic);
        let selected = format!("properties/{component}.parquet");
        let selected_bytes = b"selected mapped property";
        std::fs::write(root.path().join(&selected), selected_bytes).unwrap();
        let unrelated_component = crate::route_component::component("Unrelated");
        let unrelated = format!("properties/{unrelated_component}.parquet");
        std::fs::write(root.path().join(&unrelated), vec![7; 4096]).unwrap();
        let topology_path = root.path().join("topology/nodes.parquet");
        std::fs::write(&topology_path, vec![9; 4096]).unwrap();
        let topology = crate::TopologyFiles {
            nodes: vec![(topology_path, "topology/nodes.parquet".into())],
            edges: Vec::new(),
        };
        let mut table = crate::route_component::RouteTable::default();
        table.insert(semantic, 1024, 8).unwrap();
        table.insert("Unrelated", 1024, 8).unwrap();
        std::fs::write(
            root.path().join(crate::route_component::TABLE_FILE),
            table.encode(1024).unwrap(),
        )
        .unwrap();
        let routes = std::collections::BTreeSet::from([(
            crate::PropertyRouteKind::Node,
            semantic.to_owned(),
        )]);

        let inventory =
            capture_graph_read_inventory_for_property_routes(root.path(), &topology, &routes)
                .unwrap();
        let table_bytes = std::fs::metadata(root.path().join(crate::route_component::TABLE_FILE))
            .unwrap()
            .len();
        assert!(inventory.path_names.contains(&selected));
        assert!(
            !inventory
                .files
                .iter()
                .any(|entry| { entry.relative_path == unrelated })
        );
        assert_eq!(
            inventory.authority_read_bytes(),
            selected_bytes.len() as u64 + table_bytes
        );
    }

    #[test]
    fn first_touch_read_capture_checksums_the_complete_payload() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("topology")).unwrap();
        let path = root.path().join("topology/nodes.parquet");
        std::fs::write(&path, b"capture this nonempty payload after admission").unwrap();
        let published = crate::capture_graph_files(root.path()).unwrap().0;
        crate::graph_admission::AdmissionBatch::begin().register(
            graphforge_filesystem::path_identity(&path).unwrap(),
            &published.files[0],
            path.clone(),
            root.path(),
            path.clone(),
        );

        let actual = capture_graph_read_inventory(root.path()).unwrap();
        assert!(actual.agrees_with(&GraphReadInventory::from_published(&published).unwrap()));
        resolve_entry_retained(root.path(), &actual.files[0]).unwrap();
    }

    #[test]
    fn private_capture_has_no_payload_identity_and_refuses_same_inode_mutation() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("topology")).unwrap();
        let path = root.path().join("topology/nodes.parquet");
        std::fs::write(&path, b"original").unwrap();
        let published = crate::capture_graph_files(root.path()).unwrap().0;
        let capture = graphforge_core::hash_observation::operation::Capture::start();
        let inventory = capture_graph_read_inventory(root.path()).unwrap();
        assert!(inventory.agrees_with(&GraphReadInventory::from_published(&published).unwrap()));
        assert_eq!(inventory.authority_read_calls(), 1);
        assert_eq!(capture.snapshot().artifact_payload_sha256_bytes, 0);
        assert_eq!(capture.snapshot().unclassified_sha256_bytes, 0);
        assert_eq!(capture.snapshot().checksum_bytes, 8);
        let before = graphforge_filesystem::path_identity(&path).unwrap();
        std::fs::write(&path, b"mutated!").unwrap();
        assert_eq!(graphforge_filesystem::path_identity(&path).unwrap(), before);
        assert!(resolve_entry_retained(root.path(), &inventory.files[0]).is_err());
    }

    #[test]
    fn unsupported_published_read_projection_refuses_before_admission() {
        let root = tempfile::tempdir().unwrap();
        let mut published = crate::capture_graph_files(root.path()).unwrap().0;
        for version in [1, 4, 9] {
            published.format_version = version;
            assert!(GraphReadInventory::from_published(&published).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_read_capture_refuses_links() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file"), b"bytes").unwrap();
        std::os::unix::fs::symlink(root.path().join("file"), root.path().join("alias")).unwrap();
        assert!(capture_graph_read_inventory(root.path()).is_err());
    }
}

/// Capture current bytes using topology membership supplied by the session.
pub fn capture_graph_read_inventory_with_topology(
    root: &Path,
    topology: &crate::TopologyFiles,
) -> Result<GraphReadInventory, GfError> {
    capture_graph_read_inventory_excluding(root, &std::collections::BTreeMap::new(), Some(topology))
}
