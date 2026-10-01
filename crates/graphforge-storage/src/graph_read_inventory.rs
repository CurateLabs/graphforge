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
    /// Number of declared files.
    pub file_count: u64,
    /// Sum of declared file lengths.
    pub total_byte_length: u64,
    route_table_sha256: Option<String>,
    authority_read_calls: u64,
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
            file_count: inventory.file_count,
            total_byte_length: inventory.total_byte_length,
            route_table_sha256: inventory
                .files
                .iter()
                .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
                .map(|entry| entry.content_sha256.clone()),
            authority_read_calls: 0,
        })
    }

    /// Actual nonempty reads made while capturing this private inventory.
    #[must_use]
    pub const fn authority_read_calls(&self) -> u64 {
        self.authority_read_calls
    }

    pub(crate) fn agrees_with(&self, other: &Self) -> bool {
        self.format_version == other.format_version
            && self.files == other.files
            && self.file_count == other.file_count
            && self.total_byte_length == other.total_byte_length
            && self.route_table_sha256 == other.route_table_sha256
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
                retained
                    .file
                    .take(LIMIT + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|error| io_error(&error))?;
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
                table
                    .validate_paths(self.files.iter().map(|entry| entry.relative_path.as_str()))?;
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
    capture_graph_read_inventory_excluding(root, &std::collections::BTreeMap::new())
}

/// Capture read authority for a private tree while a rewrite retains staged
/// temporaries in it. Each excluded path must keep its exact file identity
/// for the whole capture; any other unregistered file is refused as usual.
#[allow(clippy::too_many_lines)]
pub(crate) fn capture_graph_read_inventory_excluding(
    root: &Path,
    excluded: &Exclusions,
) -> Result<GraphReadInventory, GfError> {
    let paths = paths_outside_exclusions(root, excluded)?;
    if paths.len() > crate::graph_files::MAX_GRAPH_FILES {
        return Err(invalid("graph read file count exceeds limit"));
    }
    let mapped = paths.iter().any(|path| {
        path.strip_prefix(root).ok() == Some(Path::new(crate::route_component::TABLE_FILE))
    });
    let mut inventory = GraphReadInventory {
        format_version: if mapped { 7 } else { 5 },
        files: Vec::new(),
        file_count: 0,
        total_byte_length: 0,
        route_table_sha256: None,
        authority_read_calls: 0,
    };
    let mut destinations = BTreeSet::new();
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
        let mut file = crate::graph_files::open_retained_relative_file(root, relative)
            .map_err(|error| io_error(&error))?;
        let identity =
            graphforge_filesystem::file_identity(&file).map_err(|error| io_error(&error))?;
        let length = file.metadata().map_err(|error| io_error(&error))?.len();
        let mut checksum = crate::corruption_checksum::Checksum::new();
        let is_control = text == crate::route_component::TABLE_FILE;
        if is_control && length > 64 * 1024 * 1024 {
            return Err(invalid("semantic route table byte budget exceeded"));
        }
        let mut control = is_control.then(ControlSha256::new);
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
            crate::lifecycle_io::record_read(
                crate::StorageIoPhase::HydrationVerification,
                read as u64,
                1,
            );
            checksum.update(&buffer[..read]);
            if let Some(control) = &mut control {
                control.update(&buffer[..read]);
            }
        }
        if read_bytes != length
            || file.metadata().map_err(|error| io_error(&error))?.len() != length
            || graphforge_filesystem::path_identity(&path).ok() != Some(identity)
        {
            return Err(invalid("graph read payload changed during capture"));
        }
        inventory.total_byte_length = inventory
            .total_byte_length
            .checked_add(length)
            .ok_or_else(|| invalid("graph read total length overflow"))?;
        if let Some(control) = control {
            inventory.route_table_sha256 = Some(hex(&control.finalize()));
        }
        inventory.files.push(GraphReadFileEntry {
            relative_path: text,
            byte_length: length,
            content_xxh64: checksum.finish(),
            role: crate::graph_files::infer_role(relative),
        });
        crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    }
    inventory
        .files
        .sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    inventory.file_count = inventory.files.len() as u64;
    verify_exclusions(excluded)?;
    Ok(inventory)
}

type Exclusions =
    std::collections::BTreeMap<std::path::PathBuf, graphforge_filesystem::FileIdentity>;

/// Every file under `root` except identity-pinned exclusions, which must
/// still name their recorded file.
fn paths_outside_exclusions(
    root: &Path,
    excluded: &Exclusions,
) -> Result<Vec<std::path::PathBuf>, GfError> {
    let mut paths = Vec::new();
    crate::graph_files::collect_source_files(root, &mut paths)?;
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
