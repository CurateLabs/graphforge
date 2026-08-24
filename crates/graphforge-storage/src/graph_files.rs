//! Versioned file-backed graph capability under a pinned project generation.
//!
//! Graph workspace files remain ordinary files beneath `generations/<uuid>/graph/`.
//! The `graph`/`files` participant stores only the canonical inventory (paths,
//! lengths, digests, roles). Open paths validate inventory against that tree and
//! never assemble the complete graph into one in-memory Arrow/binary payload.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::project_failpoint;
use crate::project_publication::{ProjectParticipant, ProjectParticipantEncoding};

/// Capability ID for graph storage.
pub const GRAPH_CAPABILITY_ID: &str = "graph";
/// Capability contract version (shared with the legacy snapshot family).
pub const GRAPH_CAPABILITY_VERSION: u32 = 1;
/// Record family for the file-backed inventory participant.
pub const GRAPH_FILES_FAMILY: &str = "files";
/// Record contract version for [`GRAPH_FILES_FAMILY`].
pub const GRAPH_FILES_RECORD_VERSION: u32 = 1;
/// Record version for compact content-addressed graph roots.
pub const GRAPH_FILES_V2_RECORD_VERSION: u32 = 2;
/// Generation-owned directory holding graph workspace files.
pub const GRAPH_TREE_DIR: &str = "graph";

const GRAPH_FILES_FORMAT: &str = "graphforge-graph-files";
const GRAPH_FILES_FORMAT_VERSION: u32 = 1;
const MAX_GRAPH_FILES: usize = 100_000;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
const GRAPH_FILES_SCHEMA_CANONICAL_BYTES: &[u8] =
    b"graphforge-graph-files/1|relative_path|byte_length|content_sha256|role";
const GRAPH_FILES_V2_SCHEMA_CANONICAL_BYTES: &[u8] =
    b"graphforge-graph-files-root/2|root_node_sha256|logical_file_count|logical_byte_length";

/// Logical role inferred from a contained relative workspace path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphFileRole {
    /// Topology facts (nodes/edges Parquet).
    Topology,
    /// Property tables.
    Properties,
    /// Derived adjacency CSR and related index files.
    Index,
    /// Authoritative graph delta journal run (ADR 0019).
    Delta,
    /// Runtime catalog and similar control files.
    Catalog,
    /// Any other regular workspace file.
    Other,
}

/// One inventory entry for a file-backed graph generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphFileEntry {
    /// Normalized relative path beneath the generation `graph/` directory.
    pub relative_path: String,
    /// Exact byte length.
    pub byte_length: u64,
    /// SHA-256 of exact file bytes (64 lowercase hex characters).
    pub content_sha256: String,
    /// Logical role for observability and validation.
    pub role: GraphFileRole,
}

/// Canonical inventory persisted as the `graph`/`files` JSON participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphFilesInventory {
    /// Frozen format identity.
    pub format: String,
    /// Positive format version.
    pub format_version: u32,
    /// Ordered file entries.
    pub files: Vec<GraphFileEntry>,
    /// Aggregate file count (must match `files.len()`).
    pub file_count: u64,
    /// Aggregate declared byte length.
    pub total_byte_length: u64,
}

/// Explicitly decoded `graph/files` participant generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphFilesParticipant {
    /// Expanded generation-owned v1 inventory.
    V1(GraphFilesInventory),
    /// Compact project-object-store v2 manifest root.
    V2(crate::GraphFilesRootV2),
}

/// Structural evidence recorded while validating or materializing a graph tree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphFilesOpenEvidence {
    /// How the workspace was obtained.
    pub strategy: GraphFilesOpenStrategy,
    /// Files whose length and digest were verified.
    pub files_validated: u64,
    /// Bytes whose digests were verified.
    pub bytes_validated: u64,
    /// Files copied into a private workspace.
    pub files_copied: u64,
    /// Bytes copied into a private workspace.
    pub bytes_copied: u64,
    /// Files opened or mapped in place (no copy).
    pub files_opened_in_place: u64,
    /// Immutable files reused from the content-addressed object store.
    pub files_reused: u64,
    /// Logical bytes represented by reused immutable objects.
    pub bytes_reused: u64,
}

/// Open/materialization strategy for a file-backed graph.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GraphFilesOpenStrategy {
    /// No graph files were present.
    #[default]
    Empty,
    /// Read-only open pinned directly to the generation tree.
    PinnedInPlace,
    /// Writable (or otherwise private) workspace materialized file-by-file.
    PrivateMaterialize,
    /// Legacy Arrow snapshot hydrate path.
    LegacySnapshotHydrate,
}

/// Build a canonical inventory and participant from a private workspace root.
///
/// # Errors
/// Rejects links, special files, unsafe relative paths, duplicates, and
/// inventory size overflow.
pub fn capture_graph_files(
    source_root: &Path,
) -> Result<(GraphFilesInventory, ProjectParticipant), GfError> {
    let inventory = build_inventory(source_root)?;
    let bytes = encode_inventory(&inventory)?;
    let participant = inventory_participant(bytes, inventory.file_count)?;
    Ok((inventory, participant))
}

/// Encode inventory bytes as the registered `graph`/`files` participant.
pub fn inventory_participant(
    bytes: Vec<u8>,
    file_count: u64,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: GRAPH_CAPABILITY_ID.into(),
        capability_version: GRAPH_CAPABILITY_VERSION,
        record_family_id: GRAPH_FILES_FAMILY.into(),
        record_version: GRAPH_FILES_RECORD_VERSION,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            GRAPH_FILES_SCHEMA_CANONICAL_BYTES,
        )
        .map_err(|error| GfError::Validation(error.to_string()))?,
        row_count: file_count,
        bytes,
    })
}

/// Encode a compact v2 root as the registered `graph`/`files` participant.
pub fn graph_files_root_participant(
    root: &crate::GraphFilesRootV2,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: GRAPH_CAPABILITY_ID.into(),
        capability_version: GRAPH_CAPABILITY_VERSION,
        record_family_id: GRAPH_FILES_FAMILY.into(),
        record_version: GRAPH_FILES_V2_RECORD_VERSION,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            GRAPH_FILES_V2_SCHEMA_CANONICAL_BYTES,
        )
        .map_err(|error| GfError::Validation(error.to_string()))?,
        row_count: root.logical_file_count,
        bytes: crate::encode_graph_files_root_v2(root)?,
    })
}

/// Decode and mechanically validate inventory JSON bytes.
///
/// # Errors
/// Returns validation errors for contract drift, non-canonical ordering, or
/// inconsistent aggregates.
pub fn decode_inventory(bytes: &[u8]) -> Result<GraphFilesInventory, GfError> {
    if !bytes.ends_with(b"\n") || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n') {
        return Err(validation(
            "graph files inventory must be one canonical JSON line",
        ));
    }
    let inventory: GraphFilesInventory = serde_json::from_slice(bytes)
        .map_err(|error| validation(format!("invalid graph files inventory JSON: {error}")))?;
    validate_inventory_contract(&inventory)?;
    let mut canonical = serde_json::to_vec(&inventory).map_err(|error| {
        validation(format!(
            "graph files inventory cannot be re-encoded: {error}"
        ))
    })?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(validation("graph files inventory is not in canonical form"));
    }
    Ok(inventory)
}

/// Decode either the legacy expanded inventory or compact v2 root.
/// Unknown format tags and future versions fail closed.
pub fn decode_graph_files_participant(bytes: &[u8]) -> Result<GraphFilesParticipant, GfError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| validation(format!("invalid graph files participant JSON: {error}")))?;
    let format = value
        .get("format")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| validation("graph files participant format is missing"))?;
    let version = value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| validation("graph files participant format_version is missing"))?;
    match (format, version) {
        (GRAPH_FILES_FORMAT, version) if version == u64::from(GRAPH_FILES_FORMAT_VERSION) => {
            decode_inventory(bytes).map(GraphFilesParticipant::V1)
        }
        (crate::GRAPH_FILES_V2_FORMAT, version)
            if version == u64::from(crate::GRAPH_FILES_V2_VERSION) =>
        {
            crate::decode_graph_files_root_v2(bytes).map(GraphFilesParticipant::V2)
        }
        _ => Err(GfError::Project {
            code: ProjectErrorCode::UnsupportedProjectFormat,
            message: format!("unsupported graph files participant {format}/{version}"),
        }),
    }
}

/// Encode inventory as one canonical JSON line ending in LF.
pub fn encode_inventory(inventory: &GraphFilesInventory) -> Result<Vec<u8>, GfError> {
    validate_inventory_contract(inventory)?;
    let mut bytes = serde_json::to_vec(inventory)
        .map_err(|error| validation(format!("failed to encode graph files inventory: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Absolute path to the generation-owned graph tree.
#[must_use]
pub fn graph_tree_root(generation_root: &Path) -> PathBuf {
    generation_root.join(GRAPH_TREE_DIR)
}

/// Stage every inventory file from `source_root` into `generation_root/graph/`.
///
/// Files are copied (never linked) so each generation owns exclusive bytes.
///
/// # Errors
/// Rejects path escape, links, digest mismatch, and I/O failures.
pub fn stage_graph_tree(
    source_root: &Path,
    generation_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<GraphFilesOpenEvidence, GfError> {
    let destination_root = graph_tree_root(generation_root);
    if destination_root.exists() {
        return Err(publication_failed(
            "generation graph tree already exists before staging",
        ));
    }
    fs::create_dir_all(&destination_root)
        .map_err(|error| storage("create generation graph tree", &destination_root, error))?;

    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        ..GraphFilesOpenEvidence::default()
    };
    for entry in &inventory.files {
        let relative = Path::new(&entry.relative_path);
        validate_relative_path(relative)?;
        let source = source_root.join(relative);
        let destination = destination_root.join(relative);
        reject_link(&source)?;
        let metadata = fs::symlink_metadata(&source)
            .map_err(|error| storage("inspect graph source file", &source, error))?;
        if !metadata.is_file() {
            return Err(validation("graph tree source is not a regular file"));
        }
        if metadata.len() != entry.byte_length {
            return Err(validation(
                "graph tree source length does not match inventory",
            ));
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create graph tree directory", parent, error))?;
        }
        let digest = copy_regular_file(&source, &destination)?;
        if hex_digest(digest) != entry.content_sha256 {
            return Err(validation(
                "graph tree source digest does not match inventory",
            ));
        }
        sync_file(&destination)?;
        evidence.files_validated = evidence.files_validated.saturating_add(1);
        evidence.bytes_validated = evidence.bytes_validated.saturating_add(entry.byte_length);
        evidence.files_copied = evidence.files_copied.saturating_add(1);
        evidence.bytes_copied = evidence.bytes_copied.saturating_add(entry.byte_length);
        // Fail after at least one graph file is durable so interrupted staging
        // can prove CURRENT remains on the prior complete generation.
        project_failpoint::hit(
            "project.after_graph_file_staged",
            None,
            None,
            "GRAPH_TREE_STAGING",
            false,
        )?;
    }
    sync_directory_tree(&destination_root)?;
    verify_graph_tree(&destination_root, inventory)?;
    Ok(evidence)
}

/// Verify that `graph_root` exactly matches `inventory`.
///
/// # Errors
/// Returns corruption/validation errors for missing, extra, linked, or
/// digest-mismatched files.
pub fn verify_graph_tree(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<(), GfError> {
    validate_inventory_contract(inventory)?;
    if !graph_root.exists() {
        if inventory.files.is_empty() {
            return Ok(());
        }
        return Err(corrupt("generation graph tree is missing"));
    }
    reject_link(graph_root)?;
    let metadata = fs::symlink_metadata(graph_root)
        .map_err(|error| storage("inspect generation graph tree", graph_root, error))?;
    if !metadata.is_dir() {
        return Err(corrupt("generation graph tree is not a directory"));
    }

    let mut observed = BTreeMap::new();
    collect_regular_files(graph_root, graph_root, &mut observed)?;
    let expected: BTreeSet<_> = inventory
        .files
        .iter()
        .map(|entry| entry.relative_path.clone())
        .collect();
    let observed_keys: BTreeSet<_> = observed.keys().cloned().collect();
    if observed_keys != expected {
        return Err(corrupt(
            "generation graph tree inventory does not match on-disk files",
        ));
    }
    for entry in &inventory.files {
        let path = graph_root.join(&entry.relative_path);
        reject_link(&path)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| storage("inspect generation graph file", &path, error))?;
        if !metadata.is_file() {
            return Err(corrupt("generation graph entry is not a regular file"));
        }
        if metadata.len() != entry.byte_length {
            return Err(corrupt(
                "generation graph file length does not match inventory",
            ));
        }
        let digest = hash_file(&path)?;
        if hex_digest(digest) != entry.content_sha256 {
            return Err(corrupt(
                "generation graph file digest does not match inventory",
            ));
        }
    }
    Ok(())
}

/// Materialize `inventory` from `graph_root` into an empty private `target`.
///
/// Copies one file at a time. Never concatenates graph bytes into a single
/// buffer or Arrow binary array.
///
/// # Errors
/// Rejects a non-empty target, inventory mismatch, links, and I/O failures.
pub fn materialize_graph_tree(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    target: &Path,
) -> Result<GraphFilesOpenEvidence, GfError> {
    ensure_empty_directory(target)?;
    verify_graph_tree(graph_root, inventory)?;
    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: u64::try_from(inventory.files.len()).unwrap_or(u64::MAX),
        bytes_validated: inventory.total_byte_length,
        ..GraphFilesOpenEvidence::default()
    };
    for entry in &inventory.files {
        let relative = Path::new(&entry.relative_path);
        let source = graph_root.join(relative);
        let destination = target.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create private graph directory", parent, error))?;
        }
        copy_regular_file(&source, &destination)?;
        evidence.files_copied = evidence.files_copied.saturating_add(1);
        evidence.bytes_copied = evidence.bytes_copied.saturating_add(entry.byte_length);
    }
    Ok(evidence)
}

/// Open evidence for a read-only pin directly onto the generation tree.
#[must_use]
pub fn pinned_open_evidence(inventory: &GraphFilesInventory) -> GraphFilesOpenEvidence {
    GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PinnedInPlace,
        files_validated: u64::try_from(inventory.files.len()).unwrap_or(u64::MAX),
        bytes_validated: inventory.total_byte_length,
        files_copied: 0,
        bytes_copied: 0,
        files_opened_in_place: u64::try_from(inventory.files.len()).unwrap_or(u64::MAX),
        files_reused: 0,
        bytes_reused: 0,
    }
}

/// Infer a stable role from a contained relative path.
#[must_use]
pub fn infer_role(relative: &Path) -> GraphFileRole {
    let mut components = relative.components();
    match components.next() {
        Some(Component::Normal(first)) => {
            let first = first.to_string_lossy();
            match first.as_ref() {
                "topology" => GraphFileRole::Topology,
                "properties" | "edge_properties" => GraphFileRole::Properties,
                "indexes" | "index" => GraphFileRole::Index,
                "deltas" => GraphFileRole::Delta,
                "runtime_catalog.parquet" => GraphFileRole::Catalog,
                _ if first.starts_with("runtime_catalog") => GraphFileRole::Catalog,
                _ => GraphFileRole::Other,
            }
        }
        _ => GraphFileRole::Other,
    }
}

fn build_inventory(source_root: &Path) -> Result<GraphFilesInventory, GfError> {
    let mut paths = Vec::new();
    collect_source_files(source_root, &mut paths)?;
    if paths.len() > MAX_GRAPH_FILES {
        return Err(resource_limit("graph files count exceeds limit"));
    }
    let mut paths = paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(source_root)
                .map_err(|_| validation("graph file path escaped workspace"))?;
            validate_relative_path(relative)?;
            Ok((path_text(relative)?, path))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    paths.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut files = Vec::with_capacity(paths.len());
    let mut total = 0_u64;
    let mut seen = HashSet::new();
    for (relative_text, path) in paths {
        let relative = path
            .strip_prefix(source_root)
            .map_err(|_| validation("graph file path escaped workspace"))?;
        if !seen.insert(relative_text.clone()) {
            return Err(validation("graph files inventory contains duplicate paths"));
        }
        reject_link(&path)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| storage("inspect graph workspace file", &path, error))?;
        if !metadata.is_file() {
            return Err(validation("graph workspace contains a non-regular file"));
        }
        let byte_length = metadata.len();
        total = total
            .checked_add(byte_length)
            .ok_or_else(|| resource_limit("graph files total size overflow"))?;
        let digest = hash_file(&path)?;
        files.push(GraphFileEntry {
            relative_path: relative_text,
            byte_length,
            content_sha256: hex_digest(digest),
            role: infer_role(relative),
        });
    }
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_FORMAT_VERSION,
        file_count: u64::try_from(files.len()).unwrap_or(u64::MAX),
        total_byte_length: total,
        files,
    };
    validate_inventory_contract(&inventory)?;
    Ok(inventory)
}

pub(crate) fn inventory_from_entries(
    files: Vec<GraphFileEntry>,
) -> Result<GraphFilesInventory, GfError> {
    let total_byte_length = files.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph files total size overflow"))
    })?;
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_FORMAT_VERSION,
        file_count: u64::try_from(files.len()).unwrap_or(u64::MAX),
        total_byte_length,
        files,
    };
    validate_inventory_contract(&inventory)?;
    Ok(inventory)
}

fn validate_inventory_contract(inventory: &GraphFilesInventory) -> Result<(), GfError> {
    if inventory.format != GRAPH_FILES_FORMAT {
        return Err(validation("unsupported graph files inventory format"));
    }
    if inventory.format_version != GRAPH_FILES_FORMAT_VERSION {
        return Err(unsupported_version(inventory.format_version));
    }
    if inventory.files.len() > MAX_GRAPH_FILES {
        return Err(resource_limit("graph files count exceeds limit"));
    }
    if inventory.file_count != u64::try_from(inventory.files.len()).unwrap_or(u64::MAX) {
        return Err(validation(
            "graph files inventory file_count does not match entries",
        ));
    }
    let mut total = 0_u64;
    let mut previous: Option<&str> = None;
    let mut seen = HashSet::new();
    for entry in &inventory.files {
        if previous.is_some_and(|value| value >= entry.relative_path.as_str()) {
            return Err(validation(
                "graph files inventory paths are duplicate or non-canonical",
            ));
        }
        if !seen.insert(entry.relative_path.as_str()) {
            return Err(validation("graph files inventory contains duplicate paths"));
        }
        validate_relative_path(Path::new(&entry.relative_path))?;
        if entry.content_sha256.len() != 64
            || !entry
                .content_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(validation(
                "graph files inventory digest must be 64 lowercase hex characters",
            ));
        }
        total = total
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph files inventory total overflow"))?;
        previous = Some(entry.relative_path.as_str());
    }
    if total != inventory.total_byte_length {
        return Err(validation(
            "graph files inventory total_byte_length does not match entries",
        ));
    }
    Ok(())
}

fn collect_source_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), GfError> {
    let mut entries = directory
        .read_dir()
        .map_err(|error| storage("read graph workspace", directory, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| storage("read graph workspace entry", directory, error))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect graph workspace entry", &path, error))?;
        if file_type.is_symlink() {
            return Err(validation("graph workspace contains a symbolic link"));
        }
        if file_type.is_dir() {
            collect_source_files(&path, paths)?;
        } else if file_type.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".lock") || name.starts_with(".gf-stage-") {
                continue;
            }
            paths.push(path);
        } else {
            return Err(validation("graph workspace contains a special file"));
        }
    }
    Ok(())
}

fn collect_regular_files(
    root: &Path,
    directory: &Path,
    observed: &mut BTreeMap<String, PathBuf>,
) -> Result<(), GfError> {
    let mut entries = directory
        .read_dir()
        .map_err(|error| storage("read generation graph tree", directory, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| storage("read generation graph entry", directory, error))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect generation graph entry", &path, error))?;
        if file_type.is_symlink() {
            return Err(corrupt("generation graph tree contains a symbolic link"));
        }
        if file_type.is_dir() {
            collect_regular_files(root, &path, observed)?;
        } else if file_type.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".lock") || name.starts_with(".gf-stage-") {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| corrupt("generation graph path escaped tree"))?;
            validate_relative_path(relative)?;
            let key = path_text(relative)?;
            if observed.insert(key, path).is_some() {
                return Err(corrupt("generation graph tree has duplicate paths"));
            }
            if observed.len() > MAX_GRAPH_FILES {
                return Err(resource_limit("graph files count exceeds limit"));
            }
        } else {
            return Err(corrupt("generation graph tree contains a special file"));
        }
    }
    Ok(())
}

fn ensure_empty_directory(target: &Path) -> Result<(), GfError> {
    if !target.exists() {
        fs::create_dir_all(target)
            .map_err(|error| storage("create private graph workspace", target, error))?;
        return Ok(());
    }
    if target
        .read_dir()
        .map_err(|error| storage("inspect private graph workspace", target, error))?
        .next()
        .is_some()
    {
        return Err(validation(
            "graph workspace must be empty before file-backed materialization",
        ));
    }
    Ok(())
}

fn copy_regular_file(source: &Path, destination: &Path) -> Result<[u8; 32], GfError> {
    reject_link(source)?;
    // Prefer filesystem copy so sparse/holey sources stay sparse when the OS
    // supports it (Linux copy_file_range). Digest the destination so staged
    // bytes remain verified without assembling them into one buffer.
    fs::copy(source, destination)
        .map_err(|error| storage("copy graph source file", destination, error))?;
    let digest = hash_file(destination)?;
    sync_file(destination)?;
    Ok(digest)
}

fn hash_file(path: &Path) -> Result<[u8; 32], GfError> {
    let mut file = File::open(path).map_err(|error| storage("open graph file", path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph file", path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn sync_file(path: &Path) -> Result<(), GfError> {
    let file =
        File::open(path).map_err(|error| storage("open graph file for fsync", path, error))?;
    file.sync_all()
        .map_err(|error| storage("fsync graph file", path, error))
}

fn sync_directory_tree(root: &Path) -> Result<(), GfError> {
    let mut directories = vec![root.to_path_buf()];
    let mut index = 0;
    while index < directories.len() {
        let directory = directories[index].clone();
        for entry in fs::read_dir(&directory)
            .map_err(|error| storage("read graph tree for fsync", &directory, error))?
        {
            let entry =
                entry.map_err(|error| storage("read graph tree entry", &directory, error))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| storage("inspect graph tree entry", &path, error))?;
            if file_type.is_dir() {
                directories.push(path);
            }
        }
        index += 1;
    }
    for directory in directories.into_iter().rev() {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), GfError> {
    let file = File::open(path).map_err(|error| storage("open graph directory", path, error))?;
    file.sync_all()
        .map_err(|error| storage("fsync graph directory", path, error))
}

fn validate_relative_path(path: &Path) -> Result<(), GfError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || matches!(component, Component::ParentDir | Component::RootDir)
        })
    {
        return Err(validation("invalid graph file relative path"));
    }
    let _ = path_text(path)?;
    Ok(())
}

fn path_text(path: &Path) -> Result<String, GfError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| validation("graph file path is not UTF-8"))
}

fn reject_link(path: &Path) -> Result<(), GfError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| storage("inspect path for links", path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(validation("graph path must not be a symbolic link"));
    }
    Ok(())
}

fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn corrupt(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn publication_failed(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::PublicationFailed,
        message: message.into(),
    }
}

fn unsupported_version(version: u32) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::UnsupportedProjectFormat,
        message: format!("unsupported graph files inventory version {version}"),
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Execution(format!("GF_RESOURCE_LIMIT: {}", message.into()))
}

fn storage(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("{action} at {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_round_trip_is_canonical_and_path_ordered() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("topology/edges")).unwrap();
        fs::write(source.path().join("topology/nodes.parquet"), b"nodes").unwrap();
        fs::write(source.path().join("topology/edges/knows.parquet"), b"edges").unwrap();
        fs::write(source.path().join("writer.lock"), b"ignored").unwrap();

        let (inventory, participant) = capture_graph_files(source.path()).unwrap();
        assert_eq!(inventory.file_count, 2);
        assert_eq!(
            inventory.files[0].relative_path,
            "topology/edges/knows.parquet"
        );
        assert_eq!(inventory.files[1].relative_path, "topology/nodes.parquet");
        assert_eq!(inventory.files[1].role, GraphFileRole::Topology);
        assert_eq!(participant.record_family_id, GRAPH_FILES_FAMILY);
        assert_eq!(decode_inventory(&participant.bytes).unwrap(), inventory);
    }

    #[test]
    fn legacy_monolith_and_shards_sort_by_canonical_wire_path() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("topology/nodes")).unwrap();
        fs::write(source.path().join("topology/nodes.parquet"), b"legacy").unwrap();
        fs::write(
            source
                .path()
                .join("topology/nodes/00000000000000000001.parquet"),
            b"shard",
        )
        .unwrap();

        let (inventory, participant) = capture_graph_files(source.path()).unwrap();
        assert_eq!(
            inventory
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec![
                "topology/nodes.parquet",
                "topology/nodes/00000000000000000001.parquet",
            ]
        );
        assert_eq!(decode_inventory(&participant.bytes).unwrap(), inventory);
    }

    #[test]
    fn stage_and_materialize_never_assembles_one_payload() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("properties")).unwrap();
        fs::write(source.path().join("properties/Person.parquet"), b"person").unwrap();
        fs::write(source.path().join("runtime_catalog.parquet"), b"catalog").unwrap();
        let (inventory, _) = capture_graph_files(source.path()).unwrap();

        let generation = tempfile::tempdir().unwrap();
        let evidence = stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
        assert_eq!(evidence.files_copied, 2);
        assert_eq!(evidence.bytes_copied, inventory.total_byte_length);

        let private = tempfile::tempdir().unwrap();
        let opened = materialize_graph_tree(
            &graph_tree_root(generation.path()),
            &inventory,
            private.path(),
        )
        .unwrap();
        assert_eq!(opened.strategy, GraphFilesOpenStrategy::PrivateMaterialize);
        assert_eq!(opened.files_copied, 2);
        assert_eq!(
            fs::read(private.path().join("properties/Person.parquet")).unwrap(),
            b"person"
        );
        assert_eq!(pinned_open_evidence(&inventory).files_opened_in_place, 2);
    }

    #[test]
    fn verify_rejects_digest_mismatch() {
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("topology.parquet"), b"a").unwrap();
        let (inventory, _) = capture_graph_files(source.path()).unwrap();
        let generation = tempfile::tempdir().unwrap();
        stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
        fs::write(
            graph_tree_root(generation.path()).join("topology.parquet"),
            b"b",
        )
        .unwrap();
        assert!(verify_graph_tree(&graph_tree_root(generation.path()), &inventory).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn capture_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        symlink("/tmp", source.path().join("escape")).unwrap();
        assert!(capture_graph_files(source.path()).is_err());
    }

    #[test]
    fn unsupported_inventory_version_is_structured() {
        let mut inventory = GraphFilesInventory {
            format: GRAPH_FILES_FORMAT.into(),
            format_version: 99,
            files: vec![],
            file_count: 0,
            total_byte_length: 0,
        };
        let error = validate_inventory_contract(&inventory).unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        inventory.format_version = GRAPH_FILES_FORMAT_VERSION;
        validate_inventory_contract(&inventory).unwrap();
    }

    #[test]
    fn mid_graph_tree_staging_failure_leaves_current_and_recovery_cleans() {
        const ENABLE_COOKIE: &str = "graphforge-internal-subprocess-v1";
        const HELPER: &str = "graph_files::tests::subprocess_mid_graph_tree_staging_writer";

        let root = tempfile::tempdir().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let parent = publish_graph_files_fixture(root.path(), &[("a.parquet", b"aaa")]);

        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("a.parquet"), b"aaa").unwrap();
        fs::write(source.path().join("b.parquet"), b"bbb").unwrap();
        let (inventory, files) = capture_graph_files(source.path()).unwrap();
        assert!(inventory.file_count >= 2);

        let transaction_uuid = uuid::Uuid::now_v7();
        let generation_uuid = uuid::Uuid::now_v7();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(HELPER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_GRAPH_FILES_ROOT", root.path())
            .env("GRAPHFORGE_TEST_GRAPH_TREE_SOURCE", source.path())
            .env(
                "GRAPHFORGE_TEST_TRANSACTION_UUID",
                transaction_uuid.hyphenated().to_string(),
            )
            .env(
                "GRAPHFORGE_TEST_GENERATION_UUID",
                generation_uuid.hyphenated().to_string(),
            )
            .env(
                "GRAPHFORGE_TEST_GRAPH_FILES_PARTICIPANT",
                serde_json::to_string(&participant_json(&files)).unwrap(),
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "project.after_graph_file_staged.error",
            )
            .status()
            .unwrap();
        assert!(
            status.success(),
            "mid-tree staging helper must exit after the injected error"
        );

        let current = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(current.generation_uuid(), parent);
        assert!(
            root.path()
                .join("generations")
                .join(generation_uuid.hyphenated().to_string())
                .exists(),
            "incomplete attempt should still be on disk before recovery"
        );

        let report = crate::recover_project_transactions(root.path()).unwrap();
        assert_eq!(report.selected_generation_uuid, parent);
        assert!(
            !root
                .path()
                .join("generations")
                .join(generation_uuid.hyphenated().to_string())
                .exists(),
            "recovery must clean the incomplete graph-tree attempt"
        );
        let reopened = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(reopened.generation_uuid(), parent);
        let inventory = reopened.graph_files_inventory().unwrap().unwrap();
        assert_eq!(inventory.file_count, 1);
    }

    #[test]
    fn subprocess_mid_graph_tree_staging_writer() {
        let Ok(root) = std::env::var("GRAPHFORGE_TEST_GRAPH_FILES_ROOT") else {
            return;
        };
        let source = PathBuf::from(std::env::var("GRAPHFORGE_TEST_GRAPH_TREE_SOURCE").unwrap());
        let transaction_uuid =
            uuid::Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_TRANSACTION_UUID").unwrap())
                .unwrap();
        let generation_uuid =
            uuid::Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_GENERATION_UUID").unwrap())
                .unwrap();
        let participant_raw = std::env::var("GRAPHFORGE_TEST_GRAPH_FILES_PARTICIPANT").unwrap();
        let files = participant_from_json(&participant_raw);
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid,
            generation_uuid,
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let error = (|| {
            let outcome =
                crate::stage_project_generation_with_graph_tree(&root, &request, Some(&source))?;
            let crate::ProjectStageOutcome::Staged(staged) = outcome else {
                panic!("fresh graph/files transaction unexpectedly replayed");
            };
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?;
            Ok::<(), GfError>(())
        })()
        .expect_err("configured graph-tree failpoint did not fire");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(error.to_string().contains("GRAPH_TREE_STAGING"));
    }

    fn publish_graph_files_fixture(container: &Path, files: &[(&str, &[u8])]) -> uuid::Uuid {
        let source = tempfile::tempdir().unwrap();
        for (relative, bytes) in files {
            let path = source.path().join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, bytes).unwrap();
        }
        let (_, files_participant) = capture_graph_files(source.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, files_participant);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: uuid::Uuid::now_v7(),
            generation_uuid: uuid::Uuid::now_v7(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let expected = request.generation_uuid;
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree(
                container,
                &request,
                Some(source.path()),
            )
            .unwrap()
        else {
            panic!("fresh graph/files fixture unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        expected
    }

    #[derive(Serialize, Deserialize)]
    struct ParticipantWire {
        capability_id: String,
        capability_version: u32,
        record_family_id: String,
        record_version: u32,
        encoding: String,
        schema_fingerprint: [u8; 32],
        row_count: u64,
        bytes: Vec<u8>,
    }

    fn participant_json(participant: &ProjectParticipant) -> ParticipantWire {
        ParticipantWire {
            capability_id: participant.capability_id.clone(),
            capability_version: participant.capability_version,
            record_family_id: participant.record_family_id.clone(),
            record_version: participant.record_version,
            encoding: match participant.encoding {
                ProjectParticipantEncoding::Json => "json".into(),
                ProjectParticipantEncoding::Arrow => "arrow".into(),
                ProjectParticipantEncoding::Parquet => "parquet".into(),
            },
            schema_fingerprint: participant.schema_fingerprint,
            row_count: participant.row_count,
            bytes: participant.bytes.clone(),
        }
    }

    fn participant_from_json(raw: &str) -> ProjectParticipant {
        let wire: ParticipantWire = serde_json::from_str(raw).unwrap();
        ProjectParticipant {
            capability_id: wire.capability_id,
            capability_version: wire.capability_version,
            record_family_id: wire.record_family_id,
            record_version: wire.record_version,
            encoding: match wire.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "parquet" => ProjectParticipantEncoding::Parquet,
                other => panic!("unknown encoding {other}"),
            },
            schema_fingerprint: wire.schema_fingerprint,
            row_count: wire.row_count,
            bytes: wire.bytes,
        }
    }
}
