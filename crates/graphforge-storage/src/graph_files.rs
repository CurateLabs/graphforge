//! Versioned file-backed graph capability under a pinned project generation.
//!
//! Graph workspace files remain ordinary files beneath `generations/<uuid>/graph/`.
//! The `graph`/`files` participant stores only the canonical inventory (paths,
//! lengths, digests, roles). Open paths validate inventory against that tree and
//! never assemble the complete graph into one in-memory Arrow/binary payload.

mod identity_reuse;
mod read_materialization;
mod topology_capture;
pub(crate) use identity_reuse::{
    CapturedWorkspaceFile, KnownGraphFile, MAX_RETAINED_CAPTURES,
    capture_graph_files_reusing_digests, capture_payload_identity, capture_workspace_over_parent,
    capture_workspace_over_parent_repairing_adjacency,
};
use read_materialization::copy_read_inventory_file;
pub use topology_capture::capture_graph_files_with_topology;
pub(crate) use topology_capture::{
    capture_owned_route_migration_inventory, collect_source_files_with_topology,
};

use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_core::hash_observation::ControlSha256 as Sha256;
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use unicode_normalization::UnicodeNormalization;

use crate::project_failpoint;
use crate::project_publication::{ProjectParticipant, ProjectParticipantEncoding};

/// Capability ID for graph storage.
pub const GRAPH_CAPABILITY_ID: &str = "graph";
/// Capability contract version for published graph participants.
pub const GRAPH_CAPABILITY_VERSION: u32 = 1;
/// Record family for the file-backed inventory participant.
pub const GRAPH_FILES_FAMILY: &str = "files";
/// Expanded inventory carrying versioned payload corruption checksums.
pub const GRAPH_FILES_CHECKSUM_RECORD_VERSION: u32 = 5;
/// Compact root whose payload entries carry corruption checksums.
pub const GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION: u32 = 6;
/// Expanded checksum inventory with semantic routes.
pub const GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION: u32 = 7;
/// Compact checksum root with semantic routes.
pub const GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION: u32 = 8;
/// Generation-owned directory holding graph workspace files.
pub const GRAPH_TREE_DIR: &str = "graph";

const GRAPH_FILES_FORMAT: &str = "graphforge-graph-files";
pub(crate) const MAX_GRAPH_FILES: usize = 100_000;
/// Maximum bytes consumed by one instrumented graph-file copy/hash operation.
pub const GRAPH_FILES_IO_BUFFER_BYTES: usize = 64 * 1024;
const HASH_BUFFER_BYTES: usize = GRAPH_FILES_IO_BUFFER_BYTES;
const CHECKSUM_FILES_SCHEMA: &[u8] =
    b"graphforge-graph-files/5|relative_path|byte_length|content_sha256|content_xxh64|role";
const CHECKSUM_ROOT_SCHEMA: &[u8] = b"graphforge-graph-files-root/6|root_node_sha256|logical_file_count|logical_byte_length|xxh64/1";
const MAPPED_CHECKSUM_FILES_SCHEMA: &[u8] = b"graphforge-graph-files/7|relative_path|byte_length|content_sha256|content_xxh64|role|semantic-routes/1";
const MAPPED_CHECKSUM_ROOT_SCHEMA: &[u8] = b"graphforge-graph-files-root/8|root_node_sha256|logical_file_count|logical_byte_length|xxh64/1|semantic-routes/1";

pub(crate) const fn inventory_is_mapped(version: u32) -> bool {
    matches!(version, GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION)
}

pub(crate) const fn root_is_mapped(version: u32) -> bool {
    matches!(version, GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION)
}

pub(crate) fn expanded_version_for_root(version: u32) -> Result<u32, GfError> {
    match version {
        GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION => Ok(GRAPH_FILES_CHECKSUM_RECORD_VERSION),
        GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION => {
            Ok(GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION)
        }
        _ => Err(unsupported_version(version)),
    }
}

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
    /// Required XXH64, seed zero, for read-time corruption detection.
    #[serde(with = "crate::corruption_checksum::wire_hex")]
    pub content_xxh64: u64,
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
pub(crate) enum GraphFilesParticipant {
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
    /// Files whose presence and exact length were validated.
    pub files_validated: u64,
    /// Bytes whose presence and exact length were validated. This is declared
    /// length, not content read: compact generations checksum a hard-linked
    /// payload on its first touch, not when the workspace is hydrated.
    pub bytes_validated: u64,
    /// Bytes actually read and checksummed while hydrating. Zero for a compact
    /// generation's hard-linked payloads; control files copied into the
    /// workspace and expanded (V1) trees are checksummed here.
    pub bytes_checksummed: u64,
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
    /// Bytes returned by application-level hydration/verification reads.
    pub application_read_bytes: u64,
    /// Non-empty application-level hydration/verification reads.
    pub application_read_calls: u64,
    /// Bytes submitted by application-level copy operations.
    pub application_write_bytes: u64,
    /// Application-level copy submissions.
    pub application_write_calls: u64,
    /// File and directory durability barriers completed while hydrating.
    pub fsync_calls: u64,
    /// Durability barriers completed on materialized files.
    pub file_fsync_calls: u64,
    /// Durability barriers completed on containing directories.
    pub directory_fsync_calls: u64,
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
}

/// Build a canonical inventory and participant from a private workspace root.
///
/// # Errors
/// Rejects links, special files, unsafe relative paths, duplicates, and
/// inventory size overflow.
pub fn capture_graph_files(
    source_root: &Path,
) -> Result<(GraphFilesInventory, ProjectParticipant), GfError> {
    let (inventory, _) = build_inventory(source_root)?;
    let bytes = encode_inventory(&inventory)?;
    let participant = inventory_participant(bytes, inventory.file_count)?;
    Ok((inventory, participant))
}

/// Capture private graph files while polling the caller before each file's
/// authentication. Existing callers retain their uncancelled capture behavior.
pub(crate) fn capture_graph_files_with_cancellation(
    source_root: &Path,
    mut check_cancelled: impl FnMut() -> Result<(), GfError>,
) -> Result<GraphFilesInventory, GfError> {
    check_cancelled()?;
    build_inventory_for_owned_layout(
        source_root,
        false,
        None,
        ARTIFACT_IDENTITY,
        &mut check_cancelled,
        None,
        None,
        None,
    )
    .map(|(inventory, _)| inventory)
}

/// Encode inventory bytes as the registered `graph`/`files` participant.
pub fn inventory_participant(
    bytes: Vec<u8>,
    file_count: u64,
) -> Result<ProjectParticipant, GfError> {
    let version = decode_inventory(&bytes)?.format_version;
    Ok(ProjectParticipant {
        capability_id: GRAPH_CAPABILITY_ID.into(),
        capability_version: GRAPH_CAPABILITY_VERSION,
        record_family_id: GRAPH_FILES_FAMILY.into(),
        record_version: version,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            match version {
                GRAPH_FILES_CHECKSUM_RECORD_VERSION => CHECKSUM_FILES_SCHEMA,
                GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION => MAPPED_CHECKSUM_FILES_SCHEMA,
                _ => return Err(unsupported_version(version)),
            },
        )
        .map_err(|error| GfError::Validation(error.to_string()))?,
        row_count: file_count,
        bytes,
    })
}

/// Encode a compact v2 root as the registered `graph`/`files` participant.
pub(crate) fn graph_files_root_participant(
    root: &crate::GraphFilesRootV2,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: GRAPH_CAPABILITY_ID.into(),
        capability_version: GRAPH_CAPABILITY_VERSION,
        record_family_id: GRAPH_FILES_FAMILY.into(),
        record_version: root.format_version,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            match root.format_version {
                GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION => CHECKSUM_ROOT_SCHEMA,
                GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION => MAPPED_CHECKSUM_ROOT_SCHEMA,
                _ => return Err(unsupported_version(root.format_version)),
            },
        )
        .map_err(|error| GfError::Validation(error.to_string()))?,
        row_count: root.logical_file_count,
        bytes: crate::graph_manifest::encode_root(root)?,
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

/// Decode a current expanded inventory or compact root.
/// Unknown format tags and future versions fail closed.
pub(crate) fn decode_graph_files_participant(
    bytes: &[u8],
) -> Result<GraphFilesParticipant, GfError> {
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
        (GRAPH_FILES_FORMAT, 5 | 7) => decode_inventory(bytes).map(GraphFilesParticipant::V1),
        (crate::GRAPH_FILES_V2_FORMAT, 6 | 8) => {
            crate::decode_graph_files_root_v2(bytes).map(GraphFilesParticipant::V2)
        }
        _ => Err(GfError::Project {
            code: ProjectErrorCode::UnsupportedProjectFormat,
            message: format!("unsupported graph files participant {format}/{version}"),
        }),
    }
}

/// Decode a participant and enforce the descriptor/payload version pairing.
pub(crate) fn decode_versioned_graph_files_participant(
    record_version: u32,
    bytes: &[u8],
) -> Result<GraphFilesParticipant, GfError> {
    let participant = decode_graph_files_participant(bytes)?;
    let payload_version = match &participant {
        GraphFilesParticipant::V1(inventory) => inventory.format_version,
        GraphFilesParticipant::V2(root) => root.format_version,
    };
    if record_version != payload_version {
        return Err(validation(
            "graph files descriptor version does not match its encoded payload",
        ));
    }
    Ok(participant)
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
    stage_graph_tree_with_allocation(source_root, generation_root, inventory, None)
}

pub(crate) fn stage_graph_tree_with_allocation(
    source_root: &Path,
    generation_root: &Path,
    inventory: &GraphFilesInventory,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<GraphFilesOpenEvidence, GfError> {
    validate_inventory_contract(inventory)?;
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
        let source = resolve_v1_inventory_entry(source_root, entry)?;
        let relative = canonical_inventory_relative_path(&entry.relative_path)?;
        let destination = destination_root.join(&relative);
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
        let copied = copy_regular_file_with_allocation(&source, &destination, allocation)?;
        evidence.application_read_bytes = evidence
            .application_read_bytes
            .checked_add(copied.read_bytes)
            .ok_or_else(|| validation("graph staging read byte count overflows"))?;
        evidence.application_read_calls = evidence
            .application_read_calls
            .checked_add(copied.read_calls)
            .ok_or_else(|| validation("graph staging read call count overflows"))?;
        evidence.application_write_bytes = evidence
            .application_write_bytes
            .checked_add(copied.write_bytes)
            .ok_or_else(|| validation("graph staging write byte count overflows"))?;
        evidence.application_write_calls = evidence
            .application_write_calls
            .checked_add(copied.write_calls)
            .ok_or_else(|| validation("graph staging write call count overflows"))?;
        evidence.fsync_calls = evidence
            .fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| validation("graph staging fsync count overflows"))?;
        evidence.file_fsync_calls = evidence
            .file_fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| validation("graph staging file barrier count overflows"))?;
        // The inventory captured this file's SHA-256 identity and XXH64 in one
        // pass. The staged copy is admitted by exact length and checksum.
        if copied.read_bytes != entry.byte_length || copied.checksum != entry.content_xxh64 {
            return Err(validation(
                "graph tree source checksum does not match inventory",
            ));
        }
        evidence.files_validated = evidence
            .files_validated
            .checked_add(1)
            .ok_or_else(|| validation("graph staging validated-file count overflows"))?;
        evidence.bytes_validated = evidence
            .bytes_validated
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph staging validated-byte count overflows"))?;
        evidence.bytes_checksummed = evidence.bytes_checksummed.saturating_add(entry.byte_length);
        evidence.files_copied = evidence
            .files_copied
            .checked_add(1)
            .ok_or_else(|| validation("graph staging copied-file count overflows"))?;
        evidence.bytes_copied = evidence
            .bytes_copied
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph staging copied-byte count overflows"))?;
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
    let directory_fsync_calls = sync_directory_tree(&destination_root)?;
    evidence.directory_fsync_calls = evidence
        .directory_fsync_calls
        .checked_add(directory_fsync_calls)
        .ok_or_else(|| validation("graph staging directory barrier count overflows"))?;
    evidence.fsync_calls = evidence
        .fsync_calls
        .checked_add(directory_fsync_calls)
        .ok_or_else(|| validation("graph staging fsync count overflows"))?;
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

    let mut observed = BTreeSet::new();
    collect_regular_file_paths(graph_root, graph_root, &mut observed)?;
    let mut resolved = BTreeSet::new();
    for entry in &inventory.files {
        let retained = resolve_v1_inventory_entry_retained(graph_root, entry)?;
        let metadata = retained
            .file
            .metadata()
            .map_err(|error| storage("inspect generation graph file", &retained.path, error))?;
        if !metadata.is_file() {
            return Err(corrupt("generation graph entry is not a regular file"));
        }
        if metadata.len() != entry.byte_length {
            return Err(corrupt(
                "generation graph file length does not match inventory",
            ));
        }
        if graphforge_filesystem::file_identity(&retained.file).ok() != Some(retained.identity)
            || graphforge_filesystem::path_identity(&retained.path).ok() != Some(retained.identity)
        {
            return Err(corrupt("generation graph file identity changed"));
        }
        if !resolved.insert(
            retained
                .path
                .strip_prefix(graph_root)
                .map_err(|_| corrupt("generation graph path escaped tree"))?
                .to_path_buf(),
        ) {
            return Err(corrupt(
                "legacy graph entries have ambiguous physical authority",
            ));
        }
    }
    if inventory_is_mapped(inventory.format_version) {
        authenticate_route_table(graph_root, inventory)?;
    }
    if observed != resolved {
        return Err(corrupt(
            "generation graph tree inventory does not match on-disk files",
        ));
    }
    Ok(())
}

/// Authenticate the retained table bytes before exposing any decoded route.
pub(crate) fn authenticate_route_table(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<crate::route_component::RouteTable, GfError> {
    use std::io::{Seek, SeekFrom};
    const MAX_TABLE_BYTES: u64 = 64 * 1024 * 1024;
    if !inventory_is_mapped(inventory.format_version) {
        return Err(corrupt(
            "raw graph layout cannot authorize semantic route decoding",
        ));
    }
    let entry = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
        .ok_or_else(|| corrupt("mapped graph inventory lacks semantic route authority"))?;
    if entry.byte_length > MAX_TABLE_BYTES {
        return Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            message: "semantic route table byte budget exceeded".into(),
        });
    }
    let mut retained = resolve_v1_inventory_entry_retained(graph_root, entry)?;
    retained
        .file
        .seek(SeekFrom::Start(0))
        .map_err(|_| corrupt("semantic route authority seek failed"))?;
    let mut bytes = Vec::new();
    (&mut retained.file)
        .take(entry.byte_length + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| corrupt("semantic route authority read failed"))?;
    if bytes.len() as u64 != entry.byte_length
        || hex_digest(Sha256::digest(&bytes).into()) != entry.content_sha256
        || graphforge_filesystem::file_identity(&retained.file).ok() != Some(retained.identity)
    {
        return Err(corrupt("semantic route authority changed during admission"));
    }
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        entry.byte_length,
        1,
    );
    let table = crate::route_component::RouteTable::decode(
        &bytes,
        MAX_TABLE_BYTES,
        MAX_GRAPH_FILES as u64,
    )?;
    table.validate_paths(
        inventory
            .files
            .iter()
            .map(|entry| entry.relative_path.as_str()),
    )?;
    Ok(table)
}

pub(crate) fn read_route_table_counted(
    root: &Path,
    entry: &GraphFileEntry,
) -> Result<(Vec<u8>, u64), GfError> {
    if entry.relative_path != crate::route_component::TABLE_FILE {
        return Err(corrupt("invalid route authority read request"));
    }
    read_route_table_bounded(root, entry.byte_length)
}

pub(crate) fn read_route_table_bounded(
    root: &Path,
    byte_length: u64,
) -> Result<(Vec<u8>, u64), GfError> {
    if byte_length > 64 * 1024 * 1024 {
        return Err(corrupt("invalid route authority read request"));
    }
    let mut file = open_retained_relative_file(root, Path::new(crate::route_component::TABLE_FILE))
        .map_err(|error| storage("open retained route table", root, error))?;
    let mut bytes = Vec::new();
    let mut calls = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read retained route table", root, error))?;
        if read == 0 {
            break;
        }
        if bytes.len() as u64 + read as u64 > byte_length {
            return Err(corrupt("route authority exceeds authenticated length"));
        }
        bytes.extend_from_slice(&buffer[..read]);
        calls += 1;
    }
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        bytes.len() as u64,
        calls,
    );
    Ok((bytes, calls))
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
    let mut route_reads = (0_u64, 0_u64);
    let routes =
        crate::route_component::materialize::MaterializationRoutes::prepare(inventory, |entry| {
            let (bytes, calls) = read_route_table_counted(graph_root, entry)?;
            route_reads = (bytes.len() as u64, calls);
            Ok(bytes)
        })?;
    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: u64::try_from(inventory.files.len())
            .map_err(|_| validation("graph hydration file inventory exceeds u64"))?,
        bytes_validated: inventory.total_byte_length,
        bytes_checksummed: inventory.total_byte_length,
        application_read_bytes: route_reads.0,
        application_read_calls: route_reads.1,
        ..GraphFilesOpenEvidence::default()
    };
    for (entry, relative) in inventory.files.iter().zip(&routes.destinations) {
        let source = resolve_v1_inventory_entry_retained(graph_root, entry)?;
        let destination = target.join(wire_relative_path(relative)?);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create private graph directory", parent, error))?;
        }
        let copied = copy_read_inventory_file(source, &destination, entry)?;
        evidence.application_read_bytes = evidence
            .application_read_bytes
            .checked_add(copied.read_bytes)
            .ok_or_else(|| validation("graph hydration read byte count overflows"))?;
        evidence.application_read_calls = evidence
            .application_read_calls
            .checked_add(copied.read_calls)
            .ok_or_else(|| validation("graph hydration read call count overflows"))?;
        evidence.application_write_bytes = evidence
            .application_write_bytes
            .checked_add(copied.write_bytes)
            .ok_or_else(|| validation("graph hydration write byte count overflows"))?;
        evidence.application_write_calls = evidence
            .application_write_calls
            .checked_add(copied.write_calls)
            .ok_or_else(|| validation("graph hydration write call count overflows"))?;
        evidence.fsync_calls = evidence
            .fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| validation("graph hydration fsync count overflows"))?;
        evidence.file_fsync_calls = evidence
            .file_fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| validation("graph hydration file barrier count overflows"))?;
        make_private_copy_owner_writable(&destination)?;
        evidence.fsync_calls = evidence
            .fsync_calls
            .checked_add(1)
            .ok_or_else(|| validation("graph hydration fsync count overflows"))?;
        evidence.file_fsync_calls = evidence
            .file_fsync_calls
            .checked_add(1)
            .ok_or_else(|| validation("graph hydration file barrier count overflows"))?;
        evidence.files_copied = evidence
            .files_copied
            .checked_add(1)
            .ok_or_else(|| validation("graph hydration copied-file count overflows"))?;
        evidence.bytes_copied = evidence
            .bytes_copied
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph hydration copied-byte count overflows"))?;
    }
    routes.install_table(target, &mut evidence)?;
    Ok(evidence)
}

/// Open evidence for a read-only pin directly onto the generation tree.
#[must_use]
pub fn pinned_open_evidence(inventory: &GraphFilesInventory) -> GraphFilesOpenEvidence {
    GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PinnedInPlace,
        files_validated: u64::try_from(inventory.files.len()).unwrap_or(u64::MAX),
        bytes_validated: inventory.total_byte_length,
        bytes_checksummed: inventory.total_byte_length,
        files_copied: 0,
        bytes_copied: 0,
        files_opened_in_place: u64::try_from(inventory.files.len()).unwrap_or(u64::MAX),
        files_reused: 0,
        bytes_reused: 0,
        ..GraphFilesOpenEvidence::default()
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
                "runtime_catalog.parquet" | crate::route_component::TABLE_FILE => {
                    GraphFileRole::Catalog
                }
                _ if first.starts_with("runtime_catalog") => GraphFileRole::Catalog,
                _ => GraphFileRole::Other,
            }
        }
        _ => GraphFileRole::Other,
    }
}

fn build_inventory(source_root: &Path) -> Result<(GraphFilesInventory, u64), GfError> {
    build_inventory_for_owned_layout(
        source_root,
        false,
        None,
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        None,
        None,
        None,
    )
}

const ARTIFACT_IDENTITY: graphforge_core::hash_observation::HashDomain =
    graphforge_core::hash_observation::HashDomain::ArtifactPayload;

fn build_inventory_for_owned_layout(
    source_root: &Path,
    admit_raw_routes: bool,
    reuse: Option<&std::collections::HashMap<String, KnownGraphFile>>,
    domain: graphforge_core::hash_observation::HashDomain,
    check_cancelled: &mut dyn FnMut() -> Result<(), GfError>,
    mut retain: Option<&mut std::collections::BTreeMap<String, CapturedWorkspaceFile>>,
    force_hash_prefix: Option<&str>,
    topology: Option<&crate::TopologyFiles>,
) -> Result<(GraphFilesInventory, u64), GfError> {
    let mut paths = Vec::new();
    collect_source_files_with_topology(source_root, &mut paths, topology)?;
    if paths.len() > MAX_GRAPH_FILES {
        return Err(resource_limit("graph files count exceeds limit"));
    }
    let mapped = paths
        .iter()
        .any(|path| path == &source_root.join(crate::route_component::TABLE_FILE));
    let mut paths = paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(source_root)
                .map_err(|_| validation("graph file path escaped workspace"))?;
            let relative_text = owned_inventory_path_text(relative, admit_raw_routes && !mapped)?;
            Ok((relative_text, path))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    paths.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut files = Vec::with_capacity(paths.len());
    let mut total = 0_u64;
    let mut read_calls = 0_u64;
    let mut seen = HashSet::new();
    for (relative_text, path) in paths {
        check_cancelled()?;
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
        let reused = reuse
            .and_then(|known| known.get(&relative_text))
            .filter(|_| !force_hash_prefix.is_some_and(|prefix| relative_text.starts_with(prefix)))
            .filter(|known| known.byte_length == byte_length);
        let (content_sha256, content_xxh64, calls, hashed) =
            capture_payload_identity(&path, reused, domain)?;
        // A file hashed afresh is about to be named by that digest. Keep the
        // exact handle that was hashed, so installing it needs no second SHA-256.
        if let (Some(retain), Some(file)) = (retain.as_deref_mut(), hashed)
            && retain.len() < MAX_RETAINED_CAPTURES
        {
            retain.insert(
                relative_text.clone(),
                CapturedWorkspaceFile::new(
                    file,
                    path.clone(),
                    byte_length,
                    content_sha256.clone(),
                    content_xxh64,
                ),
            );
        }
        read_calls = read_calls
            .checked_add(calls)
            .ok_or_else(|| resource_limit("graph files authentication read calls overflow"))?;
        files.push(GraphFileEntry {
            content_xxh64,
            relative_path: relative_text,
            byte_length,
            content_sha256,
            role: infer_role(relative),
        });
    }
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: if files
            .iter()
            .any(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
        {
            GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION
        } else {
            GRAPH_FILES_CHECKSUM_RECORD_VERSION
        },
        file_count: u64::try_from(files.len()).unwrap_or(u64::MAX),
        total_byte_length: total,
        files,
    };
    validate_inventory_contract(&inventory)?;
    if inventory_is_mapped(inventory.format_version) {
        authenticate_route_table(source_root, &inventory)?;
    }
    Ok((inventory, read_calls))
}

pub(crate) fn owned_inventory_path_text(
    relative: &Path,
    admit_raw_routes: bool,
) -> Result<String, GfError> {
    if admit_raw_routes {
        let parts = relative
            .components()
            .map(|part| match part {
                Component::Normal(value) => value
                    .to_str()
                    .ok_or_else(|| validation("graph route path is not UTF-8")),
                _ => Err(validation("invalid owned graph route path")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let text = parts.join("/");
        legacy_route_destination(&text)?;
        Ok(text)
    } else {
        validate_relative_path(relative)?;
        path_text(relative)
    }
}

/// Read authority for a private workspace a rewrite is about to change. The
/// rewrite reads existing files; it never publishes this inventory, so it
/// admits them by exact length and XXH64 and names nothing by SHA-256.
pub(crate) fn capture_rewrite_baseline(
    root: &Path,
    rewrite: &crate::RewriteBatch,
) -> Result<crate::GraphReadInventory, GfError> {
    let topology = rewrite
        .topology_authority()
        .map(|authority| crate::enumerate_topology_files(authority, None))
        .transpose()?;
    let inventory = crate::graph_read_inventory::capture_graph_read_inventory_excluding(
        root,
        &rewrite.retained_temporary_identities()?,
        topology.as_ref(),
    )?;
    // Refuse unregistered or unmapped files at the baseline, as before.
    inventory.authenticate_routes(root)?;
    Ok(inventory)
}

#[cfg(test)]
pub(crate) fn inventory_from_entries(
    files: Vec<GraphFileEntry>,
) -> Result<GraphFilesInventory, GfError> {
    inventory_from_entries_with_version(files, GRAPH_FILES_CHECKSUM_RECORD_VERSION)
}

pub(crate) fn inventory_from_entries_with_version(
    files: Vec<GraphFileEntry>,
    version: u32,
) -> Result<GraphFilesInventory, GfError> {
    let total_byte_length = files.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph files total size overflow"))
    })?;
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: version,
        file_count: u64::try_from(files.len()).unwrap_or(u64::MAX),
        total_byte_length,
        files,
    };
    validate_inventory_contract(&inventory)?;
    Ok(inventory)
}

pub(crate) fn validate_inventory_contract(inventory: &GraphFilesInventory) -> Result<(), GfError> {
    if inventory.format != GRAPH_FILES_FORMAT {
        return Err(validation("unsupported graph files inventory format"));
    }
    if !matches!(
        inventory.format_version,
        GRAPH_FILES_CHECKSUM_RECORD_VERSION | GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION
    ) {
        return Err(unsupported_version(inventory.format_version));
    }
    if !inventory_is_mapped(inventory.format_version)
        && inventory
            .files
            .iter()
            .any(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
    {
        return Err(corrupt(
            "raw graph layout contains reserved semantic route authority",
        ));
    }
    if inventory_is_mapped(inventory.format_version)
        && !inventory
            .files
            .iter()
            .any(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
    {
        return Err(corrupt(
            "mapped graph inventory lacks semantic route authority",
        ));
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
    let mut canonical_destinations = HashSet::new();
    for entry in &inventory.files {
        if previous.is_some_and(|value| value >= entry.relative_path.as_str()) {
            return Err(validation(
                "graph files inventory paths are duplicate or non-canonical",
            ));
        }
        if !seen.insert(entry.relative_path.as_str()) {
            return Err(validation("graph files inventory contains duplicate paths"));
        }
        let canonical_destination = if inventory_is_mapped(inventory.format_version) {
            wire_relative_path(&entry.relative_path)?
        } else {
            let _ = inventory_relative_path_candidates(&entry.relative_path)?;
            legacy_route_destination(&entry.relative_path)?
        };
        if !canonical_destinations.insert(portable_case_collision_key(&canonical_destination)?) {
            return Err(validation(
                "graph files inventory paths have an ambiguous canonical destination",
            ));
        }
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

/// Conservative portable key for filesystems with case-insensitive lookup.
///
/// NFC before and after Unicode uppercase expansion makes the comparison
/// deterministic across hosts without rewriting the authenticated wire path.
pub(crate) fn portable_case_collision_key(path: &Path) -> Result<String, GfError> {
    let text = path_text(path)?;
    Ok(text.nfc().flat_map(char::to_uppercase).nfc().collect())
}

pub(crate) fn collect_source_files(
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> Result<(), GfError> {
    collect_source_files_with_topology(directory, paths, None)
}

/// Operational files that may legitimately coexist with the graph workspace
/// but are never generation data. Keep this list structural and exact: a
/// basename suffix/prefix rule would let unauthenticated data disappear from
/// inventory reconciliation while topology readers consume it.
pub(crate) fn is_graph_operational_file(relative: &Path) -> bool {
    let Some(components) = relative
        .components()
        .map(|component| match component {
            std::path::Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    match components.as_slice() {
        [".graphforge-cache", ..]
        | [".graphforge-rewrite.lock"]
        | ["embeddings", ".catalog.lock" | ".refresh.lock"]
        | ["graph-objects", "lifecycle.lock"]
        | ["indexes", "search", .., ".writer.lock"]
        | ["embeddings", "space", .., ".writer.lock"] => true,
        ["embeddings", name]
            if name
                .strip_prefix(".writer-")
                .and_then(|value| value.strip_suffix(".lock"))
                .is_some_and(|value| {
                    value.len() == 64
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                }) =>
        {
            true
        }
        ["embeddings", "spaces", identity, ".writer.lock"]
            if identity.len() == 64
                && identity
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            true
        }
        ["graph-objects", "active", lease]
            if lease.strip_suffix(".lock").is_some_and(|value| {
                uuid::Uuid::parse_str(value).is_ok_and(|parsed| {
                    value.len() == 36 && parsed.hyphenated().to_string() == value
                })
            }) =>
        {
            true
        }
        _ => false,
    }
}

fn collect_source_files_from(
    root: &Path,
    directory: &Path,
    paths: &mut Vec<PathBuf>,
    declared_topology: bool,
) -> Result<(), GfError> {
    let mut entries = directory
        .read_dir()
        .map_err(|error| storage("read graph workspace", directory, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| storage("read graph workspace entry", directory, error))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if declared_topology
            && path
                .strip_prefix(root)
                .is_ok_and(crate::topology_files::is_topology)
        {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect graph workspace entry", &path, error))?;
        if file_type.is_symlink() {
            return Err(validation("graph workspace contains a symbolic link"));
        }
        if file_type.is_dir() {
            collect_source_files_from(root, &path, paths, declared_topology)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| validation("graph workspace path escaped root"))?;
            if is_graph_operational_file(relative) {
                continue;
            }
            paths.push(path);
        } else {
            return Err(validation("graph workspace contains a special file"));
        }
    }
    Ok(())
}

fn collect_regular_file_paths(
    root: &Path,
    directory: &Path,
    observed: &mut BTreeSet<PathBuf>,
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
            collect_regular_file_paths(root, &path, observed)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| corrupt("generation graph path escaped tree"))?;
            if !is_graph_operational_file(relative) {
                observed.insert(relative.to_path_buf());
                if observed.len() > MAX_GRAPH_FILES {
                    return Err(resource_limit("graph files count exceeds limit"));
                }
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

struct CopyIoEvidence {
    checksum: u64,
    read_bytes: u64,
    read_calls: u64,
    write_bytes: u64,
    write_calls: u64,
    fsync_calls: u64,
}

fn copy_regular_file_with_allocation(
    source: &Path,
    destination: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<CopyIoEvidence, GfError> {
    let result = copy_regular_file(source, destination);
    let observed = if let Some(allocation) = allocation {
        (|| {
            let parent = destination
                .parent()
                .ok_or_else(|| validation("graph destination has no parent"))?;
            let directory = graphforge_filesystem::StableDirectory::open(parent)
                .map_err(|error| storage("observe graph destination", destination, error))?;
            let name = destination
                .file_name()
                .ok_or_else(|| validation("graph destination has no name"))?;
            match directory.open_child_file(name) {
                Ok(file) => allocation.replace_file_at(destination, &file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && result.is_err() => {
                    Ok(())
                }
                Err(error) => Err(storage("observe graph destination", destination, error)),
            }
        })()
    } else {
        Ok(())
    };
    match result {
        Err(primary) => Err(primary),
        Ok(copied) => {
            observed?;
            Ok(copied)
        }
    }
}

fn copy_regular_file(source: &Path, destination: &Path) -> Result<CopyIoEvidence, GfError> {
    reject_link(source)?;
    // Prefer filesystem copy so sparse/holey sources stay sparse when the OS
    // supports it (Linux copy_file_range). Checksum the destination so staged
    // bytes remain verified without assembling them into one buffer.
    let copied = fs::copy(source, destination)
        .map_err(|error| storage("copy graph source file", destination, error))?;
    #[cfg(windows)]
    {
        // Copy preserves read-only source attributes. Only the new private
        // destination needs write access for its durability barrier.
        let mut permissions = fs::metadata(destination)
            .map_err(|error| storage("inspect copied graph permissions", destination, error))?
            .permissions();
        permissions.set_readonly(false);
        fs::set_permissions(destination, permissions)
            .map_err(|error| storage("make copied graph writable", destination, error))?;
    }
    let (checksum, read_bytes, read_calls) = checksum_file_io_counted(destination)?;
    sync_file(destination)?;
    crate::lifecycle_io::record_write(
        crate::StorageIoPhase::HydrationVerification,
        copied,
        u64::from(copied != 0),
    );
    crate::lifecycle_io::record_fsync(crate::StorageIoPhase::HydrationVerification, 1);
    Ok(CopyIoEvidence {
        checksum,
        read_bytes,
        read_calls,
        write_bytes: copied,
        write_calls: u64::from(copied != 0),
        fsync_calls: 1,
    })
}

fn checksum_file_io_counted(path: &Path) -> Result<(u64, u64, u64), GfError> {
    let mut file = File::open(path).map_err(|error| storage("open graph file", path, error))?;
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut bytes = 0_u64;
    let mut calls = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph file", path, error))?;
        if read == 0 {
            break;
        }
        checksum.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| validation("graph file checksum byte count overflows"))?;
        calls = calls
            .checked_add(1)
            .ok_or_else(|| validation("graph file checksum call count overflows"))?;
    }
    crate::lifecycle_io::record_read(crate::StorageIoPhase::HydrationVerification, bytes, calls);
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    Ok((checksum.finish(), bytes, calls))
}

#[cfg(unix)]
fn make_private_copy_owner_writable(path: &Path) -> Result<(), GfError> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = fs::metadata(path)
        .map_err(|error| storage("inspect private graph file permissions", path, error))?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(permissions.mode() | 0o200);
    fs::set_permissions(path, permissions)
        .map_err(|error| storage("make private graph file owner-writable", path, error))?;
    sync_file(path)
}

#[cfg(not(unix))]
fn make_private_copy_owner_writable(path: &Path) -> Result<(), GfError> {
    let mut permissions = fs::metadata(path)
        .map_err(|error| storage("inspect private graph file permissions", path, error))?
        .permissions();
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)
        .map_err(|error| storage("make private graph file owner-writable", path, error))?;
    sync_file(path)
}

#[cfg(test)]
fn hash_file(path: &Path) -> Result<[u8; 32], GfError> {
    hash_file_counted(path).map(|(digest, _)| digest)
}

#[cfg(test)]
fn hash_file_counted(path: &Path) -> Result<([u8; 32], u64), GfError> {
    let mut file = File::open(path).map_err(|error| storage("open graph file", path, error))?;
    hash_reader(&mut file, path)
}

#[cfg(test)]
fn hash_reader(file: &mut File, path: &Path) -> Result<([u8; 32], u64), GfError> {
    hash_reader_with_checksum(file, path, ARTIFACT_IDENTITY)
        .map(|(digest, _, calls)| (digest, calls))
}

fn hash_reader_with_checksum(
    file: &mut File,
    path: &Path,
    domain: graphforge_core::hash_observation::HashDomain,
) -> Result<([u8; 32], u64, u64), GfError> {
    // Published captures name artifact payload; a temporary replay view's
    // capture feeds only a contract fingerprint and is observed as that domain.
    let mut hasher = crate::payload_digest::PayloadSha256::for_domain(domain);
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut read_calls = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph file", path, error))?;
        if read == 0 {
            break;
        }
        read_calls = read_calls
            .checked_add(1)
            .ok_or_else(|| resource_limit("graph file authentication read calls overflow"))?;
        hasher.update(&buffer[..read]);
        checksum.update(&buffer[..read]);
    }
    Ok((hasher.finalize().into(), checksum.finish(), read_calls))
}

pub(crate) fn checksum_reader(file: &mut impl Read, path: &Path) -> Result<(u64, u64), GfError> {
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut calls = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph payload checksum", path, error))?;
        if read == 0 {
            break;
        }
        checksum.update(&buffer[..read]);
        calls = calls
            .checked_add(1)
            .ok_or_else(|| resource_limit("graph payload checksum read calls overflow"))?;
    }
    Ok((checksum.finish(), calls))
}

fn sync_file(path: &Path) -> Result<(), GfError> {
    #[cfg(not(windows))]
    let file = File::open(path);
    #[cfg(windows)]
    let file = fs::OpenOptions::new().write(true).open(path);
    let file = file.map_err(|error| storage("open graph file for fsync", path, error))?;
    crate::durable_commit::seal_file(&file)
        .map_err(|error| storage("fsync graph file", path, error))
}

fn sync_directory_tree(root: &Path) -> Result<u64, GfError> {
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
    let count = u64::try_from(directories.len()).unwrap_or(u64::MAX);
    for directory in directories.into_iter().rev() {
        sync_directory(&directory)?;
    }
    Ok(count)
}

fn sync_directory(path: &Path) -> Result<(), GfError> {
    crate::project_publication::sync_directory(path)
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
    let mut encoded = String::new();
    for component in path.components() {
        let Component::Normal(component) = component else {
            return Err(validation("invalid graph file relative path"));
        };
        let component = component
            .to_str()
            .ok_or_else(|| validation("graph file path is not UTF-8"))?;
        validate_wire_component(component)?;
        if !encoded.is_empty() {
            encoded.push('/');
        }
        encoded.push_str(component);
    }
    if encoded.is_empty() {
        return Err(validation("invalid graph file relative path"));
    }
    Ok(encoded)
}

/// Decode the platform-independent inventory spelling without asking the host
/// path parser to interpret wire separators or aliases.
pub(crate) fn wire_relative_path(text: &str) -> Result<PathBuf, GfError> {
    if text.is_empty() || text.starts_with('/') || text.ends_with('/') {
        return Err(validation("invalid graph file wire path"));
    }
    let mut path = PathBuf::new();
    for component in text.split('/') {
        validate_wire_component(component)?;
        path.push(component);
    }
    if path_text(&path)? != text {
        return Err(validation("graph file wire path is not canonical"));
    }
    Ok(path)
}

pub(crate) fn canonical_inventory_relative_path(text: &str) -> Result<PathBuf, GfError> {
    if text.contains('\\') {
        wire_relative_path(&text.replace('\\', "/"))
    } else {
        wire_relative_path(text)
    }
}

pub(crate) fn canonical_inventory_relative_text(text: &str) -> Result<String, GfError> {
    Ok(canonical_inventory_relative_path(text)?
        .to_string_lossy()
        .replace('\\', "/"))
}

/// Preserve exact legacy semantic route components while canonicalizing only
/// legacy wire separators outside recognized route positions.
pub(crate) fn legacy_inventory_logical_text(text: &str) -> Result<String, GfError> {
    if crate::route_component::route_position(text)?.is_some() {
        legacy_route_destination(text)?;
        Ok(text.to_owned())
    } else {
        canonical_inventory_relative_text(text)
    }
}

/// Legacy route spellings are admitted only at known semantic positions. The
/// translated destination still passes the unchanged portable wire validator.
pub(crate) fn legacy_route_destination(text: &str) -> Result<PathBuf, GfError> {
    let mut table = crate::route_component::RouteTable::default();
    let encoded =
        crate::route_component::encode_relative_route(text, &mut table, 64 * 1024 * 1024, 100_000)?;
    if encoded == text {
        canonical_inventory_relative_path(text)
    } else {
        wire_relative_path(&encoded)
    }
}

fn inventory_relative_path_candidates(text: &str) -> Result<Vec<PathBuf>, GfError> {
    if crate::route_component::route_position(text)?.is_some() {
        // Validate semantic containment and every non-route component before
        // opening the raw legacy spelling. No portable-v2 parser is relaxed.
        legacy_route_destination(text)?;
        let exact = text.split('/').fold(PathBuf::new(), |mut path, component| {
            path.push(component);
            path
        });
        let mut candidates = vec![exact];
        if text.contains('\\')
            && let Ok(normalized) = canonical_inventory_relative_path(text)
            && normalized != candidates[0]
        {
            candidates.push(normalized);
        }
        return Ok(candidates);
    }
    if !text.contains('\\') {
        return wire_relative_path(text).map(|path| vec![path]);
    }
    if text.is_empty()
        || text.starts_with(['/', '\\'])
        || text.ends_with(['/', '\\'])
        || text
            .split(['/', '\\'])
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(validation("invalid legacy graph file path"));
    }

    let exact = text.split('/').fold(PathBuf::new(), |mut path, component| {
        path.push(component);
        path
    });
    let normalized = canonical_inventory_relative_path(text)?;
    if exact == normalized {
        Ok(vec![exact])
    } else {
        Ok(vec![exact, normalized])
    }
}

pub(crate) fn resolve_v1_inventory_entry(
    graph_root: &Path,
    entry: &GraphFileEntry,
) -> Result<PathBuf, GfError> {
    resolve_v1_inventory_entry_retained(graph_root, entry).map(|resolved| resolved.path)
}

pub(crate) struct RetainedV1InventoryEntry {
    pub(crate) path: PathBuf,
    pub(crate) file: File,
    pub(crate) identity: graphforge_filesystem::FileIdentity,
}

/// Retains the authenticated file at its post-hash cursor (EOF). Callers that
/// read the payload must rewind this same handle before reading.
pub(crate) fn resolve_v1_inventory_entry_retained(
    graph_root: &Path,
    entry: &GraphFileEntry,
) -> Result<RetainedV1InventoryEntry, GfError> {
    resolve_read_inventory_entry_retained(graph_root, &entry.into())
}

pub(crate) fn resolve_read_inventory_entry_retained(
    graph_root: &Path,
    entry: &crate::GraphReadFileEntry,
) -> Result<RetainedV1InventoryEntry, GfError> {
    let mut authenticated = Vec::new();
    for relative in inventory_relative_path_candidates(&entry.relative_path)? {
        let path = graph_root.join(&relative);
        let Ok(mut file) = open_retained_relative_file(graph_root, &relative) else {
            continue;
        };
        let metadata = file
            .metadata()
            .map_err(|error| storage("inspect retained graph file", &path, error))?;
        if !metadata.is_file() || metadata.len() != entry.byte_length {
            continue;
        }
        let identity = graphforge_filesystem::file_identity(&file)
            .map_err(|error| storage("identify retained graph file", &path, error))?;
        if graphforge_filesystem::path_identity(&path).ok() != Some(identity) {
            continue;
        }
        let intact = checksum_reader(&mut file, &path)?.0 == entry.content_xxh64;
        if intact && graphforge_filesystem::path_identity(&path).ok() == Some(identity) {
            authenticated.push(RetainedV1InventoryEntry {
                path,
                file,
                identity,
            });
        }
    }
    match authenticated.as_slice() {
        [resolved] => Ok(RetainedV1InventoryEntry {
            path: resolved.path.clone(),
            file: resolved.file.try_clone().map_err(|error| {
                storage("retain authenticated graph file", &resolved.path, error)
            })?,
            identity: resolved.identity,
        }),
        [] => Err(corrupt(
            "generation graph file has no authenticated legacy path resolution",
        )),
        _ => Err(corrupt(
            "generation graph file has ambiguous authenticated legacy path resolutions",
        )),
    }
}

pub(crate) fn open_retained_relative_file(
    graph_root: &Path,
    relative: &Path,
) -> std::io::Result<File> {
    let mut directory = graphforge_filesystem::StableDirectory::open(graph_root)?;
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(std::io::Error::other("graph file path is not canonical"));
        };
        if components.peek().is_some() {
            directory = directory.open_child_directory(name)?;
        } else {
            return directory.open_child_file(name);
        }
    }
    Err(std::io::Error::other("graph file path is empty"))
}

fn validate_wire_component(component: &str) -> Result<(), GfError> {
    if component.is_empty()
        || matches!(component, "." | "..")
        || component.ends_with([' ', '.'])
        || component.chars().any(|character| {
            character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        })
    {
        return Err(validation("graph file path has a non-portable component"));
    }
    let stem = component.split('.').next().unwrap_or(component);
    if matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    ) {
        return Err(validation("graph file path has a reserved component"));
    }
    Ok(())
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
mod tests;

/// Disposable, file-backed rollback state for a private graph workspace.
///
/// This is not a durable recovery protocol. Generation owners use their
/// authoritative parent instead; unpublished workspaces use this streaming-copy
/// fallback so abort never needs to materialize all graph bytes in memory.
/// The copy bounds memory, not total scratch-disk consumption.
pub struct GraphWorkspaceCheckpoint {
    source: PathBuf,
    backup: Option<tempfile::TempDir>,
    inventory: GraphFilesInventory,
    source_was_absent: bool,
}

/// Whether restoration retained an untouched absent target or restored a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphWorkspaceRestoration {
    /// The source was absent at capture and remains absent after an early error.
    Absent,
    /// A workspace tree was verified after restoration, possibly empty.
    Materialized,
}

impl GraphWorkspaceCheckpoint {
    /// Copy an admitted workspace using the existing validated graph inventory.
    ///
    /// # Errors
    /// Rejects links, invalid inventory and copy failures.
    pub fn capture(source: &Path) -> Result<Self, GfError> {
        let backup = tempfile::Builder::new()
            .prefix("graphforge-mutation-rollback-")
            .tempdir()
            .map_err(|error| storage("create mutation rollback directory", source, error))?;
        let (inventory, source_was_absent) = match fs::symlink_metadata(source) {
            Ok(_) => {
                let (inventory, _) = capture_graph_files(source)?;
                // A rollback snapshot preserves the admitted layout exactly;
                // ordinary hydration may instead upgrade legacy route names.
                for entry in &inventory.files {
                    let input = resolve_v1_inventory_entry(source, entry)?;
                    let relative = input
                        .strip_prefix(source)
                        .map_err(|_| validation("rollback source escaped workspace"))?;
                    let output = backup.path().join(relative);
                    if let Some(parent) = output.parent() {
                        fs::create_dir_all(parent).map_err(|error| {
                            storage("create rollback snapshot directory", parent, error)
                        })?;
                    }
                    copy_regular_file(&input, &output)?;
                    make_private_copy_owner_writable(&output)?;
                }
                verify_graph_tree(backup.path(), &inventory)?;
                (inventory, false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Keep a fresh target absent until lowering captures its empty
                // schema. GraphWriter creates it only after that boundary.
                (capture_graph_files(backup.path())?.0, true)
            }
            Err(error) => return Err(storage("inspect mutation workspace", source, error)),
        };
        Ok(Self {
            source: source.to_path_buf(),
            backup: Some(backup),
            inventory,
            source_was_absent,
        })
    }

    /// Restore data files while preserving operational lock/cache files.
    /// Callers must hold mutation admission and establish publication authority
    /// before invoking this method.
    ///
    /// # Errors
    /// Fails closed if the backup, destination or restoration cannot be verified.
    pub fn restore(&mut self, target: &Path) -> Result<GraphWorkspaceRestoration, GfError> {
        if target != self.source {
            return Err(validation(
                "mutation checkpoint target differs from captured workspace",
            ));
        }
        match self.restore_inner(target) {
            Ok(restoration) => Ok(restoration),
            Err(error) => {
                let Some(backup) = self.backup.take() else {
                    return Err(error);
                };
                let backup = backup.keep();
                Err(GfError::Storage(format!(
                    "mutation restore failed; rollback backup retained at {}: {error}",
                    backup.display()
                )))
            }
        }
    }

    fn restore_inner(&self, target: &Path) -> Result<GraphWorkspaceRestoration, GfError> {
        let backup = self
            .backup
            .as_ref()
            .ok_or_else(|| validation("mutation checkpoint already retained after failure"))?;
        verify_graph_tree(backup.path(), &self.inventory)?;
        let metadata = match fs::symlink_metadata(target) {
            Err(error)
                if self.source_was_absent && error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(GraphWorkspaceRestoration::Absent);
            }
            Err(error) => return Err(storage("inspect mutation restore target", target, error)),
            Ok(metadata) => metadata,
        };
        // Validate the root before read_dir can follow a substituted link and
        // before removing any current file, including on originally absent roots.
        reject_link(target)?;
        if !metadata.is_dir() {
            return Err(validation("mutation restore target must be a directory"));
        }
        let mut current = Vec::new();
        collect_source_files(target, &mut current)?;
        for path in current {
            fs::remove_file(&path)
                .map_err(|error| storage("remove aborted mutation file", &path, error))?;
        }
        for entry in &self.inventory.files {
            let source = resolve_v1_inventory_entry(backup.path(), entry)?;
            let relative = source
                .strip_prefix(backup.path())
                .map_err(|_| validation("rollback source escaped backup"))?;
            let destination = target.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| storage("restore mutation directory", parent, error))?;
            }
            copy_regular_file(&source, &destination)?;
            make_private_copy_owner_writable(&destination)?;
            crate::project_failpoint::hit(
                "mutation.restore.after_copy",
                None,
                None,
                "MUTATION_RESTORE",
                false,
            )?;
        }
        verify_graph_tree(target, &self.inventory)?;
        Ok(GraphWorkspaceRestoration::Materialized)
    }
}
