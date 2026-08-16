//! Authoritative durable graph mutation delta journal (ADR 0019 / #752).
//!
//! Base Parquet plus immutable ordered `.gfdr` runs form one
//! inventory-verified graph generation. Derived adjacency deltas remain a
//! separate rebuildable accelerator (`adjacency_delta`).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_files::{
    GraphFileEntry, GraphFileRole, GraphFilesInventory, capture_graph_files, verify_graph_tree,
};
use crate::project_generation::resolve_project_generation;
use crate::project_publication::{
    ProjectCapability, ProjectGenerationRequest, ProjectPublicationReceipt, ProjectStageOutcome,
    published_project_transaction, stage_project_generation_with_graph_tree,
};
use crate::{GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, empty_workspace_participants};

/// Run file format identity implemented by this release.
pub const GRAPH_DELTA_RUN_FORMAT_VERSION: u32 = 1;
/// Record frame version implemented by this release.
pub const GRAPH_DELTA_RECORD_VERSION: u16 = 1;
/// File extension for authoritative delta runs.
pub const GRAPH_DELTA_RUN_EXTENSION: &str = "gfdr";
/// Directory holding authoritative runs beneath a generation graph tree.
pub const GRAPH_DELTA_DIR: &str = "deltas";

/// Maximum contiguous runs retained in one generation before compaction (#753).
pub const MAX_GRAPH_DELTA_RUNS: u64 = 64;
/// Maximum encoded bytes for one run file (including framing).
pub const MAX_GRAPH_DELTA_RUN_BYTES: usize = 16 * 1024 * 1024;
/// Maximum framed records in one run.
pub const MAX_GRAPH_DELTA_RECORDS_PER_RUN: usize = 100_000;
/// Maximum payload bytes for one framed record.
pub const MAX_GRAPH_DELTA_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Estimated replay memory budget (logical row + property maps).
pub const MAX_GRAPH_DELTA_REPLAY_MEMORY_BYTES: usize = 256 * 1024 * 1024;

const MAGIC: &[u8; 4] = b"GFDR";
const SCHEMA_JSON_V1: u16 = 1;

/// Bounds enforced for encode, open validation, and replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphDeltaJournalLimits {
    /// Maximum runs in one generation chain.
    pub max_runs: u64,
    /// Maximum bytes per run file.
    pub max_run_bytes: usize,
    /// Maximum records per run.
    pub max_records_per_run: usize,
    /// Maximum payload bytes per record.
    pub max_payload_bytes: usize,
    /// Estimated replay memory ceiling.
    pub max_replay_memory_bytes: usize,
}

impl Default for GraphDeltaJournalLimits {
    fn default() -> Self {
        Self {
            max_runs: MAX_GRAPH_DELTA_RUNS,
            max_run_bytes: MAX_GRAPH_DELTA_RUN_BYTES,
            max_records_per_run: MAX_GRAPH_DELTA_RECORDS_PER_RUN,
            max_payload_bytes: MAX_GRAPH_DELTA_PAYLOAD_BYTES,
            max_replay_memory_bytes: MAX_GRAPH_DELTA_REPLAY_MEMORY_BYTES,
        }
    }
}

/// Supported authoritative mutation kinds for current graph surfaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum GraphDeltaOpKind {
    /// Insert or replace a node identity row.
    UpsertNode = 1,
    /// Delete a node by UUID.
    DeleteNode = 2,
    /// Insert or replace an edge identity row.
    UpsertEdge = 3,
    /// Delete an edge by UUID.
    DeleteEdge = 4,
    /// Set one node property.
    SetNodeProperty = 5,
    /// Remove one node property.
    RemoveNodeProperty = 6,
    /// Set one edge property.
    SetEdgeProperty = 7,
    /// Remove one edge property.
    RemoveEdgeProperty = 8,
}

impl GraphDeltaOpKind {
    fn from_u8(value: u8) -> Result<Self, GfError> {
        match value {
            1 => Ok(Self::UpsertNode),
            2 => Ok(Self::DeleteNode),
            3 => Ok(Self::UpsertEdge),
            4 => Ok(Self::DeleteEdge),
            5 => Ok(Self::SetNodeProperty),
            6 => Ok(Self::RemoveNodeProperty),
            7 => Ok(Self::SetEdgeProperty),
            8 => Ok(Self::RemoveEdgeProperty),
            other => Err(validation(format!(
                "unsupported graph delta mutation kind {other}"
            ))),
        }
    }
}

/// One framed mutation before encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaOp {
    /// Stable operation identity for idempotent replay.
    pub operation_uuid: Uuid,
    /// Mutation kind.
    pub kind: GraphDeltaOpKind,
    /// Canonical JSON payload matching schema id 1.
    pub payload: GraphDeltaPayload,
}

/// Typed payloads for supported mutation kinds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GraphDeltaPayload {
    /// Node upsert.
    UpsertNode {
        /// Node UUID (hyphenated lowercase).
        node_uuid: String,
        /// Complete label type id set.
        type_ids: Vec<u32>,
    },
    /// Node delete.
    DeleteNode {
        /// Node UUID.
        node_uuid: String,
    },
    /// Edge upsert.
    UpsertEdge {
        /// Edge UUID.
        edge_uuid: String,
        /// Source node UUID.
        src_uuid: String,
        /// Destination node UUID.
        dst_uuid: String,
        /// Relation type name (exploratory or typed stem).
        rel_type: String,
    },
    /// Edge delete.
    DeleteEdge {
        /// Edge UUID.
        edge_uuid: String,
    },
    /// Node property set.
    SetNodeProperty {
        /// Node UUID.
        node_uuid: String,
        /// Property key.
        key: String,
        /// Canonical scalar text encoding.
        value: String,
    },
    /// Node property remove.
    RemoveNodeProperty {
        /// Node UUID.
        node_uuid: String,
        /// Property key.
        key: String,
    },
    /// Edge property set.
    SetEdgeProperty {
        /// Edge UUID.
        edge_uuid: String,
        /// Property key.
        key: String,
        /// Canonical scalar text encoding.
        value: String,
    },
    /// Edge property remove.
    RemoveEdgeProperty {
        /// Edge UUID.
        edge_uuid: String,
        /// Property key.
        key: String,
    },
}

impl GraphDeltaPayload {
    fn expected_kind(&self) -> GraphDeltaOpKind {
        match self {
            Self::UpsertNode { .. } => GraphDeltaOpKind::UpsertNode,
            Self::DeleteNode { .. } => GraphDeltaOpKind::DeleteNode,
            Self::UpsertEdge { .. } => GraphDeltaOpKind::UpsertEdge,
            Self::DeleteEdge { .. } => GraphDeltaOpKind::DeleteEdge,
            Self::SetNodeProperty { .. } => GraphDeltaOpKind::SetNodeProperty,
            Self::RemoveNodeProperty { .. } => GraphDeltaOpKind::RemoveNodeProperty,
            Self::SetEdgeProperty { .. } => GraphDeltaOpKind::SetEdgeProperty,
            Self::RemoveEdgeProperty { .. } => GraphDeltaOpKind::RemoveEdgeProperty,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, GfError> {
        serde_json::to_vec(self)
            .map_err(|error| validation(format!("graph delta payload encode failed: {error}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self, GfError> {
        serde_json::from_slice(bytes)
            .map_err(|error| corrupt(format!("graph delta payload decode failed: {error}")))
    }
}

/// Decoded framed record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaRecord {
    /// Record contract version.
    pub record_version: u16,
    /// Operation UUID.
    pub operation_uuid: Uuid,
    /// Contiguous sequence inside the run (0-based).
    pub op_sequence: u32,
    /// Mutation kind.
    pub kind: GraphDeltaOpKind,
    /// Payload schema id.
    pub schema_id: u16,
    /// Decoded payload.
    pub payload: GraphDeltaPayload,
}

/// One verified delta run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaRun {
    /// Contiguous 1-based sequence in the generation.
    pub run_sequence: u64,
    /// Run identity.
    pub run_uuid: Uuid,
    /// Publishing transaction identity.
    pub transaction_uuid: Uuid,
    /// Ordered records.
    pub records: Vec<GraphDeltaRecord>,
    /// Exact on-disk bytes.
    pub bytes: Vec<u8>,
}

/// Logical reconstructed graph after base load + ordered replay.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconstructedGraphState {
    /// Surviving nodes: uuid -> sorted type ids.
    pub nodes: BTreeMap<String, Vec<u32>>,
    /// Surviving edges: uuid -> (src, dst, rel_type).
    pub edges: BTreeMap<String, (String, String, String)>,
    /// Node properties: (node_uuid, key) -> value.
    pub node_properties: BTreeMap<(String, String), String>,
    /// Edge properties: (edge_uuid, key) -> value.
    pub edge_properties: BTreeMap<(String, String), String>,
    /// Operation UUIDs already applied (idempotency).
    pub applied_operations: BTreeMap<String, GraphDeltaPayload>,
}

impl ReconstructedGraphState {
    /// Canonical fingerprint over reconstructed logical state.
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"graphforge-graph-delta-state/1\n");
        for (uuid, types) in &self.nodes {
            hasher.update(uuid.as_bytes());
            hasher.update(b"|");
            for type_id in types {
                hasher.update(type_id.to_le_bytes());
            }
            hasher.update(b"\n");
        }
        hasher.update(b"--edges--\n");
        for (uuid, (src, dst, rel)) in &self.edges {
            hasher.update(uuid.as_bytes());
            hasher.update(b"|");
            hasher.update(src.as_bytes());
            hasher.update(b"|");
            hasher.update(dst.as_bytes());
            hasher.update(b"|");
            hasher.update(rel.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"--node-props--\n");
        for ((uuid, key), value) in &self.node_properties {
            hasher.update(uuid.as_bytes());
            hasher.update(b"|");
            hasher.update(key.as_bytes());
            hasher.update(b"|");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"--edge-props--\n");
        for ((uuid, key), value) in &self.edge_properties {
            hasher.update(uuid.as_bytes());
            hasher.update(b"|");
            hasher.update(key.as_bytes());
            hasher.update(b"|");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
        hasher.finalize().into()
    }

    /// Estimated logical memory for resource-limit enforcement.
    #[must_use]
    pub fn estimated_memory(&self) -> usize {
        let nodes = self.nodes.len().saturating_mul(64);
        let edges = self.edges.len().saturating_mul(128);
        let nprops = self.node_properties.len().saturating_mul(96);
        let eprops = self.edge_properties.len().saturating_mul(96);
        let ops = self.applied_operations.len().saturating_mul(128);
        nodes
            .saturating_add(edges)
            .saturating_add(nprops)
            .saturating_add(eprops)
            .saturating_add(ops)
    }
}

/// Evidence from validating and replaying a generation's delta chain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GraphDeltaReplayEvidence {
    /// Base generation UUID selected by CURRENT (caller-supplied context).
    pub base_generation_uuid: Option<Uuid>,
    /// Number of verified runs replayed.
    pub runs_replayed: u64,
    /// Total framed records applied (including idempotent no-ops).
    pub records_seen: u64,
    /// Exact run file bytes validated.
    pub run_bytes_validated: u64,
    /// Estimated peak logical replay memory.
    pub estimated_replay_memory_bytes: u64,
    /// Canonical reconstructed fingerprint.
    pub state_fingerprint: [u8; 32],
}

/// Input for publishing one small-write delta generation.
#[derive(Clone, Debug)]
pub struct GraphDeltaPublishRequest {
    /// Caller-stable transaction UUID (publication idempotency).
    pub transaction_uuid: Uuid,
    /// Generation UUID to publish.
    pub generation_uuid: Uuid,
    /// Run UUID for the new append-only run.
    pub run_uuid: Uuid,
    /// Ordered operations in the new run.
    pub operations: Vec<GraphDeltaOp>,
    /// Resource limits.
    pub limits: GraphDeltaJournalLimits,
}

/// Receipt for a small-write delta publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaPublicationReceipt {
    /// Underlying project publication receipt.
    pub publication: ProjectPublicationReceipt,
    /// Sequence assigned to the new run.
    pub run_sequence: u64,
    /// Whether Parquet base digests were byte-preserved from the parent.
    pub preserved_base_parquet_digests: bool,
    /// Count of base (non-delta) files whose digests matched the parent.
    pub unchanged_base_files: u64,
    /// Reconstructed fingerprint after the new run.
    pub state_fingerprint: [u8; 32],
}

/// Relative path for run sequence `n` (`1..=MAX`).
#[must_use]
pub fn delta_run_relative_path(run_sequence: u64) -> String {
    format!("{GRAPH_DELTA_DIR}/run_{run_sequence:016}.{GRAPH_DELTA_RUN_EXTENSION}")
}

/// Encode one immutable run file.
///
/// # Errors
/// Returns validation or resource-limit errors for empty ops, oversized
/// payloads, kind/payload mismatch, or limit exhaustion.
pub fn encode_delta_run(
    run_sequence: u64,
    run_uuid: Uuid,
    transaction_uuid: Uuid,
    operations: &[GraphDeltaOp],
    limits: GraphDeltaJournalLimits,
) -> Result<Vec<u8>, GfError> {
    if run_sequence == 0 || run_sequence > limits.max_runs {
        return Err(validation("graph delta run_sequence out of bounds"));
    }
    if operations.is_empty() {
        return Err(validation(
            "graph delta run must contain at least one operation",
        ));
    }
    if operations.len() > limits.max_records_per_run {
        return Err(resource_limit("graph delta records per run"));
    }

    let mut seen_ops = BTreeSet::new();
    let mut body = Vec::new();
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&GRAPH_DELTA_RUN_FORMAT_VERSION.to_le_bytes());
    body.extend_from_slice(&run_sequence.to_le_bytes());
    body.extend_from_slice(run_uuid.as_bytes());
    body.extend_from_slice(transaction_uuid.as_bytes());
    let record_count = u32::try_from(operations.len())
        .map_err(|_| validation("graph delta record count overflow"))?;
    body.extend_from_slice(&record_count.to_le_bytes());
    let header_checksum = Sha256::digest(&body);
    body.extend_from_slice(&header_checksum);

    for (index, op) in operations.iter().enumerate() {
        if !seen_ops.insert(op.operation_uuid) {
            return Err(validation(
                "graph delta run contains duplicate operation_uuid",
            ));
        }
        if op.payload.expected_kind() != op.kind {
            return Err(validation(
                "graph delta operation kind does not match payload",
            ));
        }
        let payload = op.payload.encode()?;
        if payload.len() > limits.max_payload_bytes {
            return Err(resource_limit("graph delta payload bytes"));
        }
        let op_sequence = u32::try_from(index).map_err(|_| validation("op sequence overflow"))?;
        let mut record = Vec::new();
        record.extend_from_slice(&GRAPH_DELTA_RECORD_VERSION.to_le_bytes());
        record.extend_from_slice(op.operation_uuid.as_bytes());
        record.extend_from_slice(&op_sequence.to_le_bytes());
        record.push(op.kind as u8);
        record.extend_from_slice(&SCHEMA_JSON_V1.to_le_bytes());
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| validation("graph delta payload length overflow"))?;
        record.extend_from_slice(&payload_len.to_le_bytes());
        record.extend_from_slice(&payload);
        let record_checksum = Sha256::digest(&record);
        body.extend_from_slice(&record);
        body.extend_from_slice(&record_checksum);
        if body.len() > limits.max_run_bytes.saturating_sub(32) {
            return Err(resource_limit("graph delta run bytes"));
        }
    }

    let file_checksum = Sha256::digest(&body);
    body.extend_from_slice(&file_checksum);
    if body.len() > limits.max_run_bytes {
        return Err(resource_limit("graph delta run bytes"));
    }
    Ok(body)
}

/// Decode and mechanically validate one run file.
///
/// # Errors
/// Returns corruption errors for torn, truncated, reordered, duplicated, or
/// checksum-invalid bytes. Unknown format versions are unsupported.
#[allow(clippy::too_many_lines)] // Framed decode must check every field fail-closed.
pub fn decode_delta_run(
    bytes: &[u8],
    expected_sequence: Option<u64>,
    limits: GraphDeltaJournalLimits,
) -> Result<GraphDeltaRun, GfError> {
    if bytes.len() > limits.max_run_bytes {
        return Err(resource_limit("graph delta run bytes"));
    }
    if bytes.len() < 4 + 4 + 8 + 16 + 16 + 4 + 32 + 32 {
        return Err(corrupt("graph delta run truncated before header"));
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("graph delta run magic mismatch"));
    }
    let format_version = read_u32(bytes, 4)?;
    if format_version != GRAPH_DELTA_RUN_FORMAT_VERSION {
        return Err(unsupported_run_version(format_version));
    }
    let run_sequence = read_u64(bytes, 8)?;
    if expected_sequence.is_some_and(|expected| run_sequence != expected) {
        return Err(corrupt("graph delta run sequence mismatch"));
    }
    if run_sequence == 0 || run_sequence > limits.max_runs {
        return Err(corrupt("graph delta run sequence out of bounds"));
    }
    let run_uuid = read_uuid(bytes, 16)?;
    let transaction_uuid = read_uuid(bytes, 32)?;
    let record_count = read_u32(bytes, 48)? as usize;
    if record_count == 0 || record_count > limits.max_records_per_run {
        return Err(corrupt("graph delta record count invalid"));
    }
    let header_end = 52;
    let stored_header_checksum = &bytes[header_end..header_end + 32];
    let actual_header_checksum = Sha256::digest(&bytes[..header_end]);
    if stored_header_checksum != actual_header_checksum.as_slice() {
        return Err(corrupt("graph delta run header checksum mismatch"));
    }

    let file_checksum_offset = bytes.len().saturating_sub(32);
    if file_checksum_offset <= header_end + 32 {
        return Err(corrupt("graph delta run truncated"));
    }
    let stored_file_checksum = &bytes[file_checksum_offset..];
    let actual_file_checksum = Sha256::digest(&bytes[..file_checksum_offset]);
    if stored_file_checksum != actual_file_checksum.as_slice() {
        return Err(corrupt("graph delta run file checksum mismatch"));
    }

    let mut cursor = header_end + 32;
    let mut records = Vec::with_capacity(record_count);
    let mut seen_ops = BTreeSet::new();
    for expected_op_sequence in 0..record_count {
        if cursor + 2 + 16 + 4 + 1 + 2 + 4 + 32 > file_checksum_offset {
            return Err(corrupt("graph delta run truncated mid-record"));
        }
        let record_start = cursor;
        let record_version = read_u16(bytes, cursor)?;
        cursor += 2;
        if record_version != GRAPH_DELTA_RECORD_VERSION {
            return Err(unsupported_run_version(u32::from(record_version)));
        }
        let operation_uuid = read_uuid(bytes, cursor)?;
        cursor += 16;
        if !seen_ops.insert(operation_uuid) {
            return Err(corrupt("graph delta run has duplicated operation_uuid"));
        }
        let op_sequence = read_u32(bytes, cursor)?;
        cursor += 4;
        if op_sequence as usize != expected_op_sequence {
            return Err(corrupt("graph delta op_sequence reordered or gapped"));
        }
        let kind = GraphDeltaOpKind::from_u8(bytes[cursor])?;
        cursor += 1;
        let schema_id = read_u16(bytes, cursor)?;
        cursor += 2;
        if schema_id != SCHEMA_JSON_V1 {
            return Err(unsupported_run_version(u32::from(schema_id)));
        }
        let payload_len = read_u32(bytes, cursor)? as usize;
        cursor += 4;
        if payload_len > limits.max_payload_bytes {
            return Err(resource_limit("graph delta payload bytes"));
        }
        if cursor + payload_len + 32 > file_checksum_offset {
            return Err(corrupt("graph delta record payload truncated"));
        }
        let payload_bytes = &bytes[cursor..cursor + payload_len];
        cursor += payload_len;
        let stored_record_checksum = &bytes[cursor..cursor + 32];
        let actual_record_checksum = Sha256::digest(&bytes[record_start..cursor]);
        if stored_record_checksum != actual_record_checksum.as_slice() {
            return Err(corrupt("graph delta record checksum mismatch"));
        }
        cursor += 32;
        let payload = GraphDeltaPayload::decode(payload_bytes)?;
        if payload.expected_kind() != kind {
            return Err(corrupt("graph delta record kind/payload mismatch"));
        }
        records.push(GraphDeltaRecord {
            record_version,
            operation_uuid,
            op_sequence,
            kind,
            schema_id,
            payload,
        });
    }
    if cursor != file_checksum_offset {
        return Err(corrupt(
            "graph delta run has trailing garbage before checksum",
        ));
    }
    if records.len() != record_count {
        return Err(corrupt("graph delta record count mismatch"));
    }

    Ok(GraphDeltaRun {
        run_sequence,
        run_uuid,
        transaction_uuid,
        records,
        bytes: bytes.to_vec(),
    })
}

/// Split inventory entries into ordered delta runs.
///
/// # Errors
/// Fails closed on gaps, duplicates, unknown extensions under `deltas/`, or
/// non-contiguous sequences.
pub fn list_delta_runs(
    inventory: &GraphFilesInventory,
    limits: GraphDeltaJournalLimits,
) -> Result<Vec<&GraphFileEntry>, GfError> {
    let mut runs = Vec::new();
    for entry in &inventory.files {
        if entry.relative_path == format!("{GRAPH_DELTA_DIR}/.base_state.json") {
            continue;
        }
        if !entry.relative_path.starts_with("deltas/") {
            continue;
        }
        if entry.role != GraphFileRole::Delta {
            return Err(corrupt("delta path has non-delta inventory role"));
        }
        let sequence = parse_run_sequence(&entry.relative_path)?;
        runs.push((sequence, entry));
    }
    runs.sort_by_key(|(sequence, _)| *sequence);
    if runs.len() as u64 > limits.max_runs {
        return Err(resource_limit("graph delta runs per generation"));
    }
    for (index, (sequence, _)) in runs.iter().enumerate() {
        let expected = (index as u64).saturating_add(1);
        if *sequence != expected {
            return Err(corrupt(
                "graph delta run sequence missing, duplicated, or reordered",
            ));
        }
    }
    Ok(runs.into_iter().map(|(_, entry)| entry).collect())
}

/// Load, verify, and order every delta run under `graph_root` for `inventory`.
///
/// # Errors
/// Missing, extra, torn, or checksum-invalid runs fail closed.
pub fn load_verified_delta_runs(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    limits: GraphDeltaJournalLimits,
) -> Result<Vec<GraphDeltaRun>, GfError> {
    verify_graph_tree(graph_root, inventory)?;
    let entries = list_delta_runs(inventory, limits)?;
    let mut runs = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let expected = (index as u64).saturating_add(1);
        if usize::try_from(entry.byte_length).unwrap_or(usize::MAX) > limits.max_run_bytes {
            return Err(resource_limit("graph delta run bytes"));
        }
        let path = graph_root.join(&entry.relative_path);
        let bytes = fs::read(&path).map_err(|error| storage("read delta run", &path, error))?;
        if bytes.len() as u64 != entry.byte_length {
            return Err(corrupt("graph delta run length mismatch"));
        }
        let digest = hex_digest(Sha256::digest(&bytes).into());
        if digest != entry.content_sha256 {
            return Err(corrupt("graph delta run digest mismatch"));
        }
        let run = decode_delta_run(&bytes, Some(expected), limits)?;
        runs.push(run);
    }
    Ok(runs)
}

/// Apply verified runs onto `state` with idempotent operation semantics.
///
/// # Errors
/// Conflicting operation UUID reuse returns `GF_IDEMPOTENCY_CONFLICT`.
pub fn apply_delta_runs(
    state: &mut ReconstructedGraphState,
    runs: &[GraphDeltaRun],
    limits: GraphDeltaJournalLimits,
) -> Result<GraphDeltaReplayEvidence, GfError> {
    let mut evidence = GraphDeltaReplayEvidence::default();
    for run in runs {
        evidence.runs_replayed = evidence.runs_replayed.saturating_add(1);
        evidence.run_bytes_validated = evidence
            .run_bytes_validated
            .saturating_add(run.bytes.len() as u64);
        for record in &run.records {
            evidence.records_seen = evidence.records_seen.saturating_add(1);
            apply_one(state, record)?;
            let memory = state.estimated_memory();
            if memory > limits.max_replay_memory_bytes {
                return Err(resource_limit("graph delta replay memory"));
            }
            evidence.estimated_replay_memory_bytes =
                evidence.estimated_replay_memory_bytes.max(memory as u64);
        }
    }
    evidence.state_fingerprint = state.fingerprint();
    Ok(evidence)
}

/// Reconstruct logical graph state from a verified graph tree + inventory.
///
/// # Errors
/// Fail-closed validation/corruption/resource errors.
pub fn reconstruct_graph_state(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    limits: GraphDeltaJournalLimits,
) -> Result<(ReconstructedGraphState, GraphDeltaReplayEvidence), GfError> {
    let runs = load_verified_delta_runs(graph_root, inventory, limits)?;
    let mut state = load_base_state(graph_root)?;
    let evidence = apply_delta_runs(&mut state, &runs, limits)?;
    Ok((state, evidence))
}

/// Publish one small-write generation that preserves unchanged base Parquet.
///
/// # Errors
/// Unsupported kinds are rejected before staging. Publication and idempotency
/// errors follow the project generation protocol.
#[allow(clippy::too_many_lines)] // Publication stages copy, encode, and CURRENT commit together.
pub fn publish_graph_delta(
    container_root: &Path,
    request: &GraphDeltaPublishRequest,
) -> Result<GraphDeltaPublicationReceipt, GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let admitted_root = admission.root().to_owned();
    let container_root = admitted_root.as_path();
    for op in &request.operations {
        if op.payload.expected_kind() != op.kind {
            return Err(validation(
                "graph delta operation kind does not match payload",
            ));
        }
    }

    if let Some(publication) =
        published_project_transaction(container_root, request.transaction_uuid)?
    {
        if publication.generation_uuid != request.generation_uuid {
            return Err(idempotency_conflict(
                "graph delta transaction_uuid reused with different generation_uuid",
            ));
        }
        let resolved = resolve_project_generation(container_root)?;
        let inventory = resolved
            .graph_files_inventory()?
            .ok_or_else(|| corrupt("published generation missing graph inventory"))?;
        let (_state, evidence) =
            reconstruct_graph_state(&resolved.graph_tree_root(), &inventory, request.limits)?;
        let run_sequence = list_delta_runs(&inventory, request.limits)?.len() as u64;
        return Ok(GraphDeltaPublicationReceipt {
            publication,
            run_sequence,
            preserved_base_parquet_digests: true,
            unchanged_base_files: inventory
                .files
                .iter()
                .filter(|entry| !is_gfdr_path(&entry.relative_path))
                .count() as u64,
            state_fingerprint: evidence.state_fingerprint,
        });
    }

    let parent = resolve_project_generation(container_root)?;
    let parent_inventory = parent
        .graph_files_inventory()?
        .ok_or_else(|| validation("parent generation lacks graph/files inventory"))?;
    let parent_tree = parent.graph_tree_root();
    verify_graph_tree(&parent_tree, &parent_inventory)?;

    let parent_runs = load_verified_delta_runs(&parent_tree, &parent_inventory, request.limits)?;
    let mut committed_ops: BTreeMap<Uuid, GraphDeltaPayload> = BTreeMap::new();
    for run in &parent_runs {
        for record in &run.records {
            committed_ops.insert(record.operation_uuid, record.payload.clone());
        }
    }
    for op in &request.operations {
        if let Some(prior) = committed_ops.get(&op.operation_uuid) {
            if prior == &op.payload {
                return Err(idempotency_conflict(
                    "graph delta operation_uuid already committed",
                ));
            }
            return Err(idempotency_conflict(
                "graph delta operation_uuid reused with different payload",
            ));
        }
    }

    let next_sequence = (parent_runs.len() as u64).saturating_add(1);
    if next_sequence > request.limits.max_runs {
        return Err(resource_limit("graph delta runs per generation"));
    }
    let run_bytes = encode_delta_run(
        next_sequence,
        request.run_uuid,
        request.transaction_uuid,
        &request.operations,
        request.limits,
    )?;

    let staging = tempfile::tempdir().map_err(|error| {
        GfError::Storage(format!("create graph delta staging directory: {error}"))
    })?;
    for entry in &parent_inventory.files {
        let source = parent_tree.join(&entry.relative_path);
        let destination = staging.path().join(&entry.relative_path);
        if let Some(parent_dir) = destination.parent() {
            fs::create_dir_all(parent_dir)
                .map_err(|error| storage("create delta staging parent", parent_dir, error))?;
        }
        fs::copy(&source, &destination)
            .map_err(|error| storage("copy parent graph file", &source, error))?;
    }
    let new_relative = delta_run_relative_path(next_sequence);
    let new_path = staging.path().join(&new_relative);
    if let Some(parent_dir) = new_path.parent() {
        fs::create_dir_all(parent_dir)
            .map_err(|error| storage("create deltas directory", parent_dir, error))?;
    }
    {
        let mut file = File::create(&new_path)
            .map_err(|error| storage("create delta run", &new_path, error))?;
        file.write_all(&run_bytes)
            .map_err(|error| storage("write delta run", &new_path, error))?;
        file.sync_all()
            .map_err(|error| storage("flush delta run", &new_path, error))?;
    }

    let (inventory, files_participant) = capture_graph_files(staging.path())?;
    let unchanged_base_files = count_preserved_base_files(&parent_inventory, &inventory);
    let parent_base_count = parent_inventory
        .files
        .iter()
        .filter(|entry| {
            !entry.relative_path.starts_with("deltas/")
                || entry.relative_path.ends_with(".base_state.json")
        })
        .filter(|entry| !is_gfdr_path(&entry.relative_path))
        .count() as u64;
    let preserved_base_parquet_digests = unchanged_base_files == parent_base_count
        && parent_inventory
            .files
            .iter()
            .filter(|entry| {
                Path::new(&entry.relative_path)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
            })
            .all(|parent_entry| {
                inventory.files.iter().any(|child| {
                    child.relative_path == parent_entry.relative_path
                        && child.content_sha256 == parent_entry.content_sha256
                })
            });

    let mut participants = empty_workspace_participants()?;
    participants.insert(0, files_participant);
    let generation_request = ProjectGenerationRequest {
        transaction_uuid: request.transaction_uuid,
        generation_uuid: request.generation_uuid,
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

    drop(admission);
    let publication = match stage_project_generation_with_graph_tree(
        container_root,
        &generation_request,
        Some(staging.path()),
    )? {
        ProjectStageOutcome::Staged(staged) => {
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?
        }
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
    };

    let resolved = resolve_project_generation(container_root)?;
    let child_inventory = resolved
        .graph_files_inventory()?
        .ok_or_else(|| corrupt("published generation missing graph inventory"))?;
    let (_state, mut evidence) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &child_inventory,
        request.limits,
    )?;
    evidence.base_generation_uuid = Some(parent.generation_uuid());

    Ok(GraphDeltaPublicationReceipt {
        publication,
        run_sequence: next_sequence,
        preserved_base_parquet_digests,
        unchanged_base_files,
        state_fingerprint: evidence.state_fingerprint,
    })
}

/// Seed a workspace with opaque base files and optional base-state marker.
///
/// # Errors
/// Returns storage errors when directories or files cannot be written.
pub fn stage_base_graph_workspace(
    workspace: &Path,
    files: &[(&str, &[u8])],
    base_state: Option<&ReconstructedGraphState>,
) -> Result<(), GfError> {
    for (relative, bytes) in files {
        let path = workspace.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create base workspace dir", parent, error))?;
        }
        fs::write(&path, bytes)
            .map_err(|error| storage("write base workspace file", &path, error))?;
    }
    if let Some(state) = base_state {
        let marker = workspace.join(GRAPH_DELTA_DIR).join(".base_state.json");
        if let Some(parent) = marker.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create base state dir", parent, error))?;
        }
        let bytes = serde_json::to_vec(state)
            .map_err(|error| validation(format!("base state encode failed: {error}")))?;
        fs::write(&marker, bytes)
            .map_err(|error| storage("write base state marker", &marker, error))?;
    }
    Ok(())
}

fn load_base_state(graph_root: &Path) -> Result<ReconstructedGraphState, GfError> {
    let marker = graph_root.join(GRAPH_DELTA_DIR).join(".base_state.json");
    if !marker.exists() {
        return Ok(ReconstructedGraphState::default());
    }
    let bytes =
        fs::read(&marker).map_err(|error| storage("read base state marker", &marker, error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| corrupt(format!("invalid base state marker: {error}")))
}

fn apply_one(
    state: &mut ReconstructedGraphState,
    record: &GraphDeltaRecord,
) -> Result<(), GfError> {
    let key = record.operation_uuid.hyphenated().to_string();
    if let Some(prior) = state.applied_operations.get(&key) {
        if prior == &record.payload {
            return Ok(());
        }
        return Err(idempotency_conflict(
            "graph delta operation_uuid reused with different payload",
        ));
    }
    match &record.payload {
        GraphDeltaPayload::UpsertNode {
            node_uuid,
            type_ids,
        } => {
            let mut sorted = type_ids.clone();
            sorted.sort_unstable();
            state.nodes.insert(node_uuid.clone(), sorted);
        }
        GraphDeltaPayload::DeleteNode { node_uuid } => {
            state.nodes.remove(node_uuid);
            state
                .node_properties
                .retain(|(uuid, _), _| uuid != node_uuid);
        }
        GraphDeltaPayload::UpsertEdge {
            edge_uuid,
            src_uuid,
            dst_uuid,
            rel_type,
        } => {
            state.edges.insert(
                edge_uuid.clone(),
                (src_uuid.clone(), dst_uuid.clone(), rel_type.clone()),
            );
        }
        GraphDeltaPayload::DeleteEdge { edge_uuid } => {
            state.edges.remove(edge_uuid);
            state
                .edge_properties
                .retain(|(uuid, _), _| uuid != edge_uuid);
        }
        GraphDeltaPayload::SetNodeProperty {
            node_uuid,
            key,
            value,
        } => {
            state
                .node_properties
                .insert((node_uuid.clone(), key.clone()), value.clone());
        }
        GraphDeltaPayload::RemoveNodeProperty { node_uuid, key } => {
            state
                .node_properties
                .remove(&(node_uuid.clone(), key.clone()));
        }
        GraphDeltaPayload::SetEdgeProperty {
            edge_uuid,
            key,
            value,
        } => {
            state
                .edge_properties
                .insert((edge_uuid.clone(), key.clone()), value.clone());
        }
        GraphDeltaPayload::RemoveEdgeProperty { edge_uuid, key } => {
            state
                .edge_properties
                .remove(&(edge_uuid.clone(), key.clone()));
        }
    }
    state.applied_operations.insert(key, record.payload.clone());
    Ok(())
}

fn is_gfdr_path(relative: &str) -> bool {
    Path::new(relative)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case(GRAPH_DELTA_RUN_EXTENSION))
}

fn count_preserved_base_files(parent: &GraphFilesInventory, child: &GraphFilesInventory) -> u64 {
    parent
        .files
        .iter()
        .filter(|entry| !is_gfdr_path(&entry.relative_path))
        .filter(|parent_entry| {
            child.files.iter().any(|child_entry| {
                child_entry.relative_path == parent_entry.relative_path
                    && child_entry.content_sha256 == parent_entry.content_sha256
                    && child_entry.byte_length == parent_entry.byte_length
            })
        })
        .count() as u64
}

fn parse_run_sequence(relative: &str) -> Result<u64, GfError> {
    let prefix = "deltas/run_";
    let suffix = format!(".{GRAPH_DELTA_RUN_EXTENSION}");
    let Some(rest) = relative.strip_prefix(prefix) else {
        return Err(corrupt("unexpected graph delta run path"));
    };
    let Some(digits) = rest.strip_suffix(&suffix) else {
        return Err(corrupt("unexpected graph delta run extension"));
    };
    if digits.len() != 16 || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(corrupt("graph delta run filename not canonical"));
    }
    digits
        .parse::<u64>()
        .map_err(|_| corrupt("graph delta run sequence parse failed"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, GfError> {
    let slice = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| corrupt("graph delta truncated u16"))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, GfError> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| corrupt("graph delta truncated u32"))?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, GfError> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt("graph delta truncated u64"))?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_uuid(bytes: &[u8], offset: usize) -> Result<Uuid, GfError> {
    let slice = bytes
        .get(offset..offset + 16)
        .ok_or_else(|| corrupt("graph delta truncated uuid"))?;
    Uuid::from_slice(slice).map_err(|_| corrupt("graph delta invalid uuid"))
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

fn unsupported_run_version(version: u32) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::UnsupportedProjectFormat,
        message: format!("unsupported graph delta run format version {version}"),
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Execution(format!("GF_RESOURCE_LIMIT: {}", message.into()))
}

fn idempotency_conflict(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn storage(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("{action} at {}: {error}", path.display()))
}

#[cfg(test)]
mod crash_oracle_tests {
    use crate::project_fault_oracle::{
        AuthorityClass, PublicationIds, PublicationPhase, default_durable_ids, expected_authority,
        publication_ops, simulate_crash,
    };

    #[test]
    fn crash_oracle_before_and_after_ack_matches_frozen_contract() {
        let seed = 752u64;
        let ids = PublicationIds::from_seed(seed);
        for phase in [
            PublicationPhase::BeforeCurrentReplace,
            PublicationPhase::AfterCurrentReplace,
            PublicationPhase::AfterRootFsync,
        ] {
            let ops = publication_ops(ids, phase);
            let durable = default_durable_ids(&ops, phase);
            let report = simulate_crash(seed, phase, &durable).unwrap();
            assert_eq!(report.expected, expected_authority(phase));
            assert_eq!(report.actual, report.expected);
            match phase {
                PublicationPhase::BeforeCurrentReplace => {
                    assert_eq!(report.expected, AuthorityClass::PriorGeneration);
                }
                PublicationPhase::AfterCurrentReplace | PublicationPhase::AfterRootFsync => {
                    assert_eq!(report.expected, AuthorityClass::NewGeneration);
                }
                _ => unreachable!(),
            }
        }
    }
}
