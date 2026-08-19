//! Authoritative durable graph mutation delta journal (ADR 0019 / #752).
//!
//! Base Parquet plus immutable ordered `.gfdr` runs form one
//! inventory-verified graph generation. Derived adjacency deltas remain a
//! separate rebuildable accelerator (`adjacency_delta`).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use arrow::array::{
    Array, FixedSizeBinaryArray, ListArray, StringArray, TimestampMicrosecondArray, UInt32Array,
    UInt64Array,
};
use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_ir::IrLiteral;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_files::{
    GraphFileEntry, GraphFileRole, GraphFilesInventory, capture_graph_files, verify_graph_tree,
};
use crate::project_generation::resolve_project_generation;
use crate::project_publication::{
    ProjectCapability, ProjectGenerationRequest, ProjectPublicationReceipt, ProjectStageOutcome,
    published_project_transaction, stage_project_generation_from_admitted_parent,
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
/// Maximum accepted Parquet footer metadata before decoder allocation.
pub const MAX_GRAPH_DELTA_PARQUET_METADATA_BYTES: usize = 16 * 1024 * 1024;
/// Maximum canonical rows scanned by one replay/materialization.
pub const MAX_GRAPH_DELTA_REPLAY_WORK_ROWS: u64 = 100_000_000;
/// Maximum decoded rows resident in one streaming Arrow batch.
pub const MAX_GRAPH_DELTA_REPLAY_BATCH_ROWS: usize = 8_192;

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
    /// Maximum footer metadata bytes accepted per canonical Parquet file.
    pub max_parquet_metadata_bytes: usize,
    /// Maximum total canonical rows scanned by replay.
    pub max_work_rows: u64,
    /// Maximum rows decoded into one resident Arrow batch.
    pub max_batch_rows: usize,
}

impl Default for GraphDeltaJournalLimits {
    fn default() -> Self {
        Self {
            max_runs: MAX_GRAPH_DELTA_RUNS,
            max_run_bytes: MAX_GRAPH_DELTA_RUN_BYTES,
            max_records_per_run: MAX_GRAPH_DELTA_RECORDS_PER_RUN,
            max_payload_bytes: MAX_GRAPH_DELTA_PAYLOAD_BYTES,
            max_replay_memory_bytes: MAX_GRAPH_DELTA_REPLAY_MEMORY_BYTES,
            max_parquet_metadata_bytes: MAX_GRAPH_DELTA_PARQUET_METADATA_BYTES,
            max_work_rows: MAX_GRAPH_DELTA_REPLAY_WORK_ROWS,
            max_batch_rows: MAX_GRAPH_DELTA_REPLAY_BATCH_ROWS,
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
    /// Lossless node upsert used by the typed GFDR contract.
    UpsertNodeV2 {
        /// Node UUID.
        node_uuid: String,
        /// Stable runtime surrogate identity.
        node_id: u64,
        /// Complete label type id set.
        type_ids: Vec<u32>,
        /// Creation timestamp in UTC microseconds.
        created_at_micros: i64,
        /// Last-update timestamp in UTC microseconds.
        updated_at_micros: i64,
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
    /// Lossless edge upsert used by the typed GFDR contract.
    UpsertEdgeV2 {
        /// Edge UUID.
        edge_uuid: String,
        /// Source node UUID.
        src_uuid: String,
        /// Destination node UUID.
        dst_uuid: String,
        /// Relation type name.
        rel_type: String,
        /// Stable edge surrogate identity.
        edge_id: u64,
        /// Stable source surrogate identity.
        src_id: u64,
        /// Stable destination surrogate identity.
        dst_id: u64,
        /// Creation timestamp in UTC microseconds.
        created_at_micros: i64,
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
        /// Canonical property-file stem used for routing.
        #[serde(default)]
        property_stem: String,
        /// Property key.
        key: String,
        /// Canonical scalar text encoding.
        value: String,
    },
    /// Node property remove.
    RemoveNodeProperty {
        /// Node UUID.
        node_uuid: String,
        /// Canonical property-file stem used for routing.
        #[serde(default)]
        property_stem: String,
        /// Property key.
        key: String,
    },
    /// Edge property set.
    SetEdgeProperty {
        /// Edge UUID.
        edge_uuid: String,
        /// Canonical edge-property-file stem used for routing.
        #[serde(default)]
        property_stem: String,
        /// Property key.
        key: String,
        /// Canonical scalar text encoding.
        value: String,
    },
    /// Edge property remove.
    RemoveEdgeProperty {
        /// Edge UUID.
        edge_uuid: String,
        /// Canonical edge-property-file stem used for routing.
        #[serde(default)]
        property_stem: String,
        /// Property key.
        key: String,
    },
}

impl GraphDeltaPayload {
    fn expected_kind(&self) -> GraphDeltaOpKind {
        match self {
            Self::UpsertNode { .. } | Self::UpsertNodeV2 { .. } => GraphDeltaOpKind::UpsertNode,
            Self::DeleteNode { .. } => GraphDeltaOpKind::DeleteNode,
            Self::UpsertEdge { .. } | Self::UpsertEdgeV2 { .. } => GraphDeltaOpKind::UpsertEdge,
            Self::DeleteEdge { .. } => GraphDeltaOpKind::DeleteEdge,
            Self::SetNodeProperty { .. } => GraphDeltaOpKind::SetNodeProperty,
            Self::RemoveNodeProperty { .. } => GraphDeltaOpKind::RemoveNodeProperty,
            Self::SetEdgeProperty { .. } => GraphDeltaOpKind::SetEdgeProperty,
            Self::RemoveEdgeProperty { .. } => GraphDeltaOpKind::RemoveEdgeProperty,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, GfError> {
        self.validate_typed_values()?;
        serde_json::to_vec(self)
            .map_err(|error| validation(format!("graph delta payload encode failed: {error}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self, GfError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| corrupt(format!("graph delta payload decode failed: {error}")))?;
        payload.validate_typed_values()?;
        Ok(payload)
    }

    fn validate_typed_values(&self) -> Result<(), GfError> {
        match self {
            Self::UpsertNode { .. } | Self::UpsertEdge { .. } => Err(GfError::Project {
                code: ProjectErrorCode::UnsupportedProjectFormat,
                message: "unsupported lossless-metadata-free GFDR topology payload".into(),
            }),
            Self::SetNodeProperty {
                property_stem,
                value,
                ..
            }
            | Self::SetEdgeProperty {
                property_stem,
                value,
                ..
            } => {
                if property_stem.is_empty() {
                    return Err(GfError::Project {
                        code: ProjectErrorCode::UnsupportedProjectFormat,
                        message: "unsupported routing-free GFDR property payload".into(),
                    });
                }
                decode_graph_delta_value(value).map(|_| ())
            }
            Self::RemoveNodeProperty { property_stem, .. }
            | Self::RemoveEdgeProperty { property_stem, .. }
                if property_stem.is_empty() =>
            {
                Err(GfError::Project {
                    code: ProjectErrorCode::UnsupportedProjectFormat,
                    message: "unsupported routing-free GFDR property removal payload".into(),
                })
            }
            _ => Ok(()),
        }
    }
}

/// Encode a property value for GFDR without collapsing its openCypher type.
///
/// # Errors
/// Returns validation errors when the typed literal cannot be serialized.
pub fn encode_graph_delta_value(value: &IrLiteral) -> Result<String, GfError> {
    serde_json::to_string(value)
        .map_err(|error| validation(format!("graph delta typed value encode failed: {error}")))
}

/// Decode GFDR's canonical typed property representation.
///
/// Plain strings emitted by the superseded prototype are deliberately rejected
/// instead of being silently reinterpreted as typed values.
///
/// # Errors
/// Returns `GF_UNSUPPORTED_PROJECT_FORMAT` for the old string-only encoding.
pub fn decode_graph_delta_value(encoded: &str) -> Result<IrLiteral, GfError> {
    serde_json::from_str(encoded).map_err(|error| GfError::Project {
        code: ProjectErrorCode::UnsupportedProjectFormat,
        message: format!("unsupported legacy GFDR property encoding: {error}"),
    })
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
    /// Stable node surrogate identities loaded from canonical Parquet.
    pub node_ids: BTreeMap<String, u64>,
    /// Canonical node creation/update timestamps in UTC microseconds.
    pub node_timestamps: BTreeMap<String, (i64, i64)>,
    /// Surviving edges: uuid -> (src, dst, rel_type).
    pub edges: BTreeMap<String, (String, String, String)>,
    /// Stable edge and endpoint surrogate identities loaded from canonical Parquet.
    pub edge_ids: BTreeMap<String, (u64, u64, u64)>,
    /// Canonical edge creation timestamps in UTC microseconds.
    pub edge_created_at: BTreeMap<String, i64>,
    /// Node properties: (node_uuid, key) -> value.
    pub node_properties: BTreeMap<(String, String), String>,
    /// Canonical property-file stem for each node property.
    pub node_property_stems: BTreeMap<(String, String), String>,
    /// Edge properties: (edge_uuid, key) -> value.
    pub edge_properties: BTreeMap<(String, String), String>,
    /// Canonical property-file stem for each edge property.
    pub edge_property_stems: BTreeMap<(String, String), String>,
    /// Operation UUIDs already applied (idempotency).
    pub applied_operations: BTreeMap<String, GraphDeltaPayload>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplayNodeRow {
    pub(crate) node_uuid: String,
    pub(crate) node_id: u64,
    pub(crate) type_ids: Vec<u32>,
    pub(crate) created_at_micros: i64,
    pub(crate) updated_at_micros: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplayEdgeRow {
    pub(crate) edge_uuid: String,
    pub(crate) src_uuid: String,
    pub(crate) dst_uuid: String,
    pub(crate) rel_type: String,
    pub(crate) edge_id: u64,
    pub(crate) src_id: u64,
    pub(crate) dst_id: u64,
    pub(crate) created_at_micros: i64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReplayOverlay {
    pub(crate) nodes: BTreeMap<String, Option<ReplayNodeRow>>,
    pub(crate) edges: BTreeMap<String, Option<ReplayEdgeRow>>,
    pub(crate) node_properties: BTreeMap<(String, String, String), Option<IrLiteral>>,
    pub(crate) edge_properties: BTreeMap<(String, String, String), Option<IrLiteral>>,
}

impl ReplayOverlay {
    fn estimated_memory(&self) -> usize {
        self.nodes
            .len()
            .saturating_mul(192)
            .saturating_add(self.edges.len().saturating_mul(256))
            .saturating_add(self.node_properties.len().saturating_mul(192))
            .saturating_add(self.edge_properties.len().saturating_mul(192))
    }
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
            if let Some(node_id) = self.node_ids.get(uuid) {
                hasher.update(node_id.to_le_bytes());
            }
            if let Some((created_at, updated_at)) = self.node_timestamps.get(uuid) {
                hasher.update(created_at.to_le_bytes());
                hasher.update(updated_at.to_le_bytes());
            }
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
            if let Some((edge_id, src_id, dst_id)) = self.edge_ids.get(uuid) {
                hasher.update(edge_id.to_le_bytes());
                hasher.update(src_id.to_le_bytes());
                hasher.update(dst_id.to_le_bytes());
            }
            if let Some(created_at) = self.edge_created_at.get(uuid) {
                hasher.update(created_at.to_le_bytes());
            }
            hasher.update(b"\n");
        }
        hasher.update(b"--node-props--\n");
        for ((uuid, key), value) in &self.node_properties {
            hasher.update(uuid.as_bytes());
            hasher.update(b"|");
            hasher.update(key.as_bytes());
            hasher.update(b"|");
            if let Some(stem) = self.node_property_stems.get(&(uuid.clone(), key.clone())) {
                hasher.update(stem.as_bytes());
            }
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
            if let Some(stem) = self.edge_property_stems.get(&(uuid.clone(), key.clone())) {
                hasher.update(stem.as_bytes());
            }
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
        let topology_metadata = self
            .node_ids
            .len()
            .saturating_mul(32)
            .saturating_add(self.node_timestamps.len().saturating_mul(40))
            .saturating_add(self.edge_ids.len().saturating_mul(48))
            .saturating_add(self.edge_created_at.len().saturating_mul(32));
        let nprops = self.node_properties.len().saturating_mul(96);
        let eprops = self.edge_properties.len().saturating_mul(96);
        let routing = self
            .node_property_stems
            .len()
            .saturating_add(self.edge_property_stems.len())
            .saturating_mul(64);
        let ops = self.applied_operations.len().saturating_mul(128);
        nodes
            .saturating_add(edges)
            .saturating_add(nprops)
            .saturating_add(eprops)
            .saturating_add(routing)
            .saturating_add(ops)
            .saturating_add(topology_metadata)
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
    /// Hard Arrow row bound used by every streaming materialization reader.
    pub materialization_batch_row_bound: u64,
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

#[allow(clippy::too_many_lines)] // Exhaustive payload handling is one fail-closed state machine.
fn build_replay_overlay(
    runs: &[GraphDeltaRun],
    limits: GraphDeltaJournalLimits,
) -> Result<(ReplayOverlay, GraphDeltaReplayEvidence), GfError> {
    let mut overlay = ReplayOverlay::default();
    let mut evidence = GraphDeltaReplayEvidence::default();
    let mut operations = BTreeMap::<Uuid, GraphDeltaPayload>::new();
    for run in runs {
        evidence.runs_replayed = evidence.runs_replayed.saturating_add(1);
        evidence.run_bytes_validated = evidence
            .run_bytes_validated
            .saturating_add(run.bytes.len() as u64);
        for record in &run.records {
            evidence.records_seen = evidence.records_seen.saturating_add(1);
            if let Some(prior) = operations.get(&record.operation_uuid) {
                if prior != &record.payload {
                    return Err(idempotency_conflict(
                        "graph delta operation_uuid reused with different payload",
                    ));
                }
                continue;
            }
            operations.insert(record.operation_uuid, record.payload.clone());
            match &record.payload {
                GraphDeltaPayload::UpsertNodeV2 {
                    node_uuid,
                    node_id,
                    type_ids,
                    created_at_micros,
                    updated_at_micros,
                } => {
                    overlay.nodes.insert(
                        node_uuid.clone(),
                        Some(ReplayNodeRow {
                            node_uuid: node_uuid.clone(),
                            node_id: *node_id,
                            type_ids: type_ids.clone(),
                            created_at_micros: *created_at_micros,
                            updated_at_micros: *updated_at_micros,
                        }),
                    );
                }
                GraphDeltaPayload::DeleteNode { node_uuid } => {
                    overlay.nodes.insert(node_uuid.clone(), None);
                }
                GraphDeltaPayload::UpsertEdgeV2 {
                    edge_uuid,
                    src_uuid,
                    dst_uuid,
                    rel_type,
                    edge_id,
                    src_id,
                    dst_id,
                    created_at_micros,
                } => {
                    overlay.edges.insert(
                        edge_uuid.clone(),
                        Some(ReplayEdgeRow {
                            edge_uuid: edge_uuid.clone(),
                            src_uuid: src_uuid.clone(),
                            dst_uuid: dst_uuid.clone(),
                            rel_type: rel_type.clone(),
                            edge_id: *edge_id,
                            src_id: *src_id,
                            dst_id: *dst_id,
                            created_at_micros: *created_at_micros,
                        }),
                    );
                }
                GraphDeltaPayload::DeleteEdge { edge_uuid } => {
                    overlay.edges.insert(edge_uuid.clone(), None);
                }
                GraphDeltaPayload::SetNodeProperty {
                    node_uuid,
                    property_stem,
                    key,
                    value,
                } => {
                    for ((uuid, stem, existing_key), existing_value) in &mut overlay.node_properties
                    {
                        if uuid == node_uuid && existing_key == key && stem != property_stem {
                            *existing_value = None;
                        }
                    }
                    overlay.node_properties.insert(
                        (node_uuid.clone(), property_stem.clone(), key.clone()),
                        Some(decode_graph_delta_value(value)?),
                    );
                }
                GraphDeltaPayload::RemoveNodeProperty {
                    node_uuid,
                    property_stem,
                    key,
                } => {
                    overlay.node_properties.insert(
                        (node_uuid.clone(), property_stem.clone(), key.clone()),
                        None,
                    );
                }
                GraphDeltaPayload::SetEdgeProperty {
                    edge_uuid,
                    property_stem,
                    key,
                    value,
                } => {
                    for ((uuid, stem, existing_key), existing_value) in &mut overlay.edge_properties
                    {
                        if uuid == edge_uuid && existing_key == key && stem != property_stem {
                            *existing_value = None;
                        }
                    }
                    overlay.edge_properties.insert(
                        (edge_uuid.clone(), property_stem.clone(), key.clone()),
                        Some(decode_graph_delta_value(value)?),
                    );
                }
                GraphDeltaPayload::RemoveEdgeProperty {
                    edge_uuid,
                    property_stem,
                    key,
                } => {
                    overlay.edge_properties.insert(
                        (edge_uuid.clone(), property_stem.clone(), key.clone()),
                        None,
                    );
                }
                GraphDeltaPayload::UpsertNode { .. } | GraphDeltaPayload::UpsertEdge { .. } => {
                    return Err(GfError::Project {
                        code: ProjectErrorCode::UnsupportedProjectFormat,
                        message: "unsupported lossless-metadata-free GFDR topology payload".into(),
                    });
                }
            }
            let memory = overlay.estimated_memory();
            if memory > limits.max_replay_memory_bytes {
                return Err(resource_limit("graph delta replay overlay memory"));
            }
            evidence.estimated_replay_memory_bytes = evidence
                .estimated_replay_memory_bytes
                .max(u64::try_from(memory).unwrap_or(u64::MAX));
        }
    }
    Ok((overlay, evidence))
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
    preflight_canonical_parquet(graph_root, inventory, limits)?;
    let runs = load_verified_delta_runs(graph_root, inventory, limits)?;
    let mut state = load_base_state(graph_root)?;
    let base_memory = state.estimated_memory();
    if base_memory > limits.max_replay_memory_bytes {
        return Err(resource_limit("graph delta replay memory"));
    }
    let mut evidence = apply_delta_runs(&mut state, &runs, limits)?;
    evidence.estimated_replay_memory_bytes = evidence
        .estimated_replay_memory_bytes
        .max(base_memory as u64);
    evidence.state_fingerprint = state.fingerprint();
    Ok((state, evidence))
}

/// Validate Parquet footer bounds and declared row work before Arrow/Parquet
/// allocates decoded arrays. Only inventory-owned canonical files are opened.
fn preflight_canonical_parquet(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    limits: GraphDeltaJournalLimits,
) -> Result<(), GfError> {
    // Authenticate the inventory authority before opening attacker-controlled
    // bytes with the Parquet decoder.
    verify_graph_tree(graph_root, inventory)?;
    if limits.max_batch_rows == 0 {
        return Err(resource_limit("graph delta replay batch rows"));
    }
    let mut work_rows = 0_u64;
    for entry in inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.ends_with(".parquet"))
    {
        let path = graph_root.join(&entry.relative_path);
        let mut file =
            File::open(&path).map_err(|error| storage("open Parquet preflight", &path, error))?;
        let file_len = file
            .metadata()
            .map_err(|error| storage("stat Parquet preflight", &path, error))?
            .len();
        if file_len < 12 {
            return Err(corrupt("canonical Parquet file is too small"));
        }
        file.seek(SeekFrom::End(-8))
            .map_err(|error| storage("seek Parquet footer", &path, error))?;
        let mut footer = [0_u8; 8];
        file.read_exact(&mut footer)
            .map_err(|error| storage("read Parquet footer", &path, error))?;
        if &footer[4..] != b"PAR1" {
            return Err(corrupt("canonical Parquet footer magic mismatch"));
        }
        let metadata_bytes = usize::try_from(u32::from_le_bytes(
            footer[..4]
                .try_into()
                .expect("four-byte Parquet footer length"),
        ))
        .map_err(|_| resource_limit("Parquet footer metadata bytes"))?;
        if metadata_bytes > limits.max_parquet_metadata_bytes {
            return Err(resource_limit("Parquet footer metadata bytes"));
        }
        if u64::try_from(metadata_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(8)
            > file_len
        {
            return Err(corrupt("canonical Parquet footer length exceeds file"));
        }
        file.rewind()
            .map_err(|error| storage("rewind Parquet preflight", &path, error))?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| {
                corrupt(format!("canonical Parquet metadata decode failed: {error}"))
            })?;
        let rows = u64::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| resource_limit("graph delta replay work rows"))?;
        work_rows = work_rows
            .checked_add(rows)
            .ok_or_else(|| resource_limit("graph delta replay work rows"))?;
        if work_rows > limits.max_work_rows {
            return Err(resource_limit("graph delta replay work rows"));
        }
    }
    Ok(())
}

/// Materialize a verified delta-bearing generation into a private canonical
/// Parquet workspace used by ordinary readers. The source remains immutable.
///
/// # Errors
/// Fails closed before returning the workspace when inventory, Parquet, GFDR,
/// typed-value, or replay-budget validation fails.
pub fn materialize_replayed_graph_tree(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    target: &Path,
    limits: GraphDeltaJournalLimits,
) -> Result<(crate::GraphFilesOpenEvidence, GraphDeltaReplayEvidence), GfError> {
    preflight_canonical_parquet(graph_root, inventory, limits)?;
    let runs = load_verified_delta_runs(graph_root, inventory, limits)?;
    let (overlay, mut evidence) = build_replay_overlay(&runs, limits)?;
    evidence.materialization_batch_row_bound = limits.max_batch_rows as u64;
    let open_evidence = crate::graph_files::materialize_graph_tree(graph_root, inventory, target)?;
    if evidence.runs_replayed == 0 {
        return Ok((open_evidence, evidence));
    }
    crate::writer::write_replay_overlay_streaming(graph_root, target, &overlay, limits)?;
    let deltas = target.join(GRAPH_DELTA_DIR);
    if deltas.exists() {
        fs::remove_dir_all(&deltas)
            .map_err(|error| storage("remove replayed delta view", &deltas, error))?;
    }
    Ok((open_evidence, evidence))
}

fn bounded_materialized_fingerprint(
    graph_root: &Path,
    inventory: &GraphFilesInventory,
    limits: GraphDeltaJournalLimits,
) -> Result<[u8; 32], GfError> {
    let target = tempfile::tempdir()
        .map_err(|error| GfError::Storage(format!("create replay fingerprint view: {error}")))?;
    materialize_replayed_graph_tree(graph_root, inventory, target.path(), limits)?;
    let (materialized, _) = capture_graph_files(target.path())?;
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-materialized-graph-tree/1\n");
    for entry in materialized.files {
        if entry.relative_path.starts_with("deltas/") {
            continue;
        }
        hasher.update(entry.relative_path.as_bytes());
        hasher.update(b"|");
        hasher.update(entry.content_sha256.as_bytes());
        hasher.update(b"\n");
    }
    Ok(hasher.finalize().into())
}

/// Publish one small-write generation that preserves unchanged base Parquet.
///
/// # Errors
/// Unsupported kinds are rejected before staging. Publication and idempotency
/// errors follow the project generation protocol.
pub fn publish_graph_delta(
    container_root: &Path,
    request: &GraphDeltaPublishRequest,
) -> Result<GraphDeltaPublicationReceipt, GfError> {
    publish_graph_delta_with_mode(
        container_root,
        request,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Publish a graph delta using the lifecycle mode established by the owning
/// facade.
///
/// # Errors
/// Returns the same errors as [`publish_graph_delta`].
pub fn publish_graph_delta_with_mode(
    container_root: &Path,
    request: &GraphDeltaPublishRequest,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<GraphDeltaPublicationReceipt, GfError> {
    publish_graph_delta_after_prepare(container_root, request, mode, |_| Ok(()))
}

#[allow(clippy::too_many_lines)] // Publication stages copy, encode, and CURRENT commit together.
fn publish_graph_delta_after_prepare(
    container_root: &Path,
    request: &GraphDeltaPublishRequest,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
    before_stage: impl FnOnce(&Path) -> Result<(), GfError>,
) -> Result<GraphDeltaPublicationReceipt, GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
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
        let state_fingerprint = bounded_materialized_fingerprint(
            &resolved.graph_tree_root(),
            &inventory,
            request.limits,
        )?;
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
            state_fingerprint,
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
        .filter(|entry| !entry.relative_path.starts_with("deltas/"))
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
    let state_fingerprint =
        bounded_materialized_fingerprint(staging.path(), &inventory, request.limits)?;

    before_stage(container_root)?;
    let publication = match stage_project_generation_from_admitted_parent(
        admission,
        parent,
        &generation_request,
        Some(staging.path()),
    )? {
        ProjectStageOutcome::Staged(staged) => {
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?
        }
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
    };

    Ok(GraphDeltaPublicationReceipt {
        publication,
        run_sequence: next_sequence,
        preserved_base_parquet_digests,
        unchanged_base_files,
        state_fingerprint,
    })
}

/// Seed a workspace with caller-provided canonical base files.
///
/// # Errors
/// Returns storage errors when directories or files cannot be written.
pub fn stage_base_graph_workspace(
    workspace: &Path,
    files: &[(&str, &[u8])],
    _base_state: Option<&ReconstructedGraphState>,
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
    Ok(())
}

pub(crate) fn load_base_state(graph_root: &Path) -> Result<ReconstructedGraphState, GfError> {
    let mut state = ReconstructedGraphState::default();
    let node_batches = crate::catalog::read_nodes(graph_root)
        .map_err(|error| corrupt(format!("canonical node Parquet decode failed: {error}")))?;
    for batch in node_batches {
        let uuids = required_array::<FixedSizeBinaryArray>(&batch, "node_uuid")?;
        let ids = required_array::<UInt64Array>(&batch, "node_id")?;
        let type_ids = required_array::<ListArray>(&batch, "type_ids")?;
        let created = required_array::<TimestampMicrosecondArray>(&batch, "created_at")?;
        let updated = required_array::<TimestampMicrosecondArray>(&batch, "updated_at")?;
        for row in 0..batch.num_rows() {
            let uuid = canonical_uuid(uuids.value(row))?;
            let labels = type_ids.value(row);
            let labels = labels
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| corrupt("canonical node type_ids item type mismatch"))?;
            let labels = (0..labels.len()).map(|index| labels.value(index)).collect();
            state.nodes.insert(uuid.clone(), labels);
            state.node_ids.insert(uuid.clone(), ids.value(row));
            state
                .node_timestamps
                .insert(uuid, (created.value(row), updated.value(row)));
        }
    }

    let edge_batches =
        crate::catalog::read_edges(graph_root, "*", graphforge_core::OntologyMode::Strict)
            .map_err(|error| corrupt(format!("canonical edge Parquet decode failed: {error}")))?;
    for batch in edge_batches {
        let edge_uuids = required_array::<FixedSizeBinaryArray>(&batch, "edge_uuid")?;
        let src_uuids = required_array::<FixedSizeBinaryArray>(&batch, "src_uuid")?;
        let dst_uuids = required_array::<FixedSizeBinaryArray>(&batch, "dst_uuid")?;
        let edge_ids = required_array::<UInt64Array>(&batch, "edge_id")?;
        let src_ids = required_array::<UInt64Array>(&batch, "src_id")?;
        let dst_ids = required_array::<UInt64Array>(&batch, "dst_id")?;
        let created = required_array::<TimestampMicrosecondArray>(&batch, "created_at")?;
        let relations = required_array::<StringArray>(&batch, "rel_type_name")?;
        for row in 0..batch.num_rows() {
            let edge_uuid = canonical_uuid(edge_uuids.value(row))?;
            let src_uuid = canonical_uuid(src_uuids.value(row))?;
            let dst_uuid = canonical_uuid(dst_uuids.value(row))?;
            state.edges.insert(
                edge_uuid.clone(),
                (src_uuid, dst_uuid, relations.value(row).to_owned()),
            );
            state.edge_ids.insert(
                edge_uuid.clone(),
                (edge_ids.value(row), src_ids.value(row), dst_ids.value(row)),
            );
            state.edge_created_at.insert(edge_uuid, created.value(row));
        }
    }

    for (stem, uuid, properties) in crate::writer::read_all_node_properties(graph_root)? {
        let uuid = Uuid::from_bytes(uuid).hyphenated().to_string();
        for (key, value) in properties {
            state
                .node_property_stems
                .insert((uuid.clone(), key.clone()), stem.clone());
            state
                .node_properties
                .insert((uuid.clone(), key), encode_typed_value(&value)?);
        }
    }
    for (stem, uuid, properties) in crate::writer::read_all_edge_properties(graph_root)? {
        let uuid = Uuid::from_bytes(uuid).hyphenated().to_string();
        for (key, value) in properties {
            state
                .edge_property_stems
                .insert((uuid.clone(), key.clone()), stem.clone());
            state
                .edge_properties
                .insert((uuid.clone(), key), encode_typed_value(&value)?);
        }
    }
    Ok(state)
}

fn required_array<'a, T: 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a T, GfError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            corrupt(format!(
                "canonical Parquet column {name} has unexpected type"
            ))
        })
}

fn canonical_uuid(bytes: &[u8]) -> Result<String, GfError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| corrupt("canonical UUID column is not 16 bytes"))?;
    Ok(Uuid::from_bytes(bytes).hyphenated().to_string())
}

fn encode_typed_value(value: &IrLiteral) -> Result<String, GfError> {
    encode_graph_delta_value(value)
        .map_err(|error| corrupt(format!("canonical property value encode failed: {error}")))
}

#[allow(clippy::too_many_lines)] // Exhaustive operation handling keeps replay ordering explicit.
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
        GraphDeltaPayload::UpsertNodeV2 {
            node_uuid,
            node_id,
            type_ids,
            created_at_micros,
            updated_at_micros,
        } => {
            let mut sorted = type_ids.clone();
            sorted.sort_unstable();
            state.nodes.insert(node_uuid.clone(), sorted);
            state.node_ids.insert(node_uuid.clone(), *node_id);
            state
                .node_timestamps
                .insert(node_uuid.clone(), (*created_at_micros, *updated_at_micros));
        }
        GraphDeltaPayload::DeleteNode { node_uuid } => {
            state.nodes.remove(node_uuid);
            state.node_ids.remove(node_uuid);
            state.node_timestamps.remove(node_uuid);
            state
                .node_properties
                .retain(|(uuid, _), _| uuid != node_uuid);
            state
                .node_property_stems
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
        GraphDeltaPayload::UpsertEdgeV2 {
            edge_uuid,
            src_uuid,
            dst_uuid,
            rel_type,
            edge_id,
            src_id,
            dst_id,
            created_at_micros,
        } => {
            state.edges.insert(
                edge_uuid.clone(),
                (src_uuid.clone(), dst_uuid.clone(), rel_type.clone()),
            );
            state
                .edge_ids
                .insert(edge_uuid.clone(), (*edge_id, *src_id, *dst_id));
            state
                .edge_created_at
                .insert(edge_uuid.clone(), *created_at_micros);
        }
        GraphDeltaPayload::DeleteEdge { edge_uuid } => {
            state.edges.remove(edge_uuid);
            state.edge_ids.remove(edge_uuid);
            state.edge_created_at.remove(edge_uuid);
            state
                .edge_properties
                .retain(|(uuid, _), _| uuid != edge_uuid);
            state
                .edge_property_stems
                .retain(|(uuid, _), _| uuid != edge_uuid);
        }
        GraphDeltaPayload::SetNodeProperty {
            node_uuid,
            property_stem,
            key,
            value,
        } => {
            state
                .node_property_stems
                .insert((node_uuid.clone(), key.clone()), property_stem.clone());
            state
                .node_properties
                .insert((node_uuid.clone(), key.clone()), value.clone());
        }
        GraphDeltaPayload::RemoveNodeProperty { node_uuid, key, .. } => {
            state
                .node_properties
                .remove(&(node_uuid.clone(), key.clone()));
            state
                .node_property_stems
                .remove(&(node_uuid.clone(), key.clone()));
        }
        GraphDeltaPayload::SetEdgeProperty {
            edge_uuid,
            property_stem,
            key,
            value,
        } => {
            state
                .edge_property_stems
                .insert((edge_uuid.clone(), key.clone()), property_stem.clone());
            state
                .edge_properties
                .insert((edge_uuid.clone(), key.clone()), value.clone());
        }
        GraphDeltaPayload::RemoveEdgeProperty { edge_uuid, key, .. } => {
            state
                .edge_properties
                .remove(&(edge_uuid.clone(), key.clone()));
            state
                .edge_property_stems
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
    use super::*;
    use crate::project_fault_oracle::{
        AuthorityClass, PublicationIds, PublicationPhase, default_durable_ids, expected_authority,
        publication_ops, simulate_crash,
    };

    #[test]
    fn parquet_footer_limit_fails_before_decoder_allocation() {
        let root = tempfile::tempdir().unwrap();
        let relative_path = "topology/nodes.parquet";
        let path = root.path().join(relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut bytes = vec![0_u8; 12];
        bytes[..4].copy_from_slice(b"PAR1");
        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        bytes[8..].copy_from_slice(b"PAR1");
        fs::write(&path, &bytes).unwrap();
        let (inventory, _) = capture_graph_files(root.path()).unwrap();
        let limits = GraphDeltaJournalLimits {
            max_parquet_metadata_bytes: 1024,
            ..GraphDeltaJournalLimits::default()
        };
        let error = preflight_canonical_parquet(root.path(), &inventory, limits).unwrap_err();
        assert!(error.to_string().contains("GF_RESOURCE_LIMIT"));
        assert!(error.to_string().contains("footer metadata bytes"));
    }

    fn publish_graph_base(root: &Path) {
        crate::open_or_initialize_project(root).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        stage_base_graph_workspace(
            workspace.path(),
            &[
                ("topology/nodes.parquet", b"nodes"),
                ("topology/edges.parquet", b"edges"),
            ],
            Some(&ReconstructedGraphState::default()),
        )
        .unwrap();
        let (_, files) = capture_graph_files(workspace.path()).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
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
            crate::stage_project_generation_with_graph_tree(root, &request, Some(workspace.path()))
                .unwrap()
        else {
            panic!("base publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
    }

    fn one_node_request() -> GraphDeltaPublishRequest {
        GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: vec![GraphDeltaOp {
                operation_uuid: Uuid::now_v7(),
                kind: GraphDeltaOpKind::UpsertNode,
                payload: GraphDeltaPayload::UpsertNodeV2 {
                    node_uuid: Uuid::now_v7().hyphenated().to_string(),
                    node_id: 1,
                    type_ids: vec![1],
                    created_at_micros: 1,
                    updated_at_micros: 1,
                },
            }],
            limits: GraphDeltaJournalLimits::default(),
        }
    }

    fn stage_graph_clone(root: &Path) -> (Uuid, Box<crate::StagedProjectGeneration>) {
        let current = resolve_project_generation(root).unwrap();
        let graph_tree = current.graph_tree_root();
        let (_, files) = capture_graph_files(&graph_tree).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let generation_uuid = Uuid::now_v7();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid,
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
            crate::stage_project_generation_with_graph_tree(root, &request, Some(&graph_tree))
                .unwrap()
        else {
            panic!("clone publication unexpectedly replayed");
        };
        (generation_uuid, staged)
    }

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

    #[test]
    fn prepared_delta_fails_busy_behind_a_live_current_writer() {
        let root = tempfile::tempdir().unwrap();
        publish_graph_base(root.path());
        let prepared = one_node_request();
        let (concurrent_generation, concurrent) = stage_graph_clone(root.path());

        let error = publish_graph_delta_after_prepare(
            root.path(),
            &prepared,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            |_| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.code(), "GF_WRITER_BUSY");
        concurrent
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            concurrent_generation
        );
    }
}
