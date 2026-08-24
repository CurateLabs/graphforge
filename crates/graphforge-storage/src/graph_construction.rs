//! Private, crash-recoverable staging for one-generation graph construction.
//!
//! Construction accepts bounded canonical Arrow windows and writes immutable
//! Parquet shards plus block-encoded sorted identity/endpoint runs. Each window
//! is acknowledged by one immutable receipt; a constant-size checkpoint names
//! the next sequence. `CURRENT` is never touched by this module's staging or
//! sealing path. A generation-last publisher consumes the sealed inventory.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, StableDirectory, file_identity, file_link_count};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::uuid_membership::{
    ConstructionUuidIdentity, UuidConstructionSnapshot, UuidConstructionSnapshotWork,
    open_uuid_construction_snapshot,
};

const FORMAT_VERSION: u32 = 2;
const PRIVATE_ROOT: &str = ".graphforge-construction";
const CHECKPOINT: &str = "checkpoint.json";
const INTENT: &str = "intent.json";
const SHAPE_INTENT: &str = "shape-intent.json";
const BLOCK_BYTES: usize = 1 << 20;
const MAX_CONTROL_BYTES: u64 = 64 << 10;
const IDENTITY_WIDTH: usize = 16;
const ENDPOINT_WIDTH: usize = 48;
const NODE_DETAIL_WIDTH: usize = 20;
const EDGE_DETAIL_WIDTH: usize = 304;
const BASE_IDENTITY_WIDTH: usize = 32;
const BASE_IDENTITIES: &str = "base-identities.run";

/// Canonical node construction input: UUID and primary type id.
pub static CONSTRUCTION_NODE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("type_id", DataType::UInt32, false),
    ]))
});

/// Canonical edge construction input: UUID endpoints and relation route.
pub static CONSTRUCTION_EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("rel_type", DataType::Utf8, false),
    ]))
});

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction session: {error}"))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Ordered construction phase.
#[serde(rename_all = "snake_case")]
pub enum ConstructionChunkKind {
    /// Node identities and primary types.
    Node,
    /// Edge identities, endpoints, and relation routes.
    Edge,
}

impl ConstructionChunkKind {
    const fn tag(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Fixed resource windows for staging and the later external merge.
pub struct GraphConstructionBudgets {
    /// Maximum rows in one Arrow window.
    pub max_batch_rows: usize,
    /// Maximum Arrow-owned bytes in one window.
    pub max_batch_bytes: usize,
    /// Maximum accepted chunks.
    pub max_chunks: u64,
    /// Maximum fixed-width records sorted for one chunk.
    pub max_run_records: usize,
    /// Maximum inputs opened by one external merge group.
    pub merge_fan_in: usize,
}

impl Default for GraphConstructionBudgets {
    fn default() -> Self {
        Self {
            max_batch_rows: 65_536,
            max_batch_bytes: 64 << 20,
            max_chunks: 1_000_000,
            max_run_records: 4 * 65_536,
            merge_fan_in: 32,
        }
    }
}

impl GraphConstructionBudgets {
    fn validate(self) -> Result<Self, GfError> {
        if self.max_batch_rows == 0
            || self.max_batch_bytes == 0
            || self.max_chunks == 0
            || self.max_run_records < 4 * self.max_batch_rows
            || self.merge_fan_in < 2
        {
            return Err(storage("invalid construction budgets"));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Private session lifecycle. Sealed is not publicly committed.
#[serde(rename_all = "snake_case")]
pub enum GraphConstructionState {
    /// Accepting chunks.
    Staging,
    /// Immutable inventory ready for the generation-last publisher.
    Sealed,
    /// Explicitly abandoned.
    Aborted,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Measured application I/O and bounded retained-window evidence.
pub struct GraphConstructionEvidence {
    /// Rows accepted.
    pub input_rows: u64,
    /// Non-replay chunks accepted.
    pub input_batches: u64,
    /// Immutable Parquet shards.
    pub parquet_shards: u64,
    /// Bytes submitted through measured immutable-artifact writers.
    /// Control-record traffic is deliberately reported by the outer I/O probe.
    pub write_bytes: u64,
    /// Actual submissions by measured immutable-artifact writers.
    pub write_operations: u64,
    /// Bytes read for independent authentication.
    pub authentication_read_bytes: u64,
    /// Actual bounded authentication reads.
    pub authentication_read_operations: u64,
    /// Fixed-width run records.
    pub run_records: u64,
    /// Largest retained Arrow row window.
    pub peak_batch_rows: u64,
    /// Largest retained Arrow byte window.
    pub peak_batch_bytes: u64,
    /// Largest fixed-width sort window.
    pub peak_run_records: u64,
    /// Prior topology rows decoded during staging. Invariant: zero.
    pub prior_topology_rows_decoded: u64,
    /// CURRENT transitions during staging. Invariant: zero.
    pub current_transitions: u64,
    /// Exact idempotent replays.
    pub replayed_chunks: u64,
    /// Temporary and final fixed-width records read by canonical shaping.
    pub merge_read_records: u64,
    /// Temporary and final fixed-width records written by canonical shaping.
    pub merge_written_records: u64,
    /// External merge groups completed (including intermediate levels).
    pub merge_groups: u64,
    /// Highest number of simultaneously open merge inputs.
    pub peak_merge_inputs: u64,
    /// Exact fixed-width payload bytes read during shaping.
    pub merge_read_bytes: u64,
    /// Exact fixed-width payload bytes written during shaping.
    pub merge_written_bytes: u64,
    /// Bounded reader refills implied by the fixed one-MiB I/O window.
    pub merge_read_blocks: u64,
    /// Bounded writer flushes implied by the fixed one-MiB I/O window.
    pub merge_write_blocks: u64,
    /// Number of completed merge levels across shaped domains.
    pub merge_passes: u64,
    /// Largest measured temporary merge footprint.
    pub peak_merge_temporary_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Publisher input produced from a sealed construction session.
pub struct ConstructionShape {
    /// UUID-sorted node/edge identity records with assigned surrogates.
    pub identities: String,
    /// UUID-sorted node type records, when nodes were staged.
    pub node_details: Option<String>,
    /// UUID-sorted edge endpoint and relation records, when edges were staged.
    pub edge_details: Option<String>,
    /// Live retained plus staged nodes.
    pub node_count: u64,
    /// Live retained plus staged edges.
    pub edge_count: u64,
    /// Assigned node surrogate tail.
    pub max_node_surrogate: u64,
    /// Assigned edge surrogate tail.
    pub max_edge_surrogate: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct IdentityRecord {
    volume_serial: u64,
    file_id: String,
}

impl From<FileIdentity> for IdentityRecord {
    fn from(value: FileIdentity) -> Self {
        Self {
            volume_serial: value.volume_serial,
            file_id: hex(&value.file_id),
        }
    }
}

impl IdentityRecord {
    fn matches(&self, value: FileIdentity) -> bool {
        self.volume_serial == value.volume_serial && self.file_id == hex(&value.file_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ArtifactReceipt {
    name: String,
    bytes: u64,
    sha256: String,
    identity: IdentityRecord,
    write_operations: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Immutable acknowledgement of one canonical Arrow chunk.
pub struct ConstructionChunkReceipt {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    prior_receipt_sha256: Option<String>,
    /// Caller-stable idempotency key.
    pub chunk_id: String,
    /// Monotonic accepted sequence.
    pub sequence: u64,
    /// Node or edge phase.
    pub kind: ConstructionChunkKind,
    /// Logical rows.
    pub rows: u64,
    /// Logical Arrow bytes charged to the window.
    pub input_bytes: u64,
    /// Canonical logical digest, independent of Arrow buffer layout.
    pub input_sha256: String,
    /// Fixed-width run records.
    pub run_records: u64,
    parquet: ArtifactReceipt,
    identities: ArtifactReceipt,
    endpoints: Option<ArtifactReceipt>,
    details: ArtifactReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Checkpoint {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    budgets: GraphConstructionBudgets,
    state: GraphConstructionState,
    next_sequence: u64,
    saw_edge: bool,
    last_receipt_sha256: Option<String>,
    base_identities: Option<ArtifactReceipt>,
    base_work: UuidConstructionSnapshotWork,
    evidence: GraphConstructionEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkIntent {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    sequence: u64,
    chunk_id: String,
    chunk_key: String,
    kind: ConstructionChunkKind,
    rows: u64,
    input_bytes: u64,
    input_sha256: String,
    parent_topology_generation: u64,
    prior_receipt_sha256: Option<String>,
    run_records: u64,
    parquet: Option<ArtifactReceipt>,
    identities: Option<ArtifactReceipt>,
    endpoints: Option<ArtifactReceipt>,
    details: Option<ArtifactReceipt>,
}

#[derive(Serialize, Deserialize)]
struct ShapeIntent {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    budgets: GraphConstructionBudgets,
    last_receipt_sha256: Option<String>,
    complete: bool,
    shape: Option<ConstructionShape>,
    outputs: Vec<ArtifactReceipt>,
}

static ACTIVE_OPERATIONS: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

struct ProcessReservation(String);

impl ProcessReservation {
    fn acquire(key: String) -> Result<Self, GfError> {
        let mut active = ACTIVE_OPERATIONS
            .lock()
            .map_err(|_| storage("process operation registry poisoned"))?;
        if !active.insert(key.clone()) {
            return Err(storage(
                "construction operation is already open in this process",
            ));
        }
        Ok(Self(key))
    }
}

impl Drop for ProcessReservation {
    fn drop(&mut self) {
        if let Ok(mut active) = ACTIVE_OPERATIONS.lock() {
            active.remove(&self.0);
        }
    }
}

/// Descriptor-relative, exclusively owned private construction session.
pub struct GraphConstructionSession {
    project_path: PathBuf,
    project: StableDirectory,
    root: StableDirectory,
    checkpoint: Checkpoint,
    base_snapshot: Option<UuidConstructionSnapshot>,
    _reservation: ProcessReservation,
}

impl GraphConstructionSession {
    /// Create or resume an operation pinned to one parent topology generation.
    pub fn open(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        let budgets = budgets.validate()?;
        let project = StableDirectory::open(project_dir).map_err(storage)?;
        if crate::read_topology_generation(project_dir)? != parent_topology_generation {
            return Err(storage(
                "requested parent generation is not current at session open",
            ));
        }
        let project_identity = project.identity();
        let key = format!(
            "{}:{}:{}",
            project_identity.volume_serial,
            hex(&project_identity.file_id),
            operation_uuid.simple()
        );
        let reservation = ProcessReservation::acquire(key)?;
        let private = project
            .create_child_directory(OsStr::new(PRIVATE_ROOT))
            .map_err(storage)?;
        let operation_name = operation_uuid.simple().to_string();
        let root = private
            .create_child_directory(OsStr::new(&operation_name))
            .map_err(storage)?;
        if !root.try_lock_exclusive().map_err(storage)? {
            return Err(storage(
                "construction operation is locked by another process",
            ));
        }
        let session_identity = root.identity();
        cleanup_authenticated_control_temps(
            &root,
            operation_uuid,
            project_identity,
            session_identity,
        )?;
        cleanup_owned_artifact_temps(&root)?;
        let checkpoint_exists = match root.open_child_file(OsStr::new(CHECKPOINT)) {
            Ok(file) => {
                drop(file);
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(storage(error)),
        };
        if !checkpoint_exists && parent_topology_generation != 0 {
            remove_unrecorded_base(&root)?;
        }
        let (base_snapshot, base_identities, base_work) = if parent_topology_generation == 0 {
            (None, None, UuidConstructionSnapshotWork::default())
        } else if checkpoint_exists {
            let mut checkpoint_file = root
                .open_child_file(OsStr::new(CHECKPOINT))
                .map_err(storage)?;
            let recorded: Checkpoint = decode_bounded(&mut checkpoint_file)?;
            let receipt = recorded
                .base_identities
                .as_ref()
                .ok_or_else(|| storage("nonempty parent lacks retained identity run"))?;
            let (snapshot, work) = authenticate_base_snapshot(
                project_dir,
                parent_topology_generation,
                &root,
                receipt,
            )?;
            (Some(snapshot), Some(receipt.clone()), work)
        } else {
            let (snapshot, receipt, work) =
                create_base_snapshot(project_dir, parent_topology_generation, &root)?;
            (Some(snapshot), Some(receipt), work)
        };
        let checkpoint = match root.open_child_file(OsStr::new(CHECKPOINT)) {
            Ok(mut file) => decode_bounded(&mut file)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut evidence = GraphConstructionEvidence::default();
                evidence.authentication_read_bytes = base_work.authentication_bytes;
                evidence.authentication_read_operations = base_work.authentication_blocks;
                if let Some(base) = &base_identities {
                    evidence.write_bytes = base.bytes;
                    evidence.write_operations = base.write_operations;
                }
                let initial = Checkpoint {
                    format_version: FORMAT_VERSION,
                    operation_uuid,
                    project_identity: project_identity.into(),
                    session_identity: session_identity.into(),
                    parent_topology_generation,
                    budgets,
                    state: GraphConstructionState::Staging,
                    next_sequence: 0,
                    saw_edge: false,
                    last_receipt_sha256: None,
                    base_identities,
                    base_work,
                    evidence,
                };
                install_control(&root, CHECKPOINT, &initial)?;
                initial
            }
            Err(error) => return Err(storage(error)),
        };
        validate_checkpoint(
            &checkpoint,
            operation_uuid,
            project_identity,
            session_identity,
            parent_topology_generation,
            budgets,
        )?;
        let mut session = Self {
            project_path: project_dir.to_path_buf(),
            project,
            root,
            checkpoint,
            base_snapshot,
            _reservation: reservation,
        };
        recover_shape_intent(&session.root, &session.checkpoint)?;
        session.recover_intent()?;
        session.revalidate_authority()?;
        Ok(session)
    }

    /// Current private state.
    #[must_use]
    pub const fn state(&self) -> GraphConstructionState {
        self.checkpoint.state
    }

    /// Pinned parent topology generation.
    #[must_use]
    pub const fn parent_topology_generation(&self) -> u64 {
        self.checkpoint.parent_topology_generation
    }

    /// Measured aggregate evidence.
    #[must_use]
    pub const fn evidence(&self) -> &GraphConstructionEvidence {
        &self.checkpoint.evidence
    }

    /// Number of durably accepted chunks.
    #[must_use]
    pub const fn accepted_chunks(&self) -> u64 {
        self.checkpoint.next_sequence
    }

    /// Append one canonical bounded Arrow chunk.
    pub fn append(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append_with_cancellation(kind, chunk_id, batch, || false)
    }

    /// Append while polling a caller-owned cancellation signal at durable
    /// artifact boundaries. Cancellation leaves an intent that the next open
    /// authenticates and rolls back without changing public authority.
    pub fn append_with_cancellation(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("session is not accepting chunks"));
        }
        validate_chunk_id(chunk_id)?;
        validate_schema(kind, batch)?;
        if batch.num_rows() == 0 {
            return Err(storage("empty construction chunk"));
        }
        let input_bytes = batch.get_array_memory_size();
        if batch.num_rows() > self.checkpoint.budgets.max_batch_rows
            || input_bytes > self.checkpoint.budgets.max_batch_bytes
            || self.checkpoint.next_sequence >= self.checkpoint.budgets.max_chunks
        {
            return Err(storage("construction resource window exhausted"));
        }
        if kind == ConstructionChunkKind::Node && self.checkpoint.saw_edge {
            return Err(storage("node chunk cannot follow edge staging"));
        }
        let input_sha256 = logical_batch_digest(kind, batch)?;
        let key_name = chunk_key_name(chunk_id);
        if let Ok(mut key_file) = self.root.open_child_file(OsStr::new(&key_name)) {
            let pointer: ReceiptPointer = decode_bounded(&mut key_file)?;
            let receipt = self.read_receipt(pointer.sequence)?;
            let receipt_body = serde_json::to_vec(&receipt).map_err(storage)?;
            if pointer.operation_uuid == self.checkpoint.operation_uuid
                && pointer.project_identity == self.checkpoint.project_identity
                && pointer.session_identity == self.checkpoint.session_identity
                && pointer.receipt_sha256 == sha256(&receipt_body)
                && receipt.chunk_id == chunk_id
                && receipt.kind == kind
                && receipt.rows == batch.num_rows() as u64
                && receipt.input_sha256 == input_sha256
            {
                validate_receipt_artifacts(&self.root, &receipt)?;
                self.checkpoint.evidence.replayed_chunks =
                    self.checkpoint.evidence.replayed_chunks.saturating_add(1);
                replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
                return Ok(receipt);
            }
            return Err(storage("conflicting construction chunk replay"));
        }
        let arrays = extract_runs(kind, batch)?;
        let run_records = arrays
            .identities
            .len()
            .saturating_add(arrays.endpoints.len())
            .saturating_add(batch.num_rows());
        if run_records > self.checkpoint.budgets.max_run_records {
            return Err(storage("construction run window exhausted"));
        }
        let sequence = self.checkpoint.next_sequence;
        let mut intent = ChunkIntent {
            format_version: FORMAT_VERSION,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            chunk_id: chunk_id.to_owned(),
            chunk_key: key_name,
            kind,
            rows: batch.num_rows() as u64,
            input_bytes: input_bytes as u64,
            input_sha256,
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            prior_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            run_records: run_records as u64,
            parquet: None,
            identities: None,
            endpoints: None,
            details: None,
        };
        install_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let stem = artifact_stem(sequence, kind);
        intent.parquet = Some(write_parquet(
            &self.root,
            &format!("{stem}.parquet"),
            batch,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        intent.identities = Some(write_fixed_run(
            &self.root,
            &format!("{stem}.identities.run"),
            &arrays.identities,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        if !arrays.endpoints.is_empty() {
            intent.endpoints = Some(write_fixed_run(
                &self.root,
                &format!("{stem}.endpoints.run"),
                &arrays.endpoints,
            )?);
            replace_control(&self.root, INTENT, &intent)?;
            reject_cancelled(&mut cancelled)?;
        }
        intent.details = Some(match &arrays.details {
            DetailRuns::Node(records) => {
                write_fixed_run(&self.root, &format!("{stem}.node-details.run"), records)?
            }
            DetailRuns::Edge(records) => {
                write_fixed_run(&self.root, &format!("{stem}.edge-details.run"), records)?
            }
        });
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let receipt = receipt_from_intent(&intent)?;
        let receipt_name = receipt_name(sequence);
        install_control(&self.root, &receipt_name, &receipt)?;
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage)?;
        let pointer = ReceiptPointer {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            receipt_sha256: sha256(&receipt_bytes),
        };
        install_control(&self.root, &intent.chunk_key, &pointer)?;
        self.advance_checkpoint(&receipt, &receipt_bytes)?;
        unlink_named(&self.root, INTENT)?;
        Ok(receipt)
    }

    /// Independently reopen and authenticate every sealed artifact once, then
    /// freeze the private inventory. No generation authority changes here.
    pub fn seal(&mut self) -> Result<(), GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("only a staging session can be sealed"));
        }
        let mut prior_digest = None;
        let mut saw_edge = false;
        let mut read_bytes = 0_u64;
        let mut read_operations = 0_u64;
        for sequence in 0..self.checkpoint.next_sequence {
            let receipt = self.read_receipt(sequence)?;
            validate_receipt_semantics(&receipt, sequence, self.checkpoint.budgets)?;
            if receipt.kind == ConstructionChunkKind::Node && saw_edge {
                return Err(storage("node receipt follows edge receipt"));
            }
            saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
            if receipt.prior_receipt_sha256 != prior_digest {
                return Err(storage("receipt journal chain is discontinuous"));
            }
            for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
                .into_iter()
                .chain(receipt.endpoints.iter())
            {
                let work = authenticate_artifact(&self.root, artifact)?;
                read_bytes = read_bytes.saturating_add(work.bytes);
                read_operations = read_operations.saturating_add(work.operations);
            }
            validate_parquet_shape(&self.root, &receipt)?;
            let body = serde_json::to_vec(&receipt).map_err(storage)?;
            prior_digest = Some(sha256(&body));
        }
        if prior_digest != self.checkpoint.last_receipt_sha256 {
            return Err(storage("receipt journal tail differs from checkpoint"));
        }
        if saw_edge != self.checkpoint.saw_edge {
            return Err(storage("checkpoint phase differs from receipt journal"));
        }
        self.checkpoint.evidence.authentication_read_bytes = self
            .checkpoint
            .evidence
            .authentication_read_bytes
            .saturating_add(read_bytes);
        self.checkpoint.evidence.authentication_read_operations = self
            .checkpoint
            .evidence
            .authentication_read_operations
            .saturating_add(read_operations);
        self.checkpoint.state = GraphConstructionState::Sealed;
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    /// Validate the sealed identity domains and produce deterministic,
    /// UUID-sorted canonical construction runs.  This is deliberately still
    /// private staging: the generation-last publisher owns Parquet and CURRENT.
    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub fn shape_canonical_with_cancellation(
        &mut self,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionShape, GfError> {
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed {
            return Err(storage("only a sealed session can be shaped"));
        }
        reject_cancelled(&mut cancelled)?;
        if let Some(shape) = read_completed_shape(&self.root, &self.checkpoint)? {
            return Ok(shape);
        }
        reject_existing_merge_artifacts(&self.root)?;

        let mut unified = Vec::new();
        if self.checkpoint.base_identities.is_some() {
            unified.push(BASE_IDENTITIES.to_owned());
        }
        let mut node_details = Vec::new();
        let mut edge_details = Vec::new();
        let mut endpoints = Vec::new();
        let mut receipts = Vec::new();
        for sequence in 0..self.checkpoint.next_sequence {
            reject_cancelled(&mut cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            validate_receipt_artifacts(&self.root, &receipt)?;
            match receipt.kind {
                ConstructionChunkKind::Node => node_details.push(String::new()),
                ConstructionChunkKind::Edge => {
                    edge_details.push(String::new());
                    endpoints.push(String::new());
                }
            }
            receipts.push(receipt);
        }
        let shape_intent = ShapeIntent {
            format_version: FORMAT_VERSION,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            budgets: self.checkpoint.budgets,
            last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            complete: false,
            shape: None,
            outputs: Vec::new(),
        };
        install_control(&self.root, SHAPE_INTENT, &shape_intent)?;
        node_details.clear();
        edge_details.clear();
        endpoints.clear();
        for (sequence, receipt) in receipts.iter().enumerate() {
            let name = format!("merge-unified-{sequence:020}.run");
            convert_identity_run(&self.root, receipt, &name, &mut self.checkpoint.evidence)?;
            unified.push(name);
            match receipt.kind {
                ConstructionChunkKind::Node => {
                    let name = format!("merge-node-source-{sequence:020}.run");
                    copy_authenticated_run::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &name,
                        &mut self.checkpoint.evidence,
                    )?;
                    node_details.push(name);
                }
                ConstructionChunkKind::Edge => {
                    let detail = format!("merge-edge-source-{sequence:020}.run");
                    copy_authenticated_run::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &detail,
                        &mut self.checkpoint.evidence,
                    )?;
                    edge_details.push(detail);
                    let endpoint = format!("merge-endpoint-source-{sequence:020}.run");
                    copy_authenticated_run::<ENDPOINT_WIDTH>(
                        &self.root,
                        receipt
                            .endpoints
                            .as_ref()
                            .ok_or_else(|| storage("edge receipt lacks endpoint run"))?,
                        &endpoint,
                        &mut self.checkpoint.evidence,
                    )?;
                    endpoints.push(endpoint);
                }
            }
        }
        let identities = merge_fixed_all::<BASE_IDENTITY_WIDTH>(
            &self.root,
            unified,
            "merge-identities",
            self.checkpoint.budgets.merge_fan_in,
            true,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let node_details = merge_optional::<NODE_DETAIL_WIDTH>(
            &self.root,
            node_details,
            "merge-node-details",
            self.checkpoint.budgets.merge_fan_in,
            true,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let edge_details = merge_optional::<EDGE_DETAIL_WIDTH>(
            &self.root,
            edge_details,
            "merge-edge-details",
            self.checkpoint.budgets.merge_fan_in,
            true,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let endpoints = merge_optional::<ENDPOINT_WIDTH>(
            &self.root,
            endpoints,
            "merge-endpoints",
            self.checkpoint.budgets.merge_fan_in,
            false,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let (base_max_node, base_max_edge) = match self.checkpoint.parent_topology_generation {
            0 => (0, 0),
            _ => crate::writer::read_surrogate_tails(
                // The retained project directory is revalidated immediately above.
                &self.project_path,
            )?
            .ok_or_else(|| storage("nonempty parent lacks surrogate tails"))?,
        };
        if base_max_node != self.checkpoint.base_work.max_node_surrogate {
            return Err(storage("UUID snapshot and surrogate tails disagree"));
        }
        let (node_count, edge_count, max_node_surrogate, max_edge_surrogate) =
            validate_unified_and_details(
                &self.root,
                &identities,
                node_details.as_deref(),
                edge_details.as_deref(),
                endpoints.as_deref(),
                base_max_node,
                base_max_edge,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
        let identities = assign_surrogates(
            &self.root,
            &identities,
            base_max_node,
            base_max_edge,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        self.checkpoint.evidence.merge_read_blocks = self
            .checkpoint
            .evidence
            .merge_read_bytes
            .div_ceil(BLOCK_BYTES as u64);
        self.checkpoint.evidence.merge_write_blocks = self
            .checkpoint
            .evidence
            .merge_written_bytes
            .div_ceil(BLOCK_BYTES as u64);
        self.checkpoint.evidence.peak_merge_temporary_bytes = self
            .checkpoint
            .evidence
            .peak_merge_temporary_bytes
            .max(measured_shape_bytes(&self.root)?);
        let shape = ConstructionShape {
            identities,
            node_details,
            edge_details,
            node_count,
            edge_count,
            max_node_surrogate,
            max_edge_surrogate,
        };
        let mut outputs = vec![receipt_for_existing(&self.root, &shape.identities)?];
        for name in shape.node_details.iter().chain(shape.edge_details.iter()) {
            outputs.push(receipt_for_existing(&self.root, name)?);
        }
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
        replace_control(
            &self.root,
            SHAPE_INTENT,
            &ShapeIntent {
                format_version: FORMAT_VERSION,
                operation_uuid: self.checkpoint.operation_uuid,
                project_identity: self.checkpoint.project_identity.clone(),
                session_identity: self.checkpoint.session_identity.clone(),
                parent_topology_generation: self.checkpoint.parent_topology_generation,
                budgets: self.checkpoint.budgets,
                last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
                complete: true,
                shape: Some(shape.clone()),
                outputs,
            },
        )?;
        Ok(shape)
    }

    /// Abort before seal. CURRENT remains unchanged.
    pub fn abort(&mut self) -> Result<(), GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        if self.checkpoint.state == GraphConstructionState::Sealed {
            return Err(storage("sealed session belongs to the publisher"));
        }
        self.checkpoint.state = GraphConstructionState::Aborted;
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    fn recover_intent(&mut self) -> Result<(), GfError> {
        let mut file = match self.root.open_child_file(OsStr::new(INTENT)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(storage(error)),
        };
        let intent: ChunkIntent = decode_bounded(&mut file)?;
        validate_intent(&intent, &self.checkpoint)?;
        let receipt_name = receipt_name(intent.sequence);
        match self.root.open_child_file(OsStr::new(&receipt_name)) {
            Ok(mut receipt_file) => {
                let receipt: ConstructionChunkReceipt = decode_bounded(&mut receipt_file)?;
                validate_receipt_semantics(&receipt, intent.sequence, self.checkpoint.budgets)?;
                if receipt != receipt_from_intent(&intent)? {
                    return Err(storage("recovered receipt differs from durable intent"));
                }
                validate_receipt_artifacts(&self.root, &receipt)?;
                let body = serde_json::to_vec(&receipt).map_err(storage)?;
                if intent.sequence < self.checkpoint.next_sequence
                    && self.checkpoint.last_receipt_sha256.as_deref()
                        != Some(sha256(&body).as_str())
                {
                    return Err(storage(
                        "completed intent receipt differs from checkpoint tail",
                    ));
                }
                let expected_pointer = ReceiptPointer {
                    operation_uuid: self.checkpoint.operation_uuid,
                    project_identity: self.checkpoint.project_identity.clone(),
                    session_identity: self.checkpoint.session_identity.clone(),
                    sequence: receipt.sequence,
                    receipt_sha256: sha256(&body),
                };
                match self.root.open_child_file(OsStr::new(&intent.chunk_key)) {
                    Ok(mut pointer_file) => {
                        let pointer: ReceiptPointer = decode_bounded(&mut pointer_file)?;
                        if pointer.operation_uuid != expected_pointer.operation_uuid
                            || pointer.project_identity != expected_pointer.project_identity
                            || pointer.session_identity != expected_pointer.session_identity
                            || pointer.sequence != expected_pointer.sequence
                            || pointer.receipt_sha256 != expected_pointer.receipt_sha256
                        {
                            return Err(storage("recovered receipt pointer is inconsistent"));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        install_control(&self.root, &intent.chunk_key, &expected_pointer)?;
                    }
                    Err(error) => return Err(storage(error)),
                }
                self.advance_checkpoint(&receipt, &body)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for artifact in [
                    intent.parquet.clone(),
                    intent.identities.clone(),
                    intent.endpoints.clone(),
                    intent.details.clone(),
                ]
                .into_iter()
                .flatten()
                {
                    authenticate_artifact(&self.root, &artifact)?;
                    unlink_artifact(&self.root, &artifact)?;
                }
                let stem = artifact_stem(intent.sequence, intent.kind);
                if intent.parquet.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.parquet"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.identities.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.identities.run"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.kind == ConstructionChunkKind::Edge && intent.endpoints.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.endpoints.run"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.details.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!(
                            "{stem}.{}-details.run",
                            if intent.kind == ConstructionChunkKind::Node {
                                "node"
                            } else {
                                "edge"
                            }
                        ),
                        intent.kind,
                        intent.rows,
                    )?;
                }
            }
            Err(error) => return Err(storage(error)),
        }
        unlink_named(&self.root, INTENT)
    }

    fn read_receipt(&self, sequence: u64) -> Result<ConstructionChunkReceipt, GfError> {
        let mut file = self
            .root
            .open_child_file(OsStr::new(&receipt_name(sequence)))
            .map_err(storage)?;
        let receipt = decode_bounded(&mut file)?;
        validate_receipt_semantics(&receipt, sequence, self.checkpoint.budgets)?;
        if receipt.operation_uuid != self.checkpoint.operation_uuid
            || receipt.project_identity != self.checkpoint.project_identity
            || receipt.session_identity != self.checkpoint.session_identity
            || receipt.parent_topology_generation != self.checkpoint.parent_topology_generation
        {
            return Err(storage("receipt authority differs from session checkpoint"));
        }
        Ok(receipt)
    }

    fn advance_checkpoint(
        &mut self,
        receipt: &ConstructionChunkReceipt,
        receipt_bytes: &[u8],
    ) -> Result<(), GfError> {
        if receipt.sequence < self.checkpoint.next_sequence {
            return Ok(());
        }
        if receipt.sequence != self.checkpoint.next_sequence {
            return Err(storage("receipt sequence is not checkpoint successor"));
        }
        let evidence = &mut self.checkpoint.evidence;
        evidence.input_rows = evidence.input_rows.saturating_add(receipt.rows);
        evidence.input_batches = evidence.input_batches.saturating_add(1);
        evidence.parquet_shards = evidence.parquet_shards.saturating_add(1);
        evidence.run_records = evidence.run_records.saturating_add(receipt.run_records);
        evidence.peak_batch_rows = evidence.peak_batch_rows.max(receipt.rows);
        evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(receipt.input_bytes);
        evidence.peak_run_records = evidence.peak_run_records.max(receipt.run_records);
        for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
            .into_iter()
            .chain(receipt.endpoints.iter())
        {
            evidence.write_bytes = evidence.write_bytes.saturating_add(artifact.bytes);
            evidence.write_operations = evidence
                .write_operations
                .saturating_add(artifact.write_operations);
        }
        self.checkpoint.next_sequence = self.checkpoint.next_sequence.saturating_add(1);
        self.checkpoint.saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
        self.checkpoint.last_receipt_sha256 = Some(sha256(receipt_bytes));
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    fn revalidate_authority(&self) -> Result<(), GfError> {
        self.project.revalidate_named().map_err(storage)?;
        self.root.revalidate_named().map_err(storage)?;
        if !self
            .checkpoint
            .project_identity
            .matches(self.project.identity())
            || !self
                .checkpoint
                .session_identity
                .matches(self.root.identity())
        {
            return Err(storage("retained construction authority identity changed"));
        }
        if let Some(snapshot) = &self.base_snapshot {
            snapshot.revalidate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReceiptPointer {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    sequence: u64,
    receipt_sha256: String,
}

struct RunArrays {
    identities: Vec<[u8; IDENTITY_WIDTH]>,
    endpoints: Vec<[u8; ENDPOINT_WIDTH]>,
    details: DetailRuns,
}

enum DetailRuns {
    Node(Vec<[u8; NODE_DETAIL_WIDTH]>),
    Edge(Vec<[u8; EDGE_DETAIL_WIDTH]>),
}

fn extract_runs(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<RunArrays, GfError> {
    let identity = uuid_column(
        batch,
        if kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        },
    )?;
    let mut identities = (0..batch.num_rows())
        .map(|row| uuid_value(identity, row))
        .collect::<Result<Vec<_>, _>>()?;
    identities.sort_unstable();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage("duplicate identity inside chunk"));
    }
    let mut endpoints = Vec::new();
    let details = if kind == ConstructionChunkKind::Edge {
        let edges = uuid_column(batch, "edge_uuid")?;
        let src = uuid_column(batch, "src_uuid")?;
        let dst = uuid_column(batch, "dst_uuid")?;
        let routes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
        endpoints.reserve(batch.num_rows().saturating_mul(2));
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let edge = uuid_value(edges, row)?;
            for (role, endpoint) in [uuid_value(src, row)?, uuid_value(dst, row)?]
                .into_iter()
                .enumerate()
            {
                let mut record = [0_u8; ENDPOINT_WIDTH];
                record[..16].copy_from_slice(&endpoint);
                record[16..32].copy_from_slice(&edge);
                record[32] = role as u8;
                endpoints.push(record);
            }
            let route = routes.value(row).as_bytes();
            let mut detail = [0_u8; EDGE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&edge);
            detail[16..32].copy_from_slice(&uuid_value(src, row)?);
            detail[32..48].copy_from_slice(&uuid_value(dst, row)?);
            detail[48] = u8::try_from(route.len())
                .map_err(|_| storage("canonical edge route exceeds identifier bound"))?;
            detail[49..49 + route.len()].copy_from_slice(route);
            details.push(detail);
        }
        endpoints.sort_unstable();
        details.sort_unstable();
        DetailRuns::Edge(details)
    } else {
        let nodes = uuid_column(batch, "node_uuid")?;
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| storage("canonical node type is not UInt32"))?;
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut detail = [0_u8; NODE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&uuid_value(nodes, row)?);
            detail[16..20].copy_from_slice(&types.value(row).to_be_bytes());
            details.push(detail);
        }
        details.sort_unstable();
        DetailRuns::Node(details)
    };
    Ok(RunArrays {
        identities,
        endpoints,
        details,
    })
}

fn validate_schema(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<(), GfError> {
    let expected = match kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    if batch.schema().as_ref() != expected.as_ref() {
        return Err(storage("construction batch schema is not canonical"));
    }
    for column in batch.columns() {
        if column.null_count() != 0 {
            return Err(storage("canonical construction columns are non-null"));
        }
    }
    if kind == ConstructionChunkKind::Edge {
        let routes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
        if routes
            .iter()
            .flatten()
            .any(|route| !graphforge_core::identifier::is_graph_identifier(route))
        {
            return Err(storage("invalid canonical edge route"));
        }
    }
    Ok(())
}

fn logical_batch_digest(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-logical-arrow/v1\0");
    digest.update(kind.tag().as_bytes());
    digest.update((batch.num_rows() as u64).to_be_bytes());
    match kind {
        ConstructionChunkKind::Node => {
            let uuids = uuid_column(batch, "node_uuid")?;
            let types = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| storage("canonical node type is not UInt32"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(uuids, row)?);
                digest.update(types.value(row).to_be_bytes());
            }
        }
        ConstructionChunkKind::Edge => {
            let edge = uuid_column(batch, "edge_uuid")?;
            let src = uuid_column(batch, "src_uuid")?;
            let dst = uuid_column(batch, "dst_uuid")?;
            let route = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(edge, row)?);
                digest.update(uuid_value(src, row)?);
                digest.update(uuid_value(dst, row)?);
                let value = route.value(row).as_bytes();
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value);
            }
        }
    }
    Ok(hex(&digest.finalize()))
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| storage(format!("missing canonical column {name}")))?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| storage(format!("{name} is not FixedSizeBinary(16)")))?;
    if array.value_length() != 16 || array.null_count() != 0 {
        return Err(storage(format!("{name} is not non-null UUID data")));
    }
    Ok(array)
}

fn uuid_value(array: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    array
        .value(row)
        .try_into()
        .map_err(|_| storage("UUID width changed"))
}

struct HashingWriter<W> {
    inner: W,
    digest: Sha256,
    bytes: u64,
    operations: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
            operations: 0,
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes = self.bytes.saturating_add(written as u64);
        self.operations = self.operations.saturating_add(1);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_parquet(
    root: &StableDirectory,
    name: &str,
    batch: &RecordBatch,
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file);
    let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut parquet = ArrowWriter::try_new(buffered, batch.schema(), None).map_err(storage)?;
    parquet.write(batch).map_err(storage)?;
    parquet.finish().map_err(storage)?;
    parquet.sync().map_err(storage)?;
    let hashing = parquet.inner().get_ref();
    hashing.inner.sync_all().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: hashing.bytes,
        sha256: hex(&hashing.digest.clone().finalize()),
        identity: identity.into(),
        write_operations: hashing.operations,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    Ok(receipt)
}

fn write_fixed_run<const N: usize>(
    root: &StableDirectory,
    name: &str,
    records: &[[u8; N]],
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = HashingWriter::new(file);
    let records_per_block = (BLOCK_BYTES / N).max(1);
    let mut block = Vec::with_capacity(records_per_block * N);
    for group in records.chunks(records_per_block) {
        block.clear();
        for record in group {
            block.extend_from_slice(record);
        }
        writer.write_all(&block).map_err(storage)?;
    }
    writer.flush().map_err(storage)?;
    writer.inner.sync_all().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: writer.bytes,
        sha256: hex(&writer.digest.finalize()),
        identity: identity.into(),
        write_operations: writer.operations,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    Ok(receipt)
}

fn encode_base_identity(identity: ConstructionUuidIdentity) -> [u8; BASE_IDENTITY_WIDTH] {
    let mut record = [0_u8; BASE_IDENTITY_WIDTH];
    record[..16].copy_from_slice(identity.uuid.as_bytes());
    record[16] = match identity.kind {
        crate::uuid_membership::UuidIndexKind::Node => 0,
        crate::uuid_membership::UuidIndexKind::Edge => 1,
    };
    // Retained parent member; session-converted records keep this byte zero.
    record[17] = 1;
    record[24..].copy_from_slice(&identity.surrogate.to_be_bytes());
    record
}

fn reject_existing_merge_artifacts(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if name.starts_with("merge-") || name.starts_with("shaped-") {
            return Err(storage("unowned construction shaping artifact exists"));
        }
    }
    Ok(())
}

fn planned_shape_names(
    chunks: usize,
    identity_inputs: usize,
    node_runs: usize,
    edge_runs: usize,
    endpoint_runs: usize,
    fan_in: usize,
) -> Vec<String> {
    let mut names = (0..chunks)
        .map(|sequence| format!("merge-unified-{sequence:020}.run"))
        .collect::<Vec<_>>();
    plan_merge_names(identity_inputs, "merge-identities", fan_in, &mut names);
    plan_merge_names(node_runs, "merge-node-details", fan_in, &mut names);
    plan_merge_names(edge_runs, "merge-edge-details", fan_in, &mut names);
    plan_merge_names(endpoint_runs, "merge-endpoints", fan_in, &mut names);
    names.push("shaped-identities.run".to_owned());
    names
}

fn plan_merge_names(mut count: usize, prefix: &str, fan_in: usize, names: &mut Vec<String>) {
    let mut level = 0;
    while count > 1 {
        let groups = count.div_ceil(fan_in);
        names.extend((0..groups).map(|group| format!("{prefix}-l{level:03}-g{group:08}.run")));
        count = groups;
        level += 1;
    }
}

fn recover_shape_intent(root: &StableDirectory, checkpoint: &Checkpoint) -> Result<(), GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let intent: ShapeIntent = decode_bounded(&mut file)?;
    validate_shape_binding(&intent, checkpoint)?;
    if intent.complete {
        let shape = intent
            .shape
            .as_ref()
            .ok_or_else(|| storage("complete shape manifest lacks output"))?;
        if intent.outputs.is_empty()
            || !intent
                .outputs
                .iter()
                .any(|item| item.name == shape.identities)
        {
            return Err(storage("complete shape manifest inventory is incomplete"));
        }
        for output in &intent.outputs {
            authenticate_shaped_output(root, output)?;
        }
        return Ok(());
    }
    if intent.shape.is_some() || !intent.outputs.is_empty() {
        return Err(storage("incomplete shape intent claims completed output"));
    }
    let mut node_runs = 0;
    let mut edge_runs = 0;
    let mut source_names = Vec::new();
    for sequence in 0..checkpoint.next_sequence {
        let mut file = root
            .open_child_file(OsStr::new(&receipt_name(sequence)))
            .map_err(storage)?;
        let receipt: ConstructionChunkReceipt = decode_bounded(&mut file)?;
        match receipt.kind {
            ConstructionChunkKind::Node => {
                node_runs += 1;
                source_names.push(format!("merge-node-source-{sequence:020}.run"));
            }
            ConstructionChunkKind::Edge => {
                edge_runs += 1;
                source_names.push(format!("merge-edge-source-{sequence:020}.run"));
                source_names.push(format!("merge-endpoint-source-{sequence:020}.run"));
            }
        }
    }
    let chunks = usize::try_from(checkpoint.next_sequence)
        .map_err(|_| storage("construction shape chunk count exceeds this platform"))?;
    let mut names = planned_shape_names(
        chunks,
        chunks + usize::from(checkpoint.base_identities.is_some()),
        node_runs,
        edge_runs,
        edge_runs,
        checkpoint.budgets.merge_fan_in,
    );
    names.extend(source_names);
    for name in names {
        match root.open_child_file(OsStr::new(&name)) {
            Ok(file) => {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("construction shape artifact has extra links"));
                }
                drop(file);
                unlink_named(root, &name)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
        }
    }
    unlink_named(root, SHAPE_INTENT)
}

fn validate_shape_binding(intent: &ShapeIntent, checkpoint: &Checkpoint) -> Result<(), GfError> {
    if intent.format_version != FORMAT_VERSION
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || intent.budgets != checkpoint.budgets
        || intent.last_receipt_sha256 != checkpoint.last_receipt_sha256
    {
        return Err(storage("construction shape manifest authority changed"));
    }
    Ok(())
}

fn read_completed_shape(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<Option<ConstructionShape>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let manifest: ShapeIntent = decode_bounded(&mut file)?;
    validate_shape_binding(&manifest, checkpoint)?;
    if !manifest.complete {
        return Err(storage("incomplete construction shape was not recovered"));
    }
    for output in &manifest.outputs {
        authenticate_shaped_output(root, output)?;
    }
    manifest
        .shape
        .ok_or_else(|| storage("complete shape manifest lacks output"))
        .map(Some)
}

fn receipt_for_existing(root: &StableDirectory, name: &str) -> Result<ArtifactReceipt, GfError> {
    let mut file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped output has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    let mut block = vec![0_u8; BLOCK_BYTES];
    loop {
        let count = file.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        bytes = bytes.saturating_add(count as u64);
        operations = operations.saturating_add(1);
    }
    Ok(ArtifactReceipt {
        name: name.to_owned(),
        bytes,
        sha256: hex(&digest.finalize()),
        identity: identity.into(),
        write_operations: operations,
    })
}

fn authenticate_shaped_output(
    root: &StableDirectory,
    expected: &ArtifactReceipt,
) -> Result<(), GfError> {
    if !is_shape_artifact_name(&expected.name) && !canonical_artifact_target(&expected.name) {
        return Err(storage("shape manifest output name is not canonical"));
    }
    let actual = receipt_for_existing(root, &expected.name)?;
    if actual.bytes != expected.bytes
        || actual.sha256 != expected.sha256
        || actual.identity != expected.identity
    {
        return Err(storage("shape manifest output authentication changed"));
    }
    Ok(())
}

fn is_shape_artifact_name(name: &str) -> bool {
    if name == "shaped-identities.run" {
        return true;
    }
    if let Some(sequence) = name
        .strip_prefix("merge-unified-")
        .and_then(|body| body.strip_suffix(".run"))
    {
        return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
    }
    for prefix in [
        "merge-node-source-",
        "merge-edge-source-",
        "merge-endpoint-source-",
    ] {
        if let Some(sequence) = name
            .strip_prefix(prefix)
            .and_then(|body| body.strip_suffix(".run"))
        {
            return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    [
        "merge-identities-l",
        "merge-node-details-l",
        "merge-edge-details-l",
        "merge-endpoints-l",
    ]
    .iter()
    .any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|tail| {
            tail.len() == 17
                && tail.get(3..5) == Some("-g")
                && tail.get(13..) == Some(".run")
                && tail[..3].bytes().all(|byte| byte.is_ascii_digit())
                && tail[5..13].bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

fn measured_shape_bytes(root: &StableDirectory) -> Result<u64, GfError> {
    let mut bytes = 0_u64;
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if is_shape_artifact_name(name) {
            bytes = bytes.saturating_add(
                root.open_child_file(OsStr::new(name))
                    .map_err(storage)?
                    .metadata()
                    .map_err(storage)?
                    .len(),
            );
        }
    }
    Ok(bytes)
}

fn account_merge_read<const N: usize>(evidence: &mut GraphConstructionEvidence) {
    evidence.merge_read_records = evidence.merge_read_records.saturating_add(1);
    evidence.merge_read_bytes = evidence.merge_read_bytes.saturating_add(N as u64);
}

fn account_merge_write<const N: usize>(evidence: &mut GraphConstructionEvidence) {
    evidence.merge_written_records = evidence.merge_written_records.saturating_add(1);
    evidence.merge_written_bytes = evidence.merge_written_bytes.saturating_add(N as u64);
}

fn convert_identity_run(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
    output: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.identities.name))
        .map_err(storage)?;
    if !receipt
        .identities
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("identity source authority changed before merge"));
    }
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, input);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, file);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(uuid) = read_fixed::<IDENTITY_WIDTH>(&mut reader)? {
        digest.update(uuid);
        bytes = bytes.saturating_add(IDENTITY_WIDTH as u64);
        let mut record = [0_u8; BASE_IDENTITY_WIDTH];
        record[..16].copy_from_slice(&uuid);
        record[16] = u8::from(receipt.kind == ConstructionChunkKind::Edge);
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<IDENTITY_WIDTH>(evidence);
        account_merge_write::<BASE_IDENTITY_WIDTH>(evidence);
    }
    if bytes != receipt.identities.bytes || hex(&digest.finalize()) != receipt.identities.sha256 {
        return Err(storage("identity source content changed before merge"));
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn copy_authenticated_run<const N: usize>(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    output: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    if !receipt
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("construction merge source authority changed"));
    }
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, input);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, file);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(record) = read_fixed::<N>(&mut reader)? {
        digest.update(record);
        bytes = bytes.saturating_add(N as u64);
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<N>(evidence);
        account_merge_write::<N>(evidence);
    }
    if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
        return Err(storage("construction merge source content changed"));
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn merge_optional<const N: usize>(
    root: &StableDirectory,
    inputs: Vec<String>,
    prefix: &str,
    fan_in: usize,
    reject_duplicates: bool,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<Option<String>, GfError> {
    if inputs.is_empty() {
        Ok(None)
    } else {
        merge_fixed_all::<N>(
            root,
            inputs,
            prefix,
            fan_in,
            reject_duplicates,
            cancelled,
            evidence,
        )
        .map(Some)
    }
}

fn merge_fixed_all<const N: usize>(
    root: &StableDirectory,
    mut inputs: Vec<String>,
    prefix: &str,
    fan_in: usize,
    reject_duplicates: bool,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    if inputs.is_empty() {
        return Err(storage("external merge has no input"));
    }
    let mut level = 0_usize;
    while inputs.len() > 1 {
        evidence.merge_passes = evidence.merge_passes.saturating_add(1);
        let mut outputs = Vec::with_capacity(inputs.len().div_ceil(fan_in));
        for (group, names) in inputs.chunks(fan_in).enumerate() {
            reject_cancelled(cancelled)?;
            let output = format!("{prefix}-l{level:03}-g{group:08}.run");
            merge_fixed_group::<N>(root, names, &output, reject_duplicates, cancelled, evidence)?;
            outputs.push(output);
        }
        for old in &inputs {
            if old.starts_with("merge-") && !outputs.contains(old) {
                unlink_named(root, old)?;
            }
        }
        inputs = outputs;
        level += 1;
    }
    Ok(inputs.pop().expect("one merge output"))
}

fn merge_fixed_group<const N: usize>(
    root: &StableDirectory,
    inputs: &[String],
    output: &str,
    reject_duplicates: bool,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|name| {
            root.open_child_file(OsStr::new(name))
                .map(|file| BufReader::with_capacity(BLOCK_BYTES, file))
                .map_err(storage)
        })
        .collect::<Result<Vec<_>, _>>()?;
    evidence.peak_merge_inputs = evidence.peak_merge_inputs.max(readers.len() as u64);
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, file);
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_fixed::<N>(reader)? {
            heap.push(Reverse((record, index)));
            account_merge_read::<N>(evidence);
        }
    }
    let mut previous = None;
    while let Some(Reverse((record, index))) = heap.pop() {
        if reject_duplicates
            && previous
                .as_ref()
                .is_some_and(|prior: &[u8; N]| prior[..16] == record[..16])
        {
            return Err(storage("duplicate identity across construction runs"));
        }
        writer.write_all(&record).map_err(storage)?;
        account_merge_write::<N>(evidence);
        previous = Some(record);
        if evidence.merge_written_records.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
        if let Some(next) = read_fixed::<N>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
            account_merge_read::<N>(evidence);
        }
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    evidence.merge_groups = evidence.merge_groups.saturating_add(1);
    evidence.peak_merge_temporary_bytes = evidence
        .peak_merge_temporary_bytes
        .max(measured_shape_bytes(root)?);
    Ok(())
}

fn read_fixed<const N: usize>(reader: &mut impl Read) -> Result<Option<[u8; N]>, GfError> {
    let mut record = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        match reader.read(&mut record[filled..]).map_err(storage)? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(storage("truncated fixed-width construction run")),
            count => filled += count,
        }
    }
    Ok(Some(record))
}

#[allow(clippy::too_many_arguments)]
fn validate_unified_and_details(
    root: &StableDirectory,
    identities_name: &str,
    node_details_name: Option<&str>,
    edge_details_name: Option<&str>,
    endpoints_name: Option<&str>,
    base_max_node: u64,
    base_max_edge: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(u64, u64, u64, u64), GfError> {
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    let mut node_count = 0_u64;
    let mut edge_count = 0_u64;
    let mut new_nodes = 0_u64;
    let mut new_edges = 0_u64;
    while let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)? {
        match record[16] {
            0 => {
                node_count += 1;
                if record[17] == 0 {
                    new_nodes += 1;
                }
            }
            1 => {
                edge_count += 1;
                if record[17] == 0 {
                    new_edges += 1;
                }
            }
            _ => return Err(storage("invalid identity kind in unified merge")),
        }
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        if (node_count + edge_count).is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    let detail_count = |name: Option<&str>, width: u64| -> Result<u64, GfError> {
        let Some(name) = name else { return Ok(0) };
        let bytes = root
            .open_child_file(OsStr::new(name))
            .map_err(storage)?
            .metadata()
            .map_err(storage)?
            .len();
        if bytes % width != 0 {
            return Err(storage("truncated canonical detail run"));
        }
        Ok(bytes / width)
    };
    if detail_count(node_details_name, NODE_DETAIL_WIDTH as u64)? != new_nodes
        || detail_count(edge_details_name, EDGE_DETAIL_WIDTH as u64)? != new_edges
    {
        return Err(storage("identity and canonical detail domains disagree"));
    }
    validate_endpoints(
        root,
        identities_name,
        endpoints_name,
        new_edges,
        cancelled,
        evidence,
    )?;
    let max_node = base_max_node
        .checked_add(new_nodes)
        .ok_or_else(|| storage("node surrogate overflow"))?;
    let max_edge = base_max_edge
        .checked_add(new_edges)
        .ok_or_else(|| storage("edge surrogate overflow"))?;
    Ok((node_count, edge_count, max_node, max_edge))
}

fn validate_endpoints(
    root: &StableDirectory,
    identities_name: &str,
    endpoints_name: Option<&str>,
    new_edges: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if new_edges == 0 {
        return if endpoints_name.is_none() {
            Ok(())
        } else {
            Err(storage("endpoint run exists without new edges"))
        };
    }
    let endpoints_name = endpoints_name.ok_or_else(|| storage("new edges lack endpoints"))?;
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    let mut endpoints = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(endpoints_name))
            .map_err(storage)?,
    );
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut endpoint_count = 0_u64;
    while let Some(endpoint) = read_fixed::<ENDPOINT_WIDTH>(&mut endpoints)? {
        while identity
            .as_ref()
            .is_some_and(|item| item[..16] < endpoint[..16])
        {
            identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        }
        let Some(node) = identity.as_ref() else {
            return Err(storage("edge endpoint UUID does not exist"));
        };
        if node[..16] != endpoint[..16] || node[16] != 0 {
            return Err(storage("edge endpoint is not a node UUID"));
        }
        if endpoint[32] > 1 || endpoint[33..].iter().any(|byte| *byte != 0) {
            return Err(storage("endpoint run record is not canonical"));
        }
        endpoint_count += 1;
        account_merge_read::<ENDPOINT_WIDTH>(evidence);
        if endpoint_count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    if endpoint_count != new_edges.saturating_mul(2) {
        return Err(storage(
            "edge endpoint cardinality differs from edge domain",
        ));
    }
    Ok(())
}

fn assign_surrogates(
    root: &StableDirectory,
    input_name: &str,
    mut node_tail: u64,
    mut edge_tail: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    let output = "shaped-identities.run";
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(input_name))
            .map_err(storage)?,
    );
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, file);
    let mut count = 0_u64;
    while let Some(mut record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut reader)? {
        if record[17] == 0 {
            let surrogate = match record[16] {
                0 => {
                    node_tail = node_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("node surrogate overflow"))?;
                    node_tail
                }
                1 => {
                    edge_tail = edge_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("edge surrogate overflow"))?;
                    edge_tail
                }
                _ => return Err(storage("invalid identity kind during shaping")),
            };
            record[24..32].copy_from_slice(&surrogate.to_be_bytes());
        } else if record[17] != 1 {
            return Err(storage("invalid retained identity marker during shaping"));
        }
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        account_merge_write::<BASE_IDENTITY_WIDTH>(evidence);
        count += 1;
        if count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    Ok(output.to_owned())
}

fn create_base_snapshot(
    project_dir: &Path,
    generation: u64,
    root: &StableDirectory,
) -> Result<
    (
        UuidConstructionSnapshot,
        ArtifactReceipt,
        UuidConstructionSnapshotWork,
    ),
    GfError,
> {
    let temporary = artifact_temp(BASE_IDENTITIES);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = HashingWriter::new(file);
    let records_per_block = (BLOCK_BYTES / BASE_IDENTITY_WIDTH).max(1);
    let block_bytes = records_per_block * BASE_IDENTITY_WIDTH;
    let mut block = Vec::with_capacity(block_bytes);
    let (snapshot, work) = open_uuid_construction_snapshot(project_dir, generation, |value| {
        block.extend_from_slice(&encode_base_identity(value));
        if block.len() == block_bytes {
            writer.write_all(&block).map_err(storage)?;
            block.clear();
        }
        Ok(())
    })?;
    if !block.is_empty() {
        writer.write_all(&block).map_err(storage)?;
    }
    writer.flush().map_err(storage)?;
    writer.inner.sync_all().map_err(storage)?;
    let receipt = ArtifactReceipt {
        name: BASE_IDENTITIES.to_owned(),
        bytes: writer.bytes,
        sha256: hex(&writer.digest.finalize()),
        identity: identity.into(),
        write_operations: writer.operations,
    };
    root.sync().map_err(storage)?;
    root.install_child(
        OsStr::new(&temporary),
        identity,
        OsStr::new(BASE_IDENTITIES),
    )
    .map_err(storage)?;
    root.sync().map_err(storage)?;
    Ok((snapshot, receipt, work))
}

fn authenticate_base_snapshot(
    project_dir: &Path,
    generation: u64,
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
) -> Result<(UuidConstructionSnapshot, UuidConstructionSnapshotWork), GfError> {
    let mut retained = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(BASE_IDENTITIES))
            .map_err(storage)?,
    );
    let (snapshot, work) = open_uuid_construction_snapshot(project_dir, generation, |value| {
        let mut record = [0_u8; BASE_IDENTITY_WIDTH];
        retained.read_exact(&mut record).map_err(storage)?;
        if record != encode_base_identity(value) {
            return Err(storage(
                "retained base identities differ from parent UUID authority",
            ));
        }
        Ok(())
    })?;
    let mut trailing = [0_u8; 1];
    if retained.read(&mut trailing).map_err(storage)? != 0 {
        return Err(storage("retained base identities have trailing records"));
    }
    authenticate_artifact(root, receipt)?;
    Ok((snapshot, work))
}

fn remove_unrecorded_base(root: &StableDirectory) -> Result<(), GfError> {
    let mut file = match root.open_child_file(OsStr::new(BASE_IDENTITIES)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = file_identity(&file).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1
        || file.metadata().map_err(storage)?.len() % BASE_IDENTITY_WIDTH as u64 != 0
    {
        return Err(storage("unrecorded base identity run is not session-owned"));
    }
    validate_sorted_run(&mut file, BASE_IDENTITY_WIDTH)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(BASE_IDENTITIES), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

#[derive(Clone, Copy)]
struct ReadWork {
    bytes: u64,
    operations: u64,
}

fn authenticate_artifact(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
) -> Result<ReadWork, GfError> {
    validate_artifact_name(receipt)?;
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    if !receipt
        .identity
        .matches(file_identity(&file).map_err(storage)?)
        || file_link_count(&file).map_err(storage)? != 1
    {
        return Err(storage("artifact identity or link count changed"));
    }
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, file);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    let width = if receipt.name.ends_with(".identities.run") {
        Some(if receipt.name == BASE_IDENTITIES {
            BASE_IDENTITY_WIDTH
        } else {
            IDENTITY_WIDTH
        })
    } else if receipt.name.ends_with(".endpoints.run") {
        Some(ENDPOINT_WIDTH)
    } else if receipt.name.ends_with(".node-details.run") {
        Some(NODE_DETAIL_WIDTH)
    } else if receipt.name.ends_with(".edge-details.run") {
        Some(EDGE_DETAIL_WIDTH)
    } else {
        None
    };
    let mut pending = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    loop {
        let count = reader.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        bytes = bytes.saturating_add(count as u64);
        operations = operations.saturating_add(1);
        if let Some(width) = width {
            pending.extend_from_slice(&block[..count]);
            let complete = pending.len() / width * width;
            for record in pending[..complete].chunks_exact(width) {
                if previous
                    .as_ref()
                    .is_some_and(|prior| prior.as_slice() >= record)
                {
                    return Err(storage("fixed construction run is not strictly sorted"));
                }
                if width == ENDPOINT_WIDTH
                    && (!matches!(record[32], 0 | 1) || record[33..].iter().any(|byte| *byte != 0))
                {
                    return Err(storage("endpoint run record is malformed"));
                }
                if width == EDGE_DETAIL_WIDTH {
                    let route_len = record[48] as usize;
                    if route_len == 0 || record[49 + route_len..].iter().any(|byte| *byte != 0) {
                        return Err(storage("edge detail run record is malformed"));
                    }
                }
                if width == BASE_IDENTITY_WIDTH
                    && (!matches!(record[16], 0 | 1)
                        || record[17] != 1
                        || record[18..24].iter().any(|byte| *byte != 0)
                        || (record[16] == 0 && record[24..].iter().all(|byte| *byte == 0))
                        || (record[16] == 1 && record[24..].iter().any(|byte| *byte != 0)))
                {
                    return Err(storage("base identity run record is malformed"));
                }
                previous = Some(record.to_vec());
            }
            pending.drain(..complete);
        }
    }
    if !pending.is_empty() {
        return Err(storage("fixed construction run has a truncated tail"));
    }
    if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
        return Err(storage("artifact digest or size changed"));
    }
    Ok(ReadWork { bytes, operations })
}

fn validate_artifact_name(receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let valid_suffix = receipt.name.ends_with(".parquet")
        || receipt.name.ends_with(".identities.run")
        || receipt.name.ends_with(".endpoints.run")
        || receipt.name.ends_with(".node-details.run")
        || receipt.name.ends_with(".edge-details.run")
        || receipt.name == BASE_IDENTITIES;
    if !valid_suffix
        || receipt.name.starts_with('.')
        || receipt.name.contains('/')
        || receipt.name.contains('\\')
        || receipt.sha256.len() != 64
        || !receipt.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || receipt.identity.file_id.len() != 32
        || !receipt
            .identity
            .file_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || ((receipt.bytes == 0 || receipt.write_operations == 0)
            && receipt.name != BASE_IDENTITIES)
    {
        return Err(storage("invalid construction artifact receipt"));
    }
    if receipt.name.ends_with(".identities.run")
        && receipt.bytes
            % if receipt.name == BASE_IDENTITIES {
                BASE_IDENTITY_WIDTH as u64
            } else {
                IDENTITY_WIDTH as u64
            }
            != 0
    {
        return Err(storage("truncated identity run"));
    }
    if receipt.name.ends_with(".endpoints.run") && receipt.bytes % ENDPOINT_WIDTH as u64 != 0 {
        return Err(storage("truncated endpoint run"));
    }
    if receipt.name.ends_with(".node-details.run") && receipt.bytes % NODE_DETAIL_WIDTH as u64 != 0
    {
        return Err(storage("truncated node detail run"));
    }
    if receipt.name.ends_with(".edge-details.run") && receipt.bytes % EDGE_DETAIL_WIDTH as u64 != 0
    {
        return Err(storage("truncated edge detail run"));
    }
    Ok(())
}

fn receipt_from_intent(intent: &ChunkIntent) -> Result<ConstructionChunkReceipt, GfError> {
    Ok(ConstructionChunkReceipt {
        operation_uuid: intent.operation_uuid,
        project_identity: intent.project_identity.clone(),
        session_identity: intent.session_identity.clone(),
        parent_topology_generation: intent.parent_topology_generation,
        prior_receipt_sha256: intent.prior_receipt_sha256.clone(),
        chunk_id: intent.chunk_id.clone(),
        sequence: intent.sequence,
        kind: intent.kind,
        rows: intent.rows,
        input_bytes: intent.input_bytes,
        input_sha256: intent.input_sha256.clone(),
        run_records: intent.run_records,
        parquet: intent
            .parquet
            .clone()
            .ok_or_else(|| storage("intent lacks Parquet artifact"))?,
        identities: intent
            .identities
            .clone()
            .ok_or_else(|| storage("intent lacks identity run"))?,
        endpoints: intent.endpoints.clone(),
        details: intent
            .details
            .clone()
            .ok_or_else(|| storage("intent lacks detail run"))?,
    })
}

fn validate_receipt_semantics(
    receipt: &ConstructionChunkReceipt,
    sequence: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    validate_chunk_id(&receipt.chunk_id)?;
    if receipt.sequence != sequence
        || receipt.operation_uuid.is_nil()
        || receipt.rows == 0
        || receipt.rows > budgets.max_batch_rows as u64
        || receipt.input_bytes > budgets.max_batch_bytes as u64
        || receipt.run_records > budgets.max_run_records as u64
        || receipt.input_sha256.len() != 64
        || !receipt
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || receipt.parquet.name != format!("{}.parquet", artifact_stem(sequence, receipt.kind))
        || receipt.identities.name
            != format!("{}.identities.run", artifact_stem(sequence, receipt.kind))
        || receipt.details.name
            != format!(
                "{}.{}-details.run",
                artifact_stem(sequence, receipt.kind),
                if receipt.kind == ConstructionChunkKind::Node {
                    "node"
                } else {
                    "edge"
                }
            )
        || (receipt.kind == ConstructionChunkKind::Node && receipt.endpoints.is_some())
        || (receipt.kind == ConstructionChunkKind::Edge && receipt.endpoints.is_none())
        || receipt.identities.bytes / IDENTITY_WIDTH as u64 != receipt.rows
        || receipt.run_records
            != receipt.rows
                * if receipt.kind == ConstructionChunkKind::Edge {
                    4
                } else {
                    2
                }
    {
        return Err(storage("receipt semantics are inconsistent"));
    }
    if let Some(endpoints) = &receipt.endpoints
        && (endpoints.name != format!("{}.endpoints.run", artifact_stem(sequence, receipt.kind))
            || endpoints.bytes / ENDPOINT_WIDTH as u64 != receipt.rows * 2)
    {
        return Err(storage("endpoint receipt semantics are inconsistent"));
    }
    let detail_width = if receipt.kind == ConstructionChunkKind::Node {
        NODE_DETAIL_WIDTH
    } else {
        EDGE_DETAIL_WIDTH
    };
    if receipt.details.bytes / detail_width as u64 != receipt.rows {
        return Err(storage("detail receipt semantics are inconsistent"));
    }
    validate_artifact_name(&receipt.parquet)?;
    validate_artifact_name(&receipt.identities)?;
    validate_artifact_name(&receipt.details)?;
    if let Some(endpoints) = &receipt.endpoints {
        validate_artifact_name(endpoints)?;
    }
    Ok(())
}

fn validate_receipt_artifacts(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<(), GfError> {
    for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
        .into_iter()
        .chain(receipt.endpoints.iter())
    {
        authenticate_artifact(root, artifact)?;
    }
    validate_parquet_shape(root, receipt)?;
    Ok(())
}

fn validate_parquet_shape(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<(), GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .map_err(storage)?;
    if !receipt
        .parquet
        .identity
        .matches(file_identity(&file).map_err(storage)?)
    {
        return Err(storage("Parquet identity changed during schema reopen"));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(storage)?;
    let expected = match receipt.kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    if builder.schema().as_ref() != expected.as_ref()
        || builder.metadata().file_metadata().num_rows() != receipt.rows as i64
    {
        return Err(storage("Parquet schema or row count differs from receipt"));
    }
    Ok(())
}

fn validate_intent(intent: &ChunkIntent, checkpoint: &Checkpoint) -> Result<(), GfError> {
    let stem = artifact_stem(intent.sequence, intent.kind);
    let expected_run_records =
        intent
            .rows
            .saturating_mul(if intent.kind == ConstructionChunkKind::Edge {
                4
            } else {
                2
            });
    if intent.format_version != FORMAT_VERSION
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || !(intent.sequence == checkpoint.next_sequence
            || intent.sequence.saturating_add(1) == checkpoint.next_sequence)
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || (intent.sequence == checkpoint.next_sequence
            && intent.prior_receipt_sha256 != checkpoint.last_receipt_sha256)
        || intent.chunk_key != chunk_key_name(&intent.chunk_id)
        || intent.rows == 0
        || intent.rows > checkpoint.budgets.max_batch_rows as u64
        || intent.input_bytes > checkpoint.budgets.max_batch_bytes as u64
        || intent.run_records > checkpoint.budgets.max_run_records as u64
        || intent.run_records != expected_run_records
        || intent.input_sha256.len() != 64
        || !intent
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || intent
            .parquet
            .as_ref()
            .is_some_and(|artifact| artifact.name != format!("{stem}.parquet"))
        || intent.identities.as_ref().is_some_and(|artifact| {
            artifact.name != format!("{stem}.identities.run")
                || artifact.bytes != intent.rows.saturating_mul(IDENTITY_WIDTH as u64)
        })
        || intent.endpoints.as_ref().is_some_and(|artifact| {
            intent.kind != ConstructionChunkKind::Edge
                || artifact.name != format!("{stem}.endpoints.run")
                || artifact.bytes
                    != intent
                        .rows
                        .saturating_mul(2)
                        .saturating_mul(ENDPOINT_WIDTH as u64)
        })
        || intent.details.as_ref().is_some_and(|artifact| {
            artifact.name
                != format!(
                    "{stem}.{}-details.run",
                    if intent.kind == ConstructionChunkKind::Node {
                        "node"
                    } else {
                        "edge"
                    }
                )
                || artifact.bytes
                    != intent
                        .rows
                        .saturating_mul(if intent.kind == ConstructionChunkKind::Node {
                            NODE_DETAIL_WIDTH
                        } else {
                            EDGE_DETAIL_WIDTH
                        } as u64)
        })
    {
        return Err(storage("durable intent is inconsistent with checkpoint"));
    }
    Ok(())
}

fn validate_checkpoint(
    checkpoint: &Checkpoint,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
    generation: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    if checkpoint.format_version != FORMAT_VERSION
        || checkpoint.operation_uuid != operation
        || !checkpoint.project_identity.matches(project)
        || !checkpoint.session_identity.matches(session)
        || checkpoint.parent_topology_generation != generation
        || checkpoint.budgets != budgets
        || checkpoint.base_identities.is_some() != (generation != 0)
        || checkpoint.next_sequence > budgets.max_chunks
        || checkpoint.last_receipt_sha256.is_some() != (checkpoint.next_sequence != 0)
        || checkpoint
            .last_receipt_sha256
            .as_ref()
            .is_some_and(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || checkpoint.evidence.input_batches != checkpoint.next_sequence
        || checkpoint.evidence.parquet_shards != checkpoint.next_sequence
        || checkpoint.evidence.peak_batch_rows > budgets.max_batch_rows as u64
        || checkpoint.evidence.peak_batch_bytes > budgets.max_batch_bytes as u64
        || checkpoint.evidence.peak_run_records > budgets.max_run_records as u64
        || checkpoint.evidence.prior_topology_rows_decoded != 0
        || checkpoint.evidence.current_transitions != 0
    {
        return Err(storage("checkpoint authority or resume parameters changed"));
    }
    if let Some(base) = &checkpoint.base_identities {
        validate_artifact_name(base)?;
        if base.name != BASE_IDENTITIES
            || base.bytes / BASE_IDENTITY_WIDTH as u64
                != checkpoint
                    .base_work
                    .live_nodes
                    .saturating_add(checkpoint.base_work.live_edges)
        {
            return Err(storage("checkpoint base identity receipt is inconsistent"));
        }
    }
    Ok(())
}

fn cleanup_authenticated_control_temps(
    root: &StableDirectory,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_control_temp(text) {
            continue;
        }
        let mut file = root.open_child_file(&name).map_err(storage)?;
        let body = read_bounded(&mut file)?;
        let authenticated =
            serde_json::from_slice::<Checkpoint>(&body).is_ok_and(|value| {
                value.format_version == FORMAT_VERSION
                    && value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            }) || serde_json::from_slice::<ChunkIntent>(&body).is_ok_and(|value| {
                value.format_version == FORMAT_VERSION
                    && value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            }) || serde_json::from_slice::<ConstructionChunkReceipt>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            }) || serde_json::from_slice::<ReceiptPointer>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            });
        if authenticated && file_link_count(&file).map_err(storage)? == 1 {
            let identity = file_identity(&file).map_err(storage)?;
            drop(file);
            root.unlink_child_if_identity(&name, identity)
                .map_err(storage)?;
            root.sync().map_err(storage)?;
        }
    }
    Ok(())
}

fn is_control_temp(name: &str) -> bool {
    let Some((prefix, suffix)) = name.rsplit_once('.') else {
        return false;
    };
    suffix == "tmp"
        && prefix.starts_with('.')
        && prefix.rsplit_once('-').is_some_and(|(_, random)| {
            random.len() == 32 && random.bytes().all(|b| b.is_ascii_hexdigit())
        })
}

fn cleanup_owned_artifact_temps(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_owned_artifact_temp(text) {
            continue;
        }
        let file = root.open_child_file(&name).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("incomplete artifact temp has unexpected links"));
        }
        let identity = file_identity(&file).map_err(storage)?;
        drop(file);
        root.unlink_child_if_identity(&name, identity)
            .map_err(storage)?;
        root.sync().map_err(storage)?;
    }
    Ok(())
}

fn is_owned_artifact_temp(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix(".artifact-")
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((target, random)) = body.rsplit_once('-') else {
        return false;
    };
    random.len() == 32
        && random.bytes().all(|byte| byte.is_ascii_hexdigit())
        && canonical_artifact_target(target)
}

fn canonical_artifact_target(name: &str) -> bool {
    if name == BASE_IDENTITIES || is_shape_artifact_name(name) {
        return true;
    }
    let Some(body) = name.strip_prefix("chunk-") else {
        return false;
    };
    let Some((sequence, tail)) = body.split_once('-') else {
        return false;
    };
    sequence.len() == 20
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && matches!(
            tail,
            "node.parquet"
                | "node.identities.run"
                | "edge.parquet"
                | "edge.identities.run"
                | "edge.endpoints.run"
                | "node.node-details.run"
                | "edge.edge-details.run"
        )
}

fn install_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = serde_json::to_vec(value).map_err(storage)?;
    if body.len() as u64 > MAX_CONTROL_BYTES {
        return Err(storage("control record exceeds bound"));
    }
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "install")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_temp_fsync.{target}"));
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_install.{target}"));
    Ok(())
}

fn replace_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = serde_json::to_vec(value).map_err(storage)?;
    if body.len() as u64 > MAX_CONTROL_BYTES {
        return Err(storage("control record exceeds bound"));
    }
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "replace")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_temp_fsync.{target}"));
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_replace.{target}"));
    Ok(())
}

fn decode_bounded<T: for<'de> Deserialize<'de>>(file: &mut File) -> Result<T, GfError> {
    serde_json::from_slice(&read_bounded(file)?).map_err(storage)
}

fn read_bounded(file: &mut File) -> Result<Vec<u8>, GfError> {
    if file.metadata().map_err(storage)?.len() > MAX_CONTROL_BYTES {
        return Err(storage("control record exceeds bound"));
    }
    let mut body = Vec::new();
    file.take(MAX_CONTROL_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(storage)?;
    if body.len() as u64 > MAX_CONTROL_BYTES {
        return Err(storage("control record exceeds bound"));
    }
    Ok(body)
}

fn unlink_named(root: &StableDirectory, name: &str) -> Result<(), GfError> {
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("control record link count changed"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn unlink_artifact(root: &StableDirectory, receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    if !receipt.identity.matches(identity) || file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("orphan artifact identity changed"));
    }
    drop(file);
    root.unlink_child_if_identity(OsStr::new(&receipt.name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn remove_unrecorded_artifact(
    root: &StableDirectory,
    name: &str,
    kind: ConstructionChunkKind,
    rows: u64,
) -> Result<(), GfError> {
    let mut file = match root.open_child_file(OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = file_identity(&file).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("unrecorded artifact has unexpected links"));
    }
    if name.ends_with(".parquet") {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(storage)?;
        let expected = match kind {
            ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
            ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
        };
        if builder.schema().as_ref() != expected.as_ref()
            || builder.metadata().file_metadata().num_rows() != rows as i64
        {
            return Err(storage("unrecorded Parquet artifact is not session-owned"));
        }
    } else {
        let width = if name.ends_with(".identities.run") {
            IDENTITY_WIDTH
        } else if name.ends_with(".endpoints.run") {
            ENDPOINT_WIDTH
        } else if name.ends_with(".node-details.run") {
            NODE_DETAIL_WIDTH
        } else if name.ends_with(".edge-details.run") {
            EDGE_DETAIL_WIDTH
        } else {
            return Err(storage("unrecorded artifact name is not canonical"));
        };
        let expected_records = if width == ENDPOINT_WIDTH {
            rows.saturating_mul(2)
        } else {
            rows
        };
        if file.metadata().map_err(storage)?.len() != expected_records.saturating_mul(width as u64)
        {
            return Err(storage("unrecorded fixed run row count changed"));
        }
        validate_sorted_run(&mut file, width)?;
    }
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn validate_sorted_run(file: &mut File, width: usize) -> Result<(), GfError> {
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, file);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut pending = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    loop {
        let count = reader.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        pending.extend_from_slice(&block[..count]);
        let complete = pending.len() / width * width;
        for record in pending[..complete].chunks_exact(width) {
            if previous
                .as_ref()
                .is_some_and(|prior| prior.as_slice() >= record)
                || (width == ENDPOINT_WIDTH
                    && (!matches!(record[32], 0 | 1) || record[33..].iter().any(|byte| *byte != 0)))
                || (width == EDGE_DETAIL_WIDTH
                    && (record[48] == 0
                        || record[49 + record[48] as usize..]
                            .iter()
                            .any(|byte| *byte != 0)))
                || (width == BASE_IDENTITY_WIDTH
                    && (!matches!(record[16], 0 | 1)
                        || record[17] != 1
                        || record[18..24].iter().any(|byte| *byte != 0)
                        || (record[16] == 0 && record[24..].iter().all(|byte| *byte == 0))
                        || (record[16] == 1 && record[24..].iter().any(|byte| *byte != 0))))
            {
                return Err(storage("unrecorded fixed run is malformed"));
            }
            previous = Some(record.to_vec());
        }
        pending.drain(..complete);
    }
    if !pending.is_empty() {
        return Err(storage("unrecorded fixed run has truncated tail"));
    }
    Ok(())
}

fn artifact_stem(sequence: u64, kind: ConstructionChunkKind) -> String {
    format!("chunk-{sequence:020}-{}", kind.tag())
}

fn receipt_name(sequence: u64) -> String {
    format!("receipt-{sequence:020}.json")
}

fn chunk_key_name(chunk_id: &str) -> String {
    format!("key-{}.json", sha256(chunk_id.as_bytes()))
}

fn control_temp(target: &str) -> OsString {
    OsString::from(format!(".{target}-{}.tmp", Uuid::new_v4().simple()))
}

fn artifact_temp(target: &str) -> OsString {
    OsString::from(format!(
        ".artifact-{target}-{}.tmp",
        Uuid::new_v4().simple()
    ))
}

fn validate_chunk_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(storage("invalid chunk id"));
    }
    Ok(())
}

fn reject_cancelled(cancelled: &mut impl FnMut() -> bool) -> Result<(), GfError> {
    if cancelled() {
        return Err(storage("construction cancelled"));
    }
    Ok(())
}

fn write_control_body(
    file: &mut File,
    body: &[u8],
    target: &str,
    operation: &str,
) -> Result<(), GfError> {
    #[cfg(test)]
    {
        let middle = body.len() / 2;
        file.write_all(&body[..middle]).map_err(storage)?;
        file.sync_all().map_err(storage)?;
        construction_failpoint(&format!("control.{operation}.after_partial.{target}"));
        file.write_all(&body[middle..]).map_err(storage)?;
    }
    #[cfg(not(test))]
    {
        let _ = (target, operation);
        file.write_all(body).map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
fn construction_failpoint(name: &str) {
    if std::env::var("GF_CONSTRUCTION_FAILPOINT_COOKIE").as_deref()
        == Ok("graphforge-construction-test-v1")
        && std::env::var("GF_CONSTRUCTION_FAILPOINT").as_deref() == Ok(name)
    {
        std::process::exit(86);
    }
}

#[cfg(not(test))]
fn construction_failpoint(_name: &str) {}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 15) as usize] as char);
    }
    value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryArray, StringArray, UInt32Array};
    use tempfile::TempDir;

    use super::*;

    fn fixed(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap()
    }

    fn node_batch(first: u128, rows: usize) -> RecordBatch {
        let uuids = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&uuids)),
                Arc::new(UInt32Array::from(vec![7_u32; rows])),
            ],
        )
        .unwrap()
    }

    fn edge_batch(first: u128, rows: usize) -> RecordBatch {
        let edges = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let src = (1..=rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let dst = (2..=rows as u128 + 1)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&edges)),
                Arc::new(fixed(&src)),
                Arc::new(fixed(&dst)),
                Arc::new(StringArray::from(vec!["R"; rows])),
            ],
        )
        .unwrap()
    }

    fn open(root: &TempDir, operation: u128) -> GraphConstructionSession {
        GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(operation),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap()
    }

    #[test]
    fn journal_is_constant_control_state_and_seal_reopens_every_artifact() {
        for chunks in [1_u64, 2, 4] {
            let root = TempDir::new().unwrap();
            let operation = 100 + u128::from(chunks);
            let mut session = open(&root, operation);
            for chunk in 0..chunks {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("n-{chunk}"),
                        &node_batch(1 + u128::from(chunk) * 32, 32),
                    )
                    .unwrap();
            }
            assert_eq!(session.accepted_chunks(), chunks);
            assert_eq!(session.evidence().input_rows, chunks * 32);
            assert_eq!(session.evidence().peak_batch_rows, 32);
            assert_eq!(session.evidence().peak_run_records, 64);
            assert_eq!(session.evidence().prior_topology_rows_decoded, 0);
            assert_eq!(session.evidence().current_transitions, 0);
            let checkpoint_bytes = session
                .root
                .open_child_file(OsStr::new(CHECKPOINT))
                .unwrap()
                .metadata()
                .unwrap()
                .len();
            assert!(checkpoint_bytes < MAX_CONTROL_BYTES);
            session.seal().unwrap();
            assert_eq!(session.state(), GraphConstructionState::Sealed);
            assert!(session.evidence().authentication_read_bytes > 0);
        }
    }

    #[test]
    fn shaping_is_bounded_deterministic_and_multipass_at_1x_2x_4x() {
        for chunks in [1_usize, 2, 4] {
            let root = TempDir::new().unwrap();
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(7_000 + chunks as u128),
                0,
                GraphConstructionBudgets {
                    merge_fan_in: 2,
                    ..GraphConstructionBudgets::default()
                },
            )
            .unwrap();
            for chunk in 0..chunks {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{chunk}"),
                        &node_batch(1 + chunk as u128 * 4, 4),
                    )
                    .unwrap();
            }
            session
                .append(
                    ConstructionChunkKind::Edge,
                    "edges",
                    &edge_batch(10_000, chunks * 2),
                )
                .unwrap();
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(shape.node_count, (chunks * 4) as u64);
            assert_eq!(shape.edge_count, (chunks * 2) as u64);
            assert_eq!(shape.max_node_surrogate, (chunks * 4) as u64);
            assert_eq!(shape.max_edge_surrogate, (chunks * 2) as u64);
            assert!(session.evidence().peak_merge_inputs <= 2);
            assert!(session.evidence().merge_read_bytes > 0);
            assert!(session.evidence().merge_written_bytes > 0);
            if chunks == 4 {
                assert!(session.evidence().merge_passes >= 2);
            }
        }
    }

    #[test]
    fn shaping_rejects_cross_kind_duplicates_and_missing_endpoints() {
        let root = TempDir::new().unwrap();
        let mut duplicate = open(&root, 8_001);
        duplicate
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        duplicate
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(1, 1))
            .unwrap();
        duplicate.seal().unwrap();
        assert!(
            duplicate
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("duplicate identity")
        );

        let root = TempDir::new().unwrap();
        let mut missing = open(&root, 8_002);
        missing
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        missing
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(20_000, 2))
            .unwrap();
        missing.seal().unwrap();
        assert!(
            missing
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("endpoint")
        );
    }

    #[test]
    fn shaping_cancellation_is_recovered_on_reopen() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 8_003);
        for chunk in 0..4 {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{chunk}"),
                    &node_batch(1 + chunk * 8, 8),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let mut polls = 0;
        assert!(
            session
                .shape_canonical_with_cancellation(|| {
                    polls += 1;
                    polls > 6
                })
                .is_err()
        );
        drop(session);
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(8_003),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        resumed.shape_canonical_with_cancellation(|| false).unwrap();
    }

    #[test]
    fn fixed_merge_reader_rejects_truncation_and_self_loop_is_valid() {
        assert!(read_fixed::<16>(&mut std::io::Cursor::new(vec![0_u8; 15])).is_err());
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 8_004);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
            .unwrap();
        let endpoint = [1_u128.to_be_bytes()];
        let edge = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&[9_000_u128.to_be_bytes()])),
                Arc::new(fixed(&endpoint)),
                Arc::new(fixed(&endpoint)),
                Arc::new(StringArray::from(vec!["R"])),
            ],
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Edge, "self-loop", &edge)
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.edge_count, 1);
        drop(session);
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(8_004),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(
            resumed.shape_canonical_with_cancellation(|| false).unwrap(),
            shape
        );
    }

    #[test]
    fn logical_digest_is_independent_of_arrow_slice_layout() {
        let whole = node_batch(10, 6);
        let sliced = whole.slice(2, 3);
        let rebuilt = node_batch(12, 3);
        assert_eq!(
            logical_batch_digest(ConstructionChunkKind::Node, &sliced).unwrap(),
            logical_batch_digest(ConstructionChunkKind::Node, &rebuilt).unwrap()
        );
    }

    #[test]
    fn replay_is_idempotent_and_reauthenticates_artifacts() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 200);
        let batch = node_batch(1, 8);
        let first = session
            .append(ConstructionChunkKind::Node, "nodes", &batch)
            .unwrap();
        assert_eq!(
            session
                .append(ConstructionChunkKind::Node, "nodes", &batch)
                .unwrap(),
            first
        );
        assert_eq!(session.accepted_chunks(), 1);
        assert_eq!(session.evidence().replayed_chunks, 1);
        assert!(
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 9))
                .is_err()
        );
    }

    #[test]
    fn cancellation_recovers_private_intent_without_accepting_a_chunk() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(300);
        let mut session = open(&root, 300);
        let mut polls = 0_u8;
        assert!(
            session
                .append_with_cancellation(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_batch(1, 8),
                    || {
                        polls = polls.saturating_add(1);
                        polls == 2
                    },
                )
                .is_err()
        );
        drop(session);
        let resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(resumed.accepted_chunks(), 0);
        assert!(resumed.root.open_child_file(OsStr::new(INTENT)).is_err());
    }

    #[test]
    fn node_after_edge_and_concurrent_same_process_open_fail_closed() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 400);
        assert!(
            GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(400),
                0,
                GraphConstructionBudgets::default()
            )
            .is_err()
        );
        session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
            .unwrap();
        assert!(
            session
                .append(ConstructionChunkKind::Node, "late-node", &node_batch(1, 1))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_substitution_is_rejected_on_independent_seal() {
        use std::os::unix::fs::symlink;

        let root = TempDir::new().unwrap();
        let mut session = open(&root, 500);
        let receipt = session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        let operation_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(500).simple().to_string());
        let artifact = operation_root.join(&receipt.identities.name);
        let displaced = operation_root.join("displaced.run");
        std::fs::rename(&artifact, &displaced).unwrap();
        symlink(&displaced, &artifact).unwrap();
        assert!(session.seal().is_err());
    }

    #[test]
    fn crash_subprocess_helper() {
        let Ok(root) = std::env::var("GF_CONSTRUCTION_CRASH_ROOT") else {
            return;
        };
        let mut session = GraphConstructionSession::open(
            Path::new(&root),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
            .unwrap();
    }

    #[test]
    fn subprocess_crashes_recover_each_durable_boundary() {
        let receipt = receipt_name(0);
        let key = chunk_key_name("nodes");
        let parquet = format!("{}.parquet", artifact_stem(0, ConstructionChunkKind::Node));
        let cases = vec![
            (
                "control.install.after_partial.checkpoint.json".to_owned(),
                0_u64,
            ),
            (
                "control.install.after_temp_fsync.checkpoint.json".to_owned(),
                0_u64,
            ),
            (
                "control.install.after_install.checkpoint.json".to_owned(),
                0,
            ),
            ("control.install.after_temp_fsync.intent.json".to_owned(), 0),
            ("control.install.after_install.intent.json".to_owned(), 0),
            (format!("artifact.after_temp_fsync.{parquet}"), 0),
            (format!("artifact.after_install.{parquet}"), 0),
            ("control.replace.after_replace.intent.json".to_owned(), 0),
            (format!("control.install.after_partial.{receipt}"), 0),
            (format!("control.install.after_temp_fsync.{receipt}"), 0),
            (format!("control.install.after_install.{receipt}"), 1),
            (format!("control.install.after_temp_fsync.{key}"), 1),
            (format!("control.install.after_install.{key}"), 1),
            (
                "control.replace.after_partial.checkpoint.json".to_owned(),
                1,
            ),
            (
                "control.replace.after_temp_fsync.checkpoint.json".to_owned(),
                1,
            ),
            (
                "control.replace.after_replace.checkpoint.json".to_owned(),
                1,
            ),
        ];
        for (failpoint, accepted) in cases {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::crash_subprocess_helper")
                .arg("--nocapture")
                .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", &failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "failpoint {failpoint}");
            let mut resumed = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(600),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            assert_eq!(resumed.accepted_chunks(), accepted, "{failpoint}");
            if accepted == 0 {
                resumed
                    .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
                    .unwrap();
            }
            resumed.seal().unwrap();
        }
    }
}
