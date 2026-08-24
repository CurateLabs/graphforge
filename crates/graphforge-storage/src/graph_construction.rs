//! Durable, bounded staging for one-generation graph construction.
//!
//! A construction session is deliberately not a repeated [`crate::GraphWriter`]
//! call. Each Arrow window is encoded directly to one immutable Parquet shard,
//! accompanied by sorted fixed-width identity/endpoint runs and an authenticated
//! receipt. No topology generation or public authority is changed while chunks
//! are accepted. The final generation-last publication consumes this sealed
//! inventory as one transaction.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch};
use graphforge_core::GfError;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const SESSION_FORMAT: u32 = 1;
const SESSION_DIR: &str = ".graphforge-construction";
const MANIFEST_FILE: &str = "session.json";
const BLOCK_BYTES: usize = 1 << 20;
const UUID_BYTES: usize = 16;
const ENDPOINT_RECORD_BYTES: usize = 48;
const MAX_CONTROL_BYTES: u64 = 16 << 20;

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction session: {error}"))
}

/// The two ordered construction phases.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConstructionChunkKind {
    /// Canonical node input. Must precede all edge chunks.
    Node,
    /// Canonical edge input.
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

/// Explicit fixed windows for a construction session.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphConstructionBudgets {
    /// Maximum rows in one accepted Arrow window.
    pub max_batch_rows: usize,
    /// Maximum Arrow-owned bytes in one accepted window.
    pub max_batch_bytes: usize,
    /// Maximum immutable chunks in one session.
    pub max_chunks: usize,
    /// Maximum fixed-width records buffered for sorting one chunk.
    pub max_run_records: usize,
    /// Maximum files opened by a later external merge group.
    pub merge_fan_in: usize,
}

impl Default for GraphConstructionBudgets {
    fn default() -> Self {
        Self {
            max_batch_rows: 65_536,
            max_batch_bytes: 64 << 20,
            // 8,192 65K-row windows cover more than 536 million accepted rows
            // while keeping the authenticated receipt inventory below its
            // explicit control-file bound.
            max_chunks: 8_192,
            max_run_records: 65_536 * 3,
            merge_fan_in: 32,
        }
    }
}

impl GraphConstructionBudgets {
    fn validate(self) -> Result<Self, GfError> {
        if self.max_batch_rows == 0
            || self.max_batch_bytes == 0
            || self.max_chunks == 0
            || self.max_chunks > 8_192
            || self.max_run_records < self.max_batch_rows
            || self.merge_fan_in < 2
        {
            return Err(storage("invalid construction budgets"));
        }
        Ok(self)
    }
}

/// Durable state of the private session. `Sealed` still does not change CURRENT.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphConstructionState {
    /// More chunks may be accepted.
    Staging,
    /// Inventory is complete and may be handed to one generation-last commit.
    Sealed,
    /// Caller abandoned the private session.
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ArtifactReceipt {
    relative_path: String,
    bytes: u64,
    sha256: String,
}

/// Authenticated, idempotent acknowledgement for one immutable Arrow chunk.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConstructionChunkReceipt {
    /// Caller-stable idempotency key.
    pub chunk_id: String,
    /// Monotonic accepted-chunk sequence.
    pub sequence: u64,
    /// Node or edge phase.
    pub kind: ConstructionChunkKind,
    /// Rows encoded into the immutable Parquet shard.
    pub rows: u64,
    /// Arrow-owned bytes charged for the accepted window.
    pub input_bytes: u64,
    /// Fixed-width records sorted for the chunk.
    pub run_records: u64,
    /// Canonical digest over the Arrow values and schema.
    pub input_sha256: String,
    parquet: ArtifactReceipt,
    identities: ArtifactReceipt,
    endpoints: Option<ArtifactReceipt>,
}

/// Aggregate-only evidence. It contains no graph identities.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphConstructionEvidence {
    /// Arrow rows accepted.
    pub input_rows: u64,
    /// Arrow batches accepted (idempotent replays excluded).
    pub input_batches: u64,
    /// Immutable Parquet shards sealed.
    pub parquet_shards: u64,
    /// Physical bytes written to private session artifacts and receipts.
    pub physical_bytes_written: u64,
    /// Physical bytes read while authenticating a resumed session.
    pub authentication_bytes_read: u64,
    /// Block-granular read operations.
    pub read_blocks: u64,
    /// Block-granular write operations.
    pub write_blocks: u64,
    /// Fixed-width identity/endpoint records written.
    pub run_records: u64,
    /// Maximum Arrow rows retained by one append call.
    pub peak_batch_rows: u64,
    /// Maximum Arrow-owned bytes retained by one append call.
    pub peak_batch_bytes: u64,
    /// Maximum fixed-width records sorted by one append call.
    pub peak_run_records: u64,
    /// Prior committed topology rows decoded while staging. Always zero.
    pub prior_topology_rows_decoded: u64,
    /// Public generation transitions while staging. Always zero.
    pub current_transitions: u64,
    /// Idempotent accepted replays.
    pub replayed_chunks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionManifest {
    format_version: u32,
    operation_uuid: Uuid,
    parent_topology_generation: u64,
    state: GraphConstructionState,
    budgets: GraphConstructionBudgets,
    receipts: Vec<ConstructionChunkReceipt>,
    evidence: GraphConstructionEvidence,
}

/// Storage-owned private construction session.
///
/// Opening an existing operation authenticates every receipt and referenced
/// artifact before accepting more input. Session files live below a private
/// project directory and are not graph-catalog discoverable.
pub struct GraphConstructionSession {
    root: PathBuf,
    manifest: SessionManifest,
}

impl GraphConstructionSession {
    /// Create or resume one stable operation.
    ///
    /// `parent_topology_generation` is pinned for the lifetime of the session;
    /// the eventual commit must reject a changed CURRENT rather than silently
    /// rebasing accepted chunks.
    pub fn open(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        let budgets = budgets.validate()?;
        let root = project_dir
            .join(SESSION_DIR)
            .join(operation_uuid.simple().to_string());
        fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join(MANIFEST_FILE);
        let mut session = if path.is_file() {
            let bytes = read_bounded(&path, MAX_CONTROL_BYTES)?;
            let manifest: SessionManifest = serde_json::from_slice(&bytes).map_err(storage)?;
            if manifest.format_version != SESSION_FORMAT
                || manifest.operation_uuid != operation_uuid
                || manifest.parent_topology_generation != parent_topology_generation
                || manifest.budgets != budgets
            {
                return Err(storage("resume parameters do not match durable session"));
            }
            Self { root, manifest }
        } else {
            let manifest = SessionManifest {
                format_version: SESSION_FORMAT,
                operation_uuid,
                parent_topology_generation,
                state: GraphConstructionState::Staging,
                budgets,
                receipts: Vec::new(),
                evidence: GraphConstructionEvidence::default(),
            };
            let session = Self { root, manifest };
            session.persist_manifest_create()?;
            session
        };
        session.recover_durable_receipts()?;
        session.authenticate_receipts()?;
        Ok(session)
    }

    /// Pinned parent topology generation.
    #[must_use]
    pub const fn parent_topology_generation(&self) -> u64 {
        self.manifest.parent_topology_generation
    }

    /// Current private state.
    #[must_use]
    pub const fn state(&self) -> GraphConstructionState {
        self.manifest.state
    }

    /// Aggregate bounded-work evidence.
    #[must_use]
    pub const fn evidence(&self) -> &GraphConstructionEvidence {
        &self.manifest.evidence
    }

    /// Durable receipts in accepted order.
    #[must_use]
    pub fn receipts(&self) -> &[ConstructionChunkReceipt] {
        &self.manifest.receipts
    }

    /// Accept one bounded Arrow chunk and seal its Parquet/run artifacts.
    ///
    /// Replaying `chunk_id` with byte-identical Arrow input is idempotent. A
    /// conflicting replay fails closed. Node chunks must precede edge chunks.
    pub fn append(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        if self.manifest.state != GraphConstructionState::Staging {
            return Err(storage("session is not accepting chunks"));
        }
        validate_chunk_id(chunk_id)?;
        if batch.num_rows() == 0 {
            return Err(storage("empty construction chunk"));
        }
        let bytes = batch.get_array_memory_size();
        if batch.num_rows() > self.manifest.budgets.max_batch_rows
            || bytes > self.manifest.budgets.max_batch_bytes
        {
            return Err(storage("Arrow chunk exceeds the configured window"));
        }
        if self.manifest.receipts.len() >= self.manifest.budgets.max_chunks {
            return Err(storage("construction chunk count exhausted"));
        }
        if kind == ConstructionChunkKind::Node
            && self
                .manifest
                .receipts
                .iter()
                .any(|receipt| receipt.kind == ConstructionChunkKind::Edge)
        {
            return Err(storage("node chunk cannot follow edge staging"));
        }
        let input_sha256 = digest_record_batch(batch);
        if let Some(existing) = self
            .manifest
            .receipts
            .iter()
            .find(|receipt| receipt.chunk_id == chunk_id)
            .cloned()
        {
            if existing.kind != kind
                || existing.rows != batch.num_rows() as u64
                || existing.input_sha256 != input_sha256
            {
                return Err(storage("conflicting construction chunk replay"));
            }
            self.manifest.evidence.replayed_chunks =
                self.manifest.evidence.replayed_chunks.saturating_add(1);
            self.persist_manifest_replace()?;
            return Ok(existing);
        }

        let arrays = extract_required_arrays(kind, batch)?;
        let run_records = arrays.identity.len().saturating_add(arrays.endpoints.len());
        if run_records > self.manifest.budgets.max_run_records {
            return Err(storage("chunk index-run window exhausted"));
        }
        let sequence = self.manifest.receipts.len() as u64;
        let stem = format!("{:020}-{}", sequence, kind.tag());
        // A crash before the receipt is durable may leave only this session's
        // exact next-sequence artifacts. They are not accepted input and are
        // safe to discard before retrying the same sequence.
        self.remove_unreceipted_sequence(&stem)?;
        let parquet = write_parquet_artifact(&self.root, &format!("{stem}.parquet"), batch)?;
        let identities = write_fixed_artifact(
            &self.root,
            &format!("{stem}.identities.run"),
            UUID_BYTES,
            arrays.identity,
        )?;
        let endpoints = if arrays.endpoints.is_empty() {
            None
        } else {
            Some(write_fixed_artifact(
                &self.root,
                &format!("{stem}.endpoints.run"),
                ENDPOINT_RECORD_BYTES,
                arrays.endpoints,
            )?)
        };
        let receipt = ConstructionChunkReceipt {
            chunk_id: chunk_id.to_owned(),
            sequence,
            kind,
            rows: batch.num_rows() as u64,
            input_bytes: bytes as u64,
            run_records: run_records as u64,
            input_sha256,
            parquet,
            identities,
            endpoints,
        };
        let receipt_path = self.root.join(format!("{stem}.receipt.json"));
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage)?;
        write_create_synced(&receipt_path, &receipt_bytes)?;

        let evidence = &mut self.manifest.evidence;
        evidence.input_rows = evidence.input_rows.saturating_add(batch.num_rows() as u64);
        evidence.input_batches = evidence.input_batches.saturating_add(1);
        evidence.parquet_shards = evidence.parquet_shards.saturating_add(1);
        evidence.physical_bytes_written = evidence
            .physical_bytes_written
            .saturating_add(receipt.parquet.bytes)
            .saturating_add(receipt.identities.bytes)
            .saturating_add(receipt.endpoints.as_ref().map_or(0, |item| item.bytes))
            .saturating_add(receipt_bytes.len() as u64);
        evidence.write_blocks = evidence
            .write_blocks
            .saturating_add(receipt.parquet.bytes.div_ceil(BLOCK_BYTES as u64))
            .saturating_add(receipt.identities.bytes.div_ceil(BLOCK_BYTES as u64))
            .saturating_add(
                receipt
                    .endpoints
                    .as_ref()
                    .map_or(0, |item| item.bytes.div_ceil(BLOCK_BYTES as u64)),
            )
            .saturating_add(1);
        evidence.run_records = evidence.run_records.saturating_add(receipt.run_records);
        evidence.peak_batch_rows = evidence.peak_batch_rows.max(batch.num_rows() as u64);
        evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(bytes as u64);
        evidence.peak_run_records = evidence.peak_run_records.max(run_records as u64);
        self.manifest.receipts.push(receipt.clone());
        self.persist_manifest_replace()?;
        Ok(receipt)
    }

    /// Seal the authenticated private inventory. This does not modify graph
    /// data, topology generation, or CURRENT.
    pub fn seal(&mut self) -> Result<(), GfError> {
        if self.manifest.state != GraphConstructionState::Staging {
            return Err(storage("only a staging session can be sealed"));
        }
        self.authenticate_receipts()?;
        self.manifest.state = GraphConstructionState::Sealed;
        self.persist_manifest_replace()
    }

    /// Mark the private session aborted. The prior graph remains authoritative.
    pub fn abort(&mut self) -> Result<(), GfError> {
        if self.manifest.state == GraphConstructionState::Sealed {
            return Err(storage(
                "sealed construction must be resolved by the publisher",
            ));
        }
        self.manifest.state = GraphConstructionState::Aborted;
        self.persist_manifest_replace()
    }

    fn authenticate_receipts(&mut self) -> Result<(), GfError> {
        let mut chunk_ids = BTreeSet::new();
        let mut expected_sequence = 0_u64;
        let mut saw_edge = false;
        let mut bytes_read = 0_u64;
        let mut blocks = 0_u64;
        for receipt in &self.manifest.receipts {
            if receipt.sequence != expected_sequence || !chunk_ids.insert(&receipt.chunk_id) {
                return Err(storage("non-canonical receipt sequence"));
            }
            if receipt.kind == ConstructionChunkKind::Node && saw_edge {
                return Err(storage("node receipt follows edge receipt"));
            }
            saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
            for artifact in [&receipt.parquet, &receipt.identities]
                .into_iter()
                .chain(receipt.endpoints.iter())
            {
                let (read, count) = authenticate_artifact(&self.root, artifact)?;
                bytes_read = bytes_read.saturating_add(read);
                blocks = blocks.saturating_add(count);
            }
            let stem = format!("{:020}-{}", receipt.sequence, receipt.kind.tag());
            let body = read_bounded(
                &self.root.join(format!("{stem}.receipt.json")),
                MAX_CONTROL_BYTES,
            )?;
            let durable: ConstructionChunkReceipt =
                serde_json::from_slice(&body).map_err(storage)?;
            if durable != *receipt {
                return Err(storage("receipt does not match session inventory"));
            }
            bytes_read = bytes_read.saturating_add(body.len() as u64);
            blocks = blocks.saturating_add(1);
            expected_sequence = expected_sequence.saturating_add(1);
        }
        self.manifest.evidence.authentication_bytes_read = self
            .manifest
            .evidence
            .authentication_bytes_read
            .saturating_add(bytes_read);
        self.manifest.evidence.read_blocks =
            self.manifest.evidence.read_blocks.saturating_add(blocks);
        Ok(())
    }

    fn recover_durable_receipts(&mut self) -> Result<(), GfError> {
        loop {
            let sequence = self.manifest.receipts.len() as u64;
            let prefix = format!("{sequence:020}-");
            let mut candidates = fs::read_dir(&self.root)
                .map_err(storage)?
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| name.starts_with(&prefix) && name.ends_with(".receipt.json"))
                .collect::<Vec<_>>();
            candidates.sort();
            if candidates.is_empty() {
                break;
            }
            if candidates.len() != 1 {
                return Err(storage("ambiguous durable receipt recovery"));
            }
            let body = read_bounded(&self.root.join(&candidates[0]), MAX_CONTROL_BYTES)?;
            let receipt: ConstructionChunkReceipt =
                serde_json::from_slice(&body).map_err(storage)?;
            if receipt.sequence != sequence
                || candidates[0]
                    != format!(
                        "{:020}-{}.receipt.json",
                        receipt.sequence,
                        receipt.kind.tag()
                    )
                || self
                    .manifest
                    .receipts
                    .iter()
                    .any(|existing| existing.chunk_id == receipt.chunk_id)
            {
                return Err(storage("invalid recovered construction receipt"));
            }
            for artifact in [&receipt.parquet, &receipt.identities]
                .into_iter()
                .chain(receipt.endpoints.iter())
            {
                authenticate_artifact(&self.root, artifact)?;
            }
            let evidence = &mut self.manifest.evidence;
            evidence.input_rows = evidence.input_rows.saturating_add(receipt.rows);
            evidence.input_batches = evidence.input_batches.saturating_add(1);
            evidence.parquet_shards = evidence.parquet_shards.saturating_add(1);
            evidence.physical_bytes_written = evidence
                .physical_bytes_written
                .saturating_add(receipt.parquet.bytes)
                .saturating_add(receipt.identities.bytes)
                .saturating_add(receipt.endpoints.as_ref().map_or(0, |item| item.bytes))
                .saturating_add(body.len() as u64);
            evidence.write_blocks = evidence
                .write_blocks
                .saturating_add(receipt.parquet.bytes.div_ceil(BLOCK_BYTES as u64))
                .saturating_add(receipt.identities.bytes.div_ceil(BLOCK_BYTES as u64))
                .saturating_add(
                    receipt
                        .endpoints
                        .as_ref()
                        .map_or(0, |item| item.bytes.div_ceil(BLOCK_BYTES as u64)),
                )
                .saturating_add(1);
            evidence.run_records = evidence.run_records.saturating_add(receipt.run_records);
            evidence.peak_batch_rows = evidence.peak_batch_rows.max(receipt.rows);
            evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(receipt.input_bytes);
            evidence.peak_run_records = evidence.peak_run_records.max(receipt.run_records);
            self.manifest.receipts.push(receipt);
            self.persist_manifest_replace()?;
        }
        Ok(())
    }

    fn remove_unreceipted_sequence(&self, stem: &str) -> Result<(), GfError> {
        for suffix in ["parquet", "identities.run", "endpoints.run"] {
            let path = self.root.join(format!("{stem}.{suffix}"));
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(storage(error)),
            }
        }
        sync_directory(&self.root)
    }

    fn persist_manifest_create(&self) -> Result<(), GfError> {
        let body = serde_json::to_vec(&self.manifest).map_err(storage)?;
        write_create_synced(&self.root.join(MANIFEST_FILE), &body)
    }

    fn persist_manifest_replace(&self) -> Result<(), GfError> {
        let body = serde_json::to_vec(&self.manifest).map_err(storage)?;
        let temporary = self
            .root
            .join(format!(".{MANIFEST_FILE}.{}.tmp", Uuid::new_v4()));
        write_create_synced(&temporary, &body)?;
        fs::rename(&temporary, self.root.join(MANIFEST_FILE)).map_err(storage)?;
        sync_directory(&self.root)
    }
}

struct ChunkArrays {
    identity: Vec<[u8; UUID_BYTES]>,
    endpoints: Vec<[u8; ENDPOINT_RECORD_BYTES]>,
}

fn extract_required_arrays(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<ChunkArrays, GfError> {
    let identity_name = match kind {
        ConstructionChunkKind::Node => "node_uuid",
        ConstructionChunkKind::Edge => "edge_uuid",
    };
    let identities = uuid_column(batch, identity_name)?;
    let mut identity = (0..batch.num_rows())
        .map(|index| uuid_value(identities, index))
        .collect::<Result<Vec<_>, _>>()?;
    identity.sort_unstable();
    if identity.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage("duplicate UUID inside construction chunk"));
    }
    let endpoints = if kind == ConstructionChunkKind::Edge {
        let src = uuid_column(batch, "src_uuid")?;
        let dst = uuid_column(batch, "dst_uuid")?;
        let edge = uuid_column(batch, "edge_uuid")?;
        let mut values = Vec::with_capacity(batch.num_rows().saturating_mul(2));
        for index in 0..batch.num_rows() {
            let edge_uuid = uuid_value(edge, index)?;
            for (role, endpoint) in [uuid_value(src, index)?, uuid_value(dst, index)?]
                .into_iter()
                .enumerate()
            {
                let mut record = [0_u8; ENDPOINT_RECORD_BYTES];
                record[..16].copy_from_slice(&endpoint);
                record[16..32].copy_from_slice(&edge_uuid);
                record[32] = role as u8;
                values.push(record);
            }
        }
        values.sort_unstable();
        values
    } else {
        Vec::new()
    };
    Ok(ChunkArrays {
        identity,
        endpoints,
    })
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| storage(format!("missing required column {name}")))?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| storage(format!("{name} must be FixedSizeBinary(16)")))?;
    if array.value_length() != 16 || array.null_count() != 0 {
        return Err(storage(format!(
            "{name} must be non-null FixedSizeBinary(16)"
        )));
    }
    Ok(array)
}

fn uuid_value(array: &FixedSizeBinaryArray, index: usize) -> Result<[u8; 16], GfError> {
    array
        .value(index)
        .try_into()
        .map_err(|_| storage("UUID value has invalid width"))
}

fn digest_record_batch(batch: &RecordBatch) -> String {
    let mut digest = Sha256::new();
    digest.update(format!("{:?}", batch.schema()).as_bytes());
    digest.update((batch.num_rows() as u64).to_be_bytes());
    for column in batch.columns() {
        digest_array_data(&column.to_data(), &mut digest);
    }
    hex_digest(digest.finalize().as_slice())
}

fn digest_array_data(data: &arrow::array::ArrayData, digest: &mut Sha256) {
    digest.update((data.len() as u64).to_be_bytes());
    digest.update((data.offset() as u64).to_be_bytes());
    for buffer in data.buffers() {
        digest.update((buffer.len() as u64).to_be_bytes());
        digest.update(buffer.as_slice());
    }
    if let Some(nulls) = data.nulls() {
        digest.update((nulls.offset() as u64).to_be_bytes());
        digest.update(nulls.buffer().as_slice());
    }
    digest.update((data.child_data().len() as u64).to_be_bytes());
    for child in data.child_data() {
        digest_array_data(child, digest);
    }
}

fn write_parquet_artifact(
    root: &Path,
    name: &str,
    batch: &RecordBatch,
) -> Result<ArtifactReceipt, GfError> {
    let path = root.join(name);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(storage)?;
    let mut writer = ArrowWriter::try_new(
        BufWriter::with_capacity(BLOCK_BYTES, file),
        batch.schema(),
        None,
    )
    .map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    writer.finish().map_err(storage)?;
    writer.sync().map_err(storage)?;
    writer.inner().get_ref().sync_all().map_err(storage)?;
    sync_directory(root)?;
    describe_artifact(root, name)
}

fn write_fixed_artifact<const N: usize>(
    root: &Path,
    name: &str,
    width: usize,
    records: Vec<[u8; N]>,
) -> Result<ArtifactReceipt, GfError> {
    if N != width {
        return Err(storage("fixed run width mismatch"));
    }
    let path = root.join(name);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(storage)?;
    let mut output = BufWriter::with_capacity(BLOCK_BYTES, file);
    for record in records {
        output.write_all(&record).map_err(storage)?;
    }
    output.flush().map_err(storage)?;
    output.get_ref().sync_all().map_err(storage)?;
    sync_directory(root)?;
    describe_artifact(root, name)
}

fn describe_artifact(root: &Path, name: &str) -> Result<ArtifactReceipt, GfError> {
    let path = root.join(name);
    let (sha256, bytes, _) = hash_file(&path)?;
    Ok(ArtifactReceipt {
        relative_path: name.to_owned(),
        bytes,
        sha256,
    })
}

fn authenticate_artifact(root: &Path, receipt: &ArtifactReceipt) -> Result<(u64, u64), GfError> {
    if receipt.relative_path.contains('/')
        || receipt.relative_path.contains('\\')
        || receipt.relative_path.starts_with('.')
    {
        return Err(storage("invalid artifact path"));
    }
    let (digest, bytes, blocks) = hash_file(&root.join(&receipt.relative_path))?;
    if bytes != receipt.bytes || digest != receipt.sha256 {
        return Err(storage("construction artifact authentication failed"));
    }
    if receipt.relative_path.ends_with(".identities.run") && bytes % UUID_BYTES as u64 != 0 {
        return Err(storage("truncated identity run"));
    }
    if receipt.relative_path.ends_with(".endpoints.run")
        && bytes % ENDPOINT_RECORD_BYTES as u64 != 0
    {
        return Err(storage("truncated endpoint run"));
    }
    Ok((bytes, blocks))
}

fn hash_file(path: &Path) -> Result<(String, u64, u64), GfError> {
    let file = File::open(path).map_err(storage)?;
    let mut input = BufReader::with_capacity(BLOCK_BYTES, file);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut blocks = 0_u64;
    loop {
        let count = input.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        bytes = bytes.saturating_add(count as u64);
        blocks = blocks.saturating_add(1);
    }
    Ok((hex_digest(digest.finalize().as_slice()), bytes, blocks))
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, GfError> {
    let file = File::open(path).map_err(storage)?;
    if file.metadata().map_err(storage)?.len() > maximum {
        return Err(storage("control file exceeds bound"));
    }
    let mut body = Vec::new();
    BufReader::new(file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(storage)?;
    if body.len() as u64 > maximum {
        return Err(storage("control file exceeds bound"));
    }
    Ok(body)
}

fn write_create_synced(path: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(storage)?;
    file.write_all(bytes).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    let parent = path
        .parent()
        .ok_or_else(|| storage("control path has no parent"))?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), GfError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(storage)
}

fn validate_chunk_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(storage("invalid construction chunk id"));
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::FixedSizeBinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    use super::*;

    fn uuid(seed: u128) -> [u8; 16] {
        seed.to_be_bytes()
    }

    fn fixed(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap()
    }

    fn node_batch(first: u128, rows: usize) -> RecordBatch {
        let values = (first..first + rows as u128).map(uuid).collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "node_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )])),
            vec![Arc::new(fixed(&values))],
        )
        .unwrap()
    }

    fn edge_batch(first: u128, rows: usize) -> RecordBatch {
        let edges = (first..first + rows as u128).map(uuid).collect::<Vec<_>>();
        let src = (0..rows)
            .map(|index| uuid(index as u128 + 1))
            .collect::<Vec<_>>();
        let dst = (0..rows)
            .map(|index| uuid(index as u128 + 2))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(fixed(&edges)),
                Arc::new(fixed(&src)),
                Arc::new(fixed(&dst)),
            ],
        )
        .unwrap()
    }

    fn open(root: &TempDir, operation: Uuid) -> GraphConstructionSession {
        GraphConstructionSession::open(
            root.path(),
            operation,
            7,
            GraphConstructionBudgets::default(),
        )
        .unwrap()
    }

    #[test]
    fn direct_chunks_resume_and_replay_without_publication() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9);
        let batch = node_batch(1, 8);
        let mut session = open(&root, operation);
        let first = session
            .append(ConstructionChunkKind::Node, "nodes-0", &batch)
            .unwrap();
        assert_eq!(first.rows, 8);
        assert_eq!(session.evidence().current_transitions, 0);
        drop(session);

        let mut resumed = open(&root, operation);
        assert_eq!(resumed.receipts(), &[first.clone()]);
        assert!(resumed.evidence().authentication_bytes_read > 0);
        assert_eq!(
            resumed
                .append(ConstructionChunkKind::Node, "nodes-0", &batch)
                .unwrap(),
            first
        );
        assert_eq!(resumed.receipts().len(), 1);
        assert_eq!(resumed.evidence().replayed_chunks, 1);
    }

    #[test]
    fn durable_receipt_recovers_crash_before_manifest_advance() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(91);
        let batch = node_batch(1, 8);
        let mut session = open(&root, operation);
        let receipt = session
            .append(ConstructionChunkKind::Node, "nodes-0", &batch)
            .unwrap();

        // Model the exact crash window: shard, run, and receipt are durable,
        // but the replace of session.json did not become authoritative.
        session.manifest.receipts.clear();
        session.manifest.evidence = GraphConstructionEvidence::default();
        session.persist_manifest_replace().unwrap();
        drop(session);

        let resumed = open(&root, operation);
        assert_eq!(resumed.receipts(), &[receipt]);
        assert_eq!(resumed.evidence().input_rows, 8);
        assert_eq!(resumed.evidence().input_batches, 1);
        assert_eq!(resumed.evidence().current_transitions, 0);
    }

    #[test]
    fn conflicting_replay_and_node_after_edge_fail_closed() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, Uuid::from_u128(10));
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        assert!(
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
                .is_err()
        );
        session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
            .unwrap();
        assert!(
            session
                .append(ConstructionChunkKind::Node, "late", &node_batch(20, 1))
                .is_err()
        );
    }

    #[test]
    fn truncated_run_fails_resume_authentication() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(11);
        let mut session = open(&root, operation);
        let receipt = session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
            .unwrap();
        let path = session.root.join(receipt.endpoints.unwrap().relative_path);
        drop(session);
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_len(47).unwrap();
        assert!(
            GraphConstructionSession::open(
                root.path(),
                operation,
                7,
                GraphConstructionBudgets::default()
            )
            .is_err()
        );
    }

    #[test]
    fn one_two_four_times_work_is_linear_and_windows_are_fixed() {
        let mut evidence = Vec::new();
        for chunks in [1_u64, 2, 4] {
            let root = TempDir::new().unwrap();
            let mut session = open(&root, Uuid::from_u128(100 + chunks as u128));
            for chunk in 0..chunks {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("n-{chunk}"),
                        &node_batch(1 + u128::from(chunk) * 32, 32),
                    )
                    .unwrap();
            }
            evidence.push(session.evidence().clone());
        }
        for (index, scale) in [1_u64, 2, 4].into_iter().enumerate() {
            assert_eq!(evidence[index].input_rows, scale * 32);
            assert_eq!(evidence[index].input_batches, scale);
            assert_eq!(evidence[index].parquet_shards, scale);
            assert_eq!(evidence[index].run_records, scale * 32);
            assert_eq!(evidence[index].peak_batch_rows, 32);
            assert_eq!(evidence[index].peak_run_records, 32);
            assert_eq!(evidence[index].prior_topology_rows_decoded, 0);
            assert_eq!(evidence[index].current_transitions, 0);
        }
    }

    #[test]
    fn seal_and_abort_preserve_private_state() {
        let root = TempDir::new().unwrap();
        let mut sealed = open(&root, Uuid::from_u128(20));
        sealed
            .append(ConstructionChunkKind::Node, "n", &node_batch(1, 1))
            .unwrap();
        sealed.seal().unwrap();
        assert_eq!(sealed.state(), GraphConstructionState::Sealed);
        assert!(
            sealed
                .append(ConstructionChunkKind::Node, "n2", &node_batch(2, 1))
                .is_err()
        );

        let mut aborted = open(&root, Uuid::from_u128(21));
        aborted.abort().unwrap();
        assert_eq!(aborted.state(), GraphConstructionState::Aborted);
        assert_eq!(aborted.evidence().current_transitions, 0);
    }
}
