//! Chunk spool: accepted chunks of an initial build, held for the bulk builder.
//!
//! An initial build reads every row once, so staging each chunk as Parquet, a
//! sorted identity run, an endpoint run and a detail run, then shaping and
//! re-encoding them, is work the bulk builder does not need. The spool keeps
//! the staged path's promise (an accepted chunk survives a crash and resumes)
//! with the least durable work that keeps it: each accepted chunk is one Arrow
//! IPC file, written to a temporary name, synced, renamed to its sequence
//! name, and the directory is synced. The file is its own receipt: its footer
//! carries the chunk id, kind, sequence and digests. No receipt journal,
//! chunk key or checkpoint is rewritten per chunk.
//!
//! Trust boundary. Acceptance acknowledges a chunk only after its file's data
//! is synced, the file is renamed to its sequence name, and the directory is
//! synced, so no crash leaves an acknowledged chunk missing, truncated or
//! half-written: an unacknowledged temporary file is all a crash can leave.
//! Reopening therefore trusts the footer descriptors, which name this session
//! and carry the digests acknowledged at acceptance, and every decode
//! authenticates the batch against them (see `read_authenticated_chunk`). A
//! person who edits or deletes files inside the private session directory and
//! rewrites the footers to match is outside the threat model, as for the staged
//! artifacts; a hole in the sequence is still refused as non-contiguous.
//!
//! Opening a spooled session scans the directory and rebuilds the accepted
//! chunks from those footers. A temporary file was never acknowledged and is
//! removed. At seal the bulk builder reads the files in place (see
//! [`SpoolReader`]), in memory or on scratch files, with the node tables on
//! scratch too when they exceed the memory budget.

use std::collections::{BTreeSet, HashMap};
use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use super::intake::{logical_batch_digest, precheck_chunk, validate_chunk_content};
use super::{
    ArtifactReceipt, BLOCK_BYTES, ConstructionChunkKind, ConstructionChunkReceipt, Deserialize,
    GfError, GraphConstructionEncoding, GraphConstructionEvidence, GraphConstructionSession,
    GraphConstructionState, HashingWriter, IdentityRecord, OsStr, Serialize, StableDirectory, Uuid,
    artifact_temp, construction_failpoint, file_identity, normalized_schema_digest,
    reject_cancelled, replace_checkpoint_control, storage, unlink_named,
};
use crate::graph_construction_encoding::{BulkBatchReader, BulkBuildPlan, BulkSource};

/// Directory of the spool inside the session root. Not a shape-scoped name.
const SPOOL_DIR: &str = "chunk-spool";
/// Footer metadata key holding the chunk's [`SpoolDescriptor`].
const DESCRIPTOR_KEY: &str = "graphforge.chunk-spool.v1";
/// Rows a bulk-builder task decodes: consecutive chunks are grouped until they
/// reach it, so a spool of tiny chunks does not become tiny tasks.
const TASK_ROWS: u64 = 65_536;

/// Where a session's accepted chunks go. Decided once, before the first chunk.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChunkRoute {
    /// Nothing spooled: either no chunk is accepted yet, or chunks are staged
    /// as Parquet and runs (appends).
    #[default]
    Undecided,
    /// Chunks are spooled for the bulk builder.
    Spool,
}

impl ChunkRoute {
    // serde's `skip_serializing_if` passes a reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) const fn is_undecided(&self) -> bool {
        matches!(self, Self::Undecided)
    }
}

/// What the caller asked for before its first chunk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ChunkPreference {
    /// Stage chunks (the default, and every append).
    #[default]
    Stage,
    /// Spool chunks for the bulk builder when the build is an empty initial one.
    Spool,
}

/// How a spooled session builds. Recorded in the checkpoint before any build
/// work starts: it closes the session to further chunks, and a retry reads it
/// back instead of deciding again.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SealRoute {
    /// Read the spooled chunks in place with the bulk builder.
    Bulk,
}

/// The one file a spooled chunk occupies.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct SpoolArtifact {
    name: String,
    bytes: u64,
    allocated_bytes: u64,
}

/// What a chunk's spool file says about itself, in its footer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SpoolDescriptor {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    sequence: u64,
    chunk_id: String,
    kind: ConstructionChunkKind,
    rows: u64,
    input_bytes: u64,
    input_sha256: String,
    schema_sha256: String,
    property_free: bool,
}

#[derive(Clone, Debug)]
struct SpoolChunk {
    descriptor: SpoolDescriptor,
    bytes: u64,
    allocated_bytes: u64,
}

/// Input counters of a spooled session's chunks.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct SpoolTotals {
    chunks: u64,
    rows: u64,
    peak_batch_rows: u64,
    peak_batch_bytes: u64,
    peak_run_records: u64,
    /// What this process wrote and synced to accept the chunks. Chunks
    /// accepted by an earlier process are not re-measured.
    write_bytes: u64,
    write_operations: u64,
    fsync_operations: u64,
    /// Exact replays this process answered.
    replayed_chunks: u64,
}

impl SpoolTotals {
    pub(super) const fn chunks(&self) -> u64 {
        self.chunks
    }
}

/// Accepted chunks of a spooled session. Rebuilt from the spool on open.
pub(super) struct SpoolState {
    directory: StableDirectory,
    chunks: Vec<SpoolChunk>,
    by_id: HashMap<String, usize>,
    saw_edge: bool,
    node_schemas: BTreeSet<String>,
    edge_schemas: BTreeSet<String>,
    /// I/O this process performed accepting chunks, and replays it answered.
    io: SpoolTotals,
}

impl SpoolState {
    fn new(directory: StableDirectory) -> Self {
        Self {
            io: SpoolTotals::default(),
            directory,
            chunks: Vec::new(),
            by_id: HashMap::new(),
            saw_edge: false,
            node_schemas: BTreeSet::new(),
            edge_schemas: BTreeSet::new(),
        }
    }

    pub(super) fn len(&self) -> u64 {
        self.chunks.len() as u64
    }

    fn totals(&self) -> SpoolTotals {
        self.chunks.iter().fold(self.io, |total, chunk| {
            let descriptor = &chunk.descriptor;
            SpoolTotals {
                chunks: total.chunks + 1,
                rows: total.rows + descriptor.rows,
                peak_batch_rows: total.peak_batch_rows.max(descriptor.rows),
                peak_batch_bytes: total.peak_batch_bytes.max(descriptor.input_bytes),
                write_bytes: total.write_bytes,
                write_operations: total.write_operations,
                fsync_operations: total.fsync_operations,
                replayed_chunks: total.replayed_chunks,
                peak_run_records: total.peak_run_records.max(
                    descriptor.rows
                        * if descriptor.kind == ConstructionChunkKind::Edge {
                            4
                        } else {
                            2
                        },
                ),
            }
        })
    }

    fn push(&mut self, chunk: SpoolChunk) {
        let descriptor = &chunk.descriptor;
        self.by_id
            .insert(descriptor.chunk_id.clone(), self.chunks.len());
        match descriptor.kind {
            ConstructionChunkKind::Node => {
                self.node_schemas.insert(descriptor.schema_sha256.clone());
            }
            ConstructionChunkKind::Edge => {
                self.saw_edge = true;
                self.edge_schemas.insert(descriptor.schema_sha256.clone());
            }
        }
        self.chunks.push(chunk);
    }
}

/// Measurements of the spool, for the chunks accepted so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkSpoolEvidence {
    /// Chunks spooled.
    pub chunks: u64,
    /// Rows in them.
    pub rows: u64,
    /// Bytes the spool files occupy.
    pub bytes: u64,
}

fn chunk_name(sequence: u64, kind: ConstructionChunkKind) -> String {
    format!("chunk-{sequence:020}-{}.arrow", kind.tag())
}

fn parse_chunk_name(name: &str) -> Option<(u64, ConstructionChunkKind)> {
    let body = name.strip_prefix("chunk-")?.strip_suffix(".arrow")?;
    let (sequence, tag) = body.split_once('-')?;
    if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let kind = match tag {
        "node" => ConstructionChunkKind::Node,
        "edge" => ConstructionChunkKind::Edge,
        _ => return None,
    };
    Some((sequence.parse().ok()?, kind))
}

/// The placeholder for a staged artifact a spooled chunk does not have. Staged
/// receipt validation refuses it, so a spooled receipt can never be mistaken
/// for a staged one.
fn absent_artifact() -> ArtifactReceipt {
    ArtifactReceipt {
        name: String::new(),
        bytes: 0,
        allocated_bytes: 0,
        xxh64: String::new(),
        identity: IdentityRecord {
            volume_serial: 0,
            file_id: String::new(),
        },
        write_operations: 0,
        fsync_operations: 0,
    }
}

/// Write one chunk file: temporary name, data sync, rename, directory sync.
fn write_chunk_file(
    directory: &StableDirectory,
    name: &str,
    batch: &RecordBatch,
    descriptor: &SpoolDescriptor,
) -> Result<(SpoolArtifact, u64, u64), GfError> {
    let temporary = artifact_temp(name);
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file)?;
    let mut writer = FileWriter::try_new(
        BufWriter::with_capacity(BLOCK_BYTES, hashing),
        batch.schema_ref(),
    )
    .map_err(storage)?;
    writer.write_metadata(
        DESCRIPTOR_KEY,
        serde_json::to_string(descriptor).map_err(storage)?,
    );
    writer.write(batch).map_err(storage)?;
    writer.finish().map_err(storage)?;
    let mut buffered = writer.into_inner().map_err(storage)?;
    buffered.flush().map_err(storage)?;
    let mut hashing = buffered
        .into_inner()
        .map_err(|error| storage(error.to_string()))?;
    directory
        .seal_cache_writer(&mut hashing.inner)
        .map_err(storage)?;
    construction_failpoint(&format!("spool.after_temp_fsync.{name}"));
    let allocated_bytes = graphforge_filesystem::file_space_usage(hashing.inner.file())
        .map_err(storage)?
        .allocated_bytes;
    let bytes = hashing.bytes;
    let write_operations = hashing.operations;
    let file_syncs = hashing.inner.evidence().sync_operations;
    directory
        .install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    construction_failpoint(&format!("spool.after_rename.{name}"));
    directory.acknowledge().map_err(storage)?;
    construction_failpoint(&format!("spool.after_install.{name}"));
    Ok((
        SpoolArtifact {
            name: name.to_owned(),
            bytes,
            allocated_bytes,
        },
        write_operations,
        // The file's own syncs and the directory sync that makes the rename durable.
        file_syncs + 1,
    ))
}

/// Read a chunk file's footer descriptor without decoding any column.
fn read_descriptor(
    directory: &StableDirectory,
    name: &str,
) -> Result<(SpoolDescriptor, u64), GfError> {
    let file = directory
        .open_child_file(OsStr::new(name))
        .map_err(storage)?;
    let bytes = file.metadata().map_err(storage)?.len();
    let reader = FileReader::try_new(file, None).map_err(storage)?;
    let text = reader
        .custom_metadata()
        .get(DESCRIPTOR_KEY)
        .ok_or_else(|| storage("spooled chunk lacks its descriptor"))?;
    Ok((serde_json::from_str(text).map_err(storage)?, bytes))
}

/// Decode one spooled chunk and authenticate it against what was acknowledged.
///
/// Acceptance acknowledged a chunk by its logical digest (identities, labels or
/// endpoints and routes, and every property value) and its schema digest. A
/// file that no longer decodes to exactly that is corrupt, including one that
/// still parses as valid Arrow IPC at the same length, so nothing is replayed
/// into a build or the staged path before it matches.
fn read_authenticated_chunk(
    directory: &StableDirectory,
    descriptor: &SpoolDescriptor,
) -> Result<RecordBatch, GfError> {
    let name = chunk_name(descriptor.sequence, descriptor.kind);
    let file = directory
        .open_child_file(OsStr::new(&name))
        .map_err(storage)?;
    let mut reader = FileReader::try_new(file, None).map_err(storage)?;
    let footer = reader
        .custom_metadata()
        .get(DESCRIPTOR_KEY)
        .ok_or_else(|| storage("spooled chunk lacks its descriptor"))?;
    if serde_json::from_str::<SpoolDescriptor>(footer).map_err(storage)? != *descriptor {
        return Err(storage("spooled chunk differs from its descriptor"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| storage("spooled chunk has no batch"))?
        .map_err(storage)?;
    if reader.next().is_some()
        || batch.num_rows() as u64 != descriptor.rows
        || normalized_schema_digest(batch.schema().as_ref()) != descriptor.schema_sha256
        || logical_batch_digest(descriptor.kind, &batch)? != descriptor.input_sha256
    {
        return Err(storage(
            "spooled chunk differs from its acknowledged digest",
        ));
    }
    Ok(batch)
}

/// Decodes the spool's chunks for the bulk builder. Consecutive chunks of one
/// kind form a task; every task yields the chunks' batches in sequence order.
struct SpoolReader {
    directory: StableDirectory,
    tasks: Vec<Vec<SpoolDescriptor>>,
    /// Largest decode transient of any one chunk: the file body and its arrays.
    decoded_workspace: u64,
}

impl BulkBatchReader for SpoolReader {
    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        for descriptor in &self.tasks[task] {
            sink(read_authenticated_chunk(&self.directory, descriptor)?)?;
        }
        Ok(())
    }

    fn admitted(&self) -> bool {
        true
    }

    fn decoded_workspace_bytes(&self) -> u64 {
        self.decoded_workspace
    }

    fn task_rows(&self, task: usize) -> usize {
        self.tasks[task]
            .iter()
            .map(|descriptor| usize::try_from(descriptor.rows).unwrap_or(usize::MAX))
            .sum()
    }
}

impl GraphConstructionSession {
    /// Ask that this session's chunks be spooled for the bulk builder rather
    /// than staged. Takes effect only on an empty initial build that has not
    /// accepted a chunk; otherwise the session keeps its route. Call before the
    /// first `append`.
    pub fn spool_chunks(&mut self) {
        self.chunk_preference = ChunkPreference::Spool;
    }

    /// Whether this session's chunks are spooled.
    #[must_use]
    pub fn is_spooled(&self) -> bool {
        self.checkpoint.chunk_route == ChunkRoute::Spool
    }

    /// The seal route recorded for this spooled session, if any.
    #[must_use]
    pub fn seal_route(&self) -> Option<SealRoute> {
        self.checkpoint.seal_route
    }

    /// The session's evidence as a caller sees it: the checkpoint's evidence
    /// with the input counters of the spooled chunks, which no checkpoint
    /// stores because accepting a chunk rewrites nothing.
    #[must_use]
    pub fn reported_evidence(&self) -> GraphConstructionEvidence {
        let mut evidence = self.checkpoint.evidence.clone();
        let totals = self
            .checkpoint
            .spool_totals
            .or_else(|| self.spool.as_ref().map(SpoolState::totals));
        if let Some(totals) = totals {
            evidence.input_rows = totals.rows;
            evidence.input_batches = totals.chunks;
            evidence.spooled_chunks = totals.chunks;
            evidence.peak_batch_rows = totals.peak_batch_rows;
            evidence.peak_batch_bytes = totals.peak_batch_bytes;
            evidence.peak_run_records = totals.peak_run_records;
            evidence.write_bytes += totals.write_bytes;
            evidence.write_operations += totals.write_operations;
            evidence.fsync_operations += totals.fsync_operations;
            evidence.replayed_chunks += totals.replayed_chunks;
        }
        evidence
    }

    /// Measurements of the chunks spooled so far.
    #[must_use]
    pub fn chunk_spool_evidence(&self) -> ChunkSpoolEvidence {
        self.spool.as_ref().map_or_else(Default::default, |spool| {
            spool
                .chunks
                .iter()
                .fold(ChunkSpoolEvidence::default(), |total, chunk| {
                    ChunkSpoolEvidence {
                        chunks: total.chunks + 1,
                        rows: total.rows + chunk.descriptor.rows,
                        bytes: total.bytes + chunk.bytes,
                    }
                })
        })
    }

    /// Append one canonical bounded Arrow chunk, polling a caller-owned
    /// cancellation signal at durable boundaries.
    ///
    /// # Errors
    /// Refuses a chunk the session cannot accept, and returns storage errors.
    pub fn append_with_cancellation(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        if self.checkpoint.chunk_route == ChunkRoute::Undecided
            && self.chunk_preference == ChunkPreference::Spool
            && self.checkpoint.next_sequence == 0
            && self.checkpoint.parent_topology_generation == 0
            && self.checkpoint.state == GraphConstructionState::Staging
        {
            self.revalidate_authority()?;
            self.begin_spool_route()?;
        }
        if self.checkpoint.chunk_route == ChunkRoute::Spool {
            return self.append_spooled(kind, chunk_id, batch, cancelled);
        }
        self.append_staged(kind, chunk_id, batch, None, cancelled)
    }

    fn begin_spool_route(&mut self) -> Result<(), GfError> {
        let directory = self
            .root
            .create_child_directory(OsStr::new(SPOOL_DIR))
            .map_err(storage)?;
        self.checkpoint.chunk_route = ChunkRoute::Spool;
        replace_checkpoint_control(&self.root, &mut self.checkpoint)?;
        self.spool = Some(SpoolState::new(directory));
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn append_spooled(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.state != GraphConstructionState::Staging
            || self.checkpoint.seal_route.is_some()
        {
            return Err(storage("session is not accepting chunks"));
        }
        let spool = self
            .spool
            .as_ref()
            .ok_or_else(|| storage("chunk spool is not open"))?;
        let sequence = spool.len();
        let input_bytes = precheck_chunk(
            &self.checkpoint.budgets,
            sequence,
            spool.saw_edge,
            kind,
            chunk_id,
            batch,
            None,
        )?;
        let input_sha256 = logical_batch_digest(kind, batch)?;
        let schema_sha256 = normalized_schema_digest(batch.schema().as_ref());
        let rows = batch.num_rows() as u64;
        if let Some(&index) = spool.by_id.get(chunk_id) {
            let held = &spool.chunks[index];
            if held.descriptor.kind == kind
                && held.descriptor.rows == rows
                && held.descriptor.input_sha256 == input_sha256
                && held.descriptor.schema_sha256 == schema_sha256
            {
                let on_disk = spool
                    .directory
                    .open_child_file(OsStr::new(&chunk_name(held.descriptor.sequence, kind)))
                    .map_err(storage)?
                    .metadata()
                    .map_err(storage)?
                    .len();
                if on_disk != held.bytes {
                    return Err(storage("spooled chunk differs from its descriptor"));
                }
                if let Some(spool) = self.spool.as_mut() {
                    spool.io.replayed_chunks += 1;
                }
                return Ok(self.spool_receipt(index));
            }
            return Err(storage("conflicting construction chunk replay"));
        }
        let (known_schemas, other_schemas) = match kind {
            ConstructionChunkKind::Node => (&spool.node_schemas, &spool.edge_schemas),
            ConstructionChunkKind::Edge => (&spool.edge_schemas, &spool.node_schemas),
        };
        let schema_groups = known_schemas
            .len()
            .checked_add(other_schemas.len())
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| storage("construction schema-group count overflow"))?;
        if !known_schemas.contains(&schema_sha256)
            && schema_groups > self.checkpoint.budgets.max_schema_groups
        {
            return Err(storage("construction schema-group budget exhausted"));
        }
        let run_records = if kind == ConstructionChunkKind::Edge {
            rows.checked_mul(4)
        } else {
            rows.checked_mul(2)
        }
        .ok_or_else(|| storage("construction run record count overflow"))?;
        if run_records > self.checkpoint.budgets.max_run_records as u64 {
            return Err(storage("construction run window exhausted"));
        }
        validate_chunk_content(kind, batch)?;
        let required = if kind == ConstructionChunkKind::Node {
            2
        } else {
            4
        };
        let descriptor = SpoolDescriptor {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            chunk_id: chunk_id.to_owned(),
            kind,
            rows,
            input_bytes: input_bytes as u64,
            input_sha256,
            schema_sha256,
            property_free: batch.num_columns() == required,
        };
        reject_cancelled(&mut cancelled)?;
        let (artifact, writes, fsyncs) = write_chunk_file(
            &spool.directory,
            &chunk_name(sequence, kind),
            batch,
            &descriptor,
        )?;
        let index = spool.chunks.len();
        let spool = self
            .spool
            .as_mut()
            .ok_or_else(|| storage("chunk spool is not open"))?;
        spool.io.write_bytes += artifact.bytes;
        spool.io.write_operations += writes;
        spool.io.fsync_operations += fsyncs;
        spool.push(SpoolChunk {
            descriptor,
            bytes: artifact.bytes,
            allocated_bytes: artifact.allocated_bytes,
        });
        Ok(self.spool_receipt(index))
    }

    fn spool_receipt(&self, index: usize) -> ConstructionChunkReceipt {
        let chunk = &self
            .spool
            .as_ref()
            .expect("a spool receipt is built from an open spool")
            .chunks[index];
        let descriptor = &chunk.descriptor;
        ConstructionChunkReceipt {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            prior_receipt_sha256: None,
            chunk_id: descriptor.chunk_id.clone(),
            sequence: descriptor.sequence,
            kind: descriptor.kind,
            rows: descriptor.rows,
            input_bytes: descriptor.input_bytes,
            input_sha256: descriptor.input_sha256.clone(),
            schema_sha256: descriptor.schema_sha256.clone(),
            run_records: descriptor.rows
                * if descriptor.kind == ConstructionChunkKind::Edge {
                    4
                } else {
                    2
                },
            accounted_live_bytes: 0,
            parquet: absent_artifact(),
            identities: absent_artifact(),
            endpoints: None,
            details: absent_artifact(),
            spool: Some(SpoolArtifact {
                name: chunk_name(descriptor.sequence, descriptor.kind),
                bytes: chunk.bytes,
                allocated_bytes: chunk.allocated_bytes,
            }),
        }
    }

    /// Rebuild the accepted chunks of a spooled session from its spool.
    ///
    /// A temporary file was never acknowledged, so it is removed. Chunks must
    /// be contiguous from zero and every footer must name this session.
    pub(super) fn restore_spool(&mut self) -> Result<(), GfError> {
        if self.checkpoint.chunk_route != ChunkRoute::Spool
            || self.checkpoint.state == GraphConstructionState::Aborted
        {
            return Ok(());
        }
        let directory = self
            .root
            .create_child_directory(OsStr::new(SPOOL_DIR))
            .map_err(storage)?;
        if self.checkpoint.encoding_inventory_sha256.is_some() {
            // The build is pinned: the chunks are spent. Finish removing them.
            self.spool = Some(SpoolState::new(directory));
            return self.retire_spool();
        }
        let mut found = Vec::new();
        for name in directory.child_names().map_err(storage)? {
            let Some(text) = name.to_str() else {
                return Err(storage("spool entry name is not Unicode"));
            };
            if text.starts_with(".artifact-") {
                unlink_named(&directory, text)?;
            } else if let Some((sequence, kind)) = parse_chunk_name(text) {
                found.push((sequence, kind, text.to_owned()));
            } else {
                return Err(storage("unrecognized entry in the chunk spool"));
            }
        }
        found.sort_unstable_by_key(|(sequence, _, _)| *sequence);
        let mut state = SpoolState::new(directory.try_clone().map_err(storage)?);
        for (position, (sequence, kind, name)) in found.into_iter().enumerate() {
            if sequence != position as u64 {
                return Err(storage("spooled chunks are not contiguous"));
            }
            let (descriptor, bytes) = read_descriptor(&directory, &name)?;
            if descriptor.operation_uuid != self.checkpoint.operation_uuid
                || descriptor.project_identity != self.checkpoint.project_identity
                || descriptor.session_identity != self.checkpoint.session_identity
                || descriptor.sequence != sequence
                || descriptor.kind != kind
                || (kind == ConstructionChunkKind::Node && state.saw_edge)
                || state.by_id.contains_key(&descriptor.chunk_id)
            {
                return Err(storage("spooled chunk differs from its session"));
            }
            let allocated_bytes = {
                let file = directory
                    .open_child_file(OsStr::new(&name))
                    .map_err(storage)?;
                graphforge_filesystem::file_space_usage(&file)
                    .map_err(storage)?
                    .allocated_bytes
            };
            state.push(SpoolChunk {
                descriptor,
                bytes,
                allocated_bytes,
            });
        }
        self.spool = Some(state);
        Ok(())
    }

    /// Delete the spool and everything in it.
    fn retire_spool(&mut self) -> Result<(), GfError> {
        self.spool = None;
        let path = self.root.path().join(SPOOL_DIR);
        if !path.exists() {
            return Ok(());
        }
        if let Some(allocation) = self.root.allocation() {
            allocation.remove_owned_tree(&path)?;
        }
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(storage)?;
        }
        self.root.acknowledge().map_err(storage)
    }

    /// Record how this spooled session builds, unless it already did. Returns
    /// the route in force: a recorded route always wins over `route`, so a
    /// retry never re-decides from live conditions.
    ///
    /// # Errors
    /// Refuses a session that is not spooled.
    pub fn record_seal_route(&mut self, route: SealRoute) -> Result<SealRoute, GfError> {
        if self.checkpoint.chunk_route != ChunkRoute::Spool {
            return Err(storage("only a spooled session records a seal route"));
        }
        if let Some(recorded) = self.checkpoint.seal_route {
            return Ok(recorded);
        }
        self.revalidate_authority()?;
        self.checkpoint.spool_totals = self.spool.as_ref().map(SpoolState::totals);
        self.checkpoint.seal_route = Some(route);
        replace_checkpoint_control(&self.root, &mut self.checkpoint)?;
        Ok(route)
    }

    /// The bulk builder's plan over the spooled chunks, routed for `memory_budget`.
    fn spool_bulk_plan(
        &self,
        memory_budget: Option<u64>,
    ) -> Result<BulkBuildPlan<'static>, GfError> {
        let spool = self
            .spool
            .as_ref()
            .ok_or_else(|| storage("chunk spool is not open"))?;
        let directory = &spool.directory;
        let source = |kind: ConstructionChunkKind| -> Result<Option<BulkSource<'static>>, GfError> {
            let mut tasks: Vec<Vec<SpoolDescriptor>> = Vec::new();
            let (mut rows, mut decoded_bytes, mut property_free) = (0_u64, 0_u64, true);
            let mut open_rows = TASK_ROWS;
            let mut decoded_workspace = 0_u64;
            for chunk in spool.chunks.iter().filter(|c| c.descriptor.kind == kind) {
                let descriptor = &chunk.descriptor;
                rows += descriptor.rows;
                decoded_bytes += descriptor.input_bytes;
                decoded_workspace = decoded_workspace.max(descriptor.input_bytes.saturating_mul(2));
                property_free &= descriptor.property_free;
                let entry = descriptor.clone();
                match tasks.last_mut() {
                    Some(task) if open_rows < TASK_ROWS => {
                        task.push(entry);
                        open_rows += descriptor.rows;
                    }
                    _ => {
                        tasks.push(vec![entry]);
                        open_rows = descriptor.rows;
                    }
                }
            }
            if tasks.is_empty() {
                return Ok(None);
            }
            Ok(Some(BulkSource {
                tasks: tasks.len(),
                rows,
                property_free,
                decoded_bytes,
                reader: Arc::new(SpoolReader {
                    directory: directory.try_clone().map_err(storage)?,
                    tasks,
                    decoded_workspace,
                }),
            }))
        };
        Ok(BulkBuildPlan {
            nodes: source(ConstructionChunkKind::Node)?.into_iter().collect(),
            edges: source(ConstructionChunkKind::Edge)?.into_iter().collect(),
            memory_budget,
        })
    }

    /// Build the generation from the spooled chunks with the bulk builder and
    /// seal it, then delete the spool. A build whose estimate exceeds
    /// `memory_budget` runs on bounded scratch files (ADR 0058). Restartable:
    /// a retry before the inventory is pinned rebuilds from the spool, after it
    /// only reports.
    ///
    /// # Errors
    /// Refuses a session that did not record the bulk route, and returns the
    /// first intake refusal of the spooled rows.
    pub fn prepare_spooled_bulk_encoding(
        &mut self,
        generation: u64,
        memory_budget: u64,
        cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        if self.checkpoint.chunk_route != ChunkRoute::Spool
            || self.checkpoint.seal_route != Some(SealRoute::Bulk)
        {
            return Err(storage("the session did not record the bulk seal route"));
        }
        let plan = if self.checkpoint.encoding_inventory_sha256.is_some() {
            BulkBuildPlan::default()
        } else {
            self.spool_bulk_plan(Some(memory_budget))?
        };
        let encoding = self.prepare_bulk_encoding(generation, &plan, cancelled)?;
        self.retire_spool()?;
        Ok(encoding)
    }
}
