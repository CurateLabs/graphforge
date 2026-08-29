//! Durable, bounded staged graph-import sessions (#738).

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::ipc::writer::FileWriter as ArrowFileWriter;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{BulkInputKind, CancellationToken, GraphConstructionBudgets, GraphForge, OperationId};

const FORMAT_VERSION: u32 = 1;
const SESSION_DIR: &str = "import-sessions";
const MANIFEST: &str = "manifest.json";

/// Explicit resource envelope for one staged import.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ImportSessionLimits {
    /// Maximum rows decoded in one record batch.
    pub batch_rows: usize,
    /// Maximum registered source bytes.
    pub max_source_bytes: u64,
    /// Maximum registered files.
    pub max_files: u64,
    /// Maximum deterministic diagnostics retained.
    pub max_rejected_rows: u64,
    /// Maximum concurrent source readers (currently one; reserved for bounded parallelism).
    pub io_concurrency: usize,
}

impl Default for ImportSessionLimits {
    fn default() -> Self {
        Self {
            batch_rows: GraphConstructionBudgets::default().max_batch_rows,
            max_source_bytes: 1 << 40,
            max_files: 100_000,
            max_rejected_rows: 1_000,
            io_concurrency: 1,
        }
    }
}

impl ImportSessionLimits {
    fn validate(self) -> Result<Self, GfError> {
        if self.batch_rows == 0
            || self.max_source_bytes == 0
            || self.max_files == 0
            || self.max_rejected_rows == 0
            || self.io_concurrency == 0
            || self.io_concurrency > 32
        {
            return Err(validation(
                "import resource limits must be positive and concurrency <= 32",
            ));
        }
        Ok(self)
    }
}

/// Durable lifecycle phase.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ImportPhase {
    /// Accepting sources.
    Open,
    /// Sources and staged rows passed validation.
    Validated,
    /// Atomic project publication completed.
    Committed,
    /// Caller aborted the session.
    Aborted,
    /// Staging was quarantined after deterministic cleanup failure.
    Quarantined,
}

/// Registered source encoding.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ImportSourceKind {
    /// Arrow IPC file containing nodes.
    ArrowNodes,
    /// Arrow IPC file containing edges.
    ArrowEdges,
    /// Parquet file containing nodes.
    ParquetNodes,
    /// Parquet file containing edges.
    ParquetEdges,
}

impl ImportSourceKind {
    const fn input_kind(self) -> BulkInputKind {
        match self {
            Self::ArrowNodes | Self::ParquetNodes => BulkInputKind::Node,
            Self::ArrowEdges | Self::ParquetEdges => BulkInputKind::Edge,
        }
    }
}

/// Content-free durable progress.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ImportProgress {
    /// Rows accepted into durable staging.
    pub rows_accepted: u64,
    /// Rows rejected by deterministic validation.
    pub rows_rejected: u64,
    /// Bytes durably registered.
    pub bytes_accepted: u64,
    /// Files durably registered.
    pub files_accepted: u64,
    /// Files not yet staged.
    pub files_pending: u64,
    /// Monotonic work elapsed, accumulated at checkpoints.
    pub elapsed_millis: u64,
    /// Peak rows held in a decoded batch.
    pub peak_batch_rows: u64,
    /// Configured source-reader concurrency bound.
    pub io_concurrency_limit: u64,
    /// Durable, content-free evidence from the ordinary graph-construction path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub construction: Option<ImportConstructionEvidence>,
}

/// Sanitized durable construction evidence for an ordinary staged import.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ImportConstructionEvidence {
    /// Configured authoritative construction chunk budget.
    pub configured_batch_rows: u64,
    /// Number of durably accepted construction chunks.
    pub accepted_chunks: u64,
    /// Whether the sole generation publication was committed.
    pub publication_committed: bool,
    /// Exact application-I/O attribution across the closed construction phases.
    pub application_io: graphforge_storage::ConstructionPhaseAttribution,
    /// Versioned named publication work, derived from the phase counters above.
    #[serde(default)]
    pub publication_work: PublicationWorkComponents,
    /// Exact accepted input rows.
    pub input_rows: u64,
    /// Exact non-replay input batches.
    pub input_batches: u64,
    /// Immutable construction artifacts accepted from authenticated receipts.
    pub immutable_artifacts: u64,
    /// Application payload bytes submitted by construction artifact writers.
    pub write_bytes: u64,
    /// Application write submissions by construction artifact writers.
    pub write_operations: u64,
    /// File and directory durability barriers completed by construction.
    pub fsync_operations: u64,
    /// Largest retained Arrow row window.
    pub peak_batch_rows: u64,
    /// Largest retained Arrow byte window.
    pub peak_batch_bytes: u64,
    /// Exact transient allocation high-water retained across resume.
    pub transient_peak_allocated_bytes: u64,
}

/// Closed semantic publication-work contract for ordinary construction evidence.
///
/// `semantic_total_operations` is exactly the sum of read calls, write calls,
/// and fsync calls across these five named phase rows. Bytes and call components
/// remain intact so downstream controllers never need to infer work from time or
/// filesystem scans.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicationWorkComponents {
    /// Versioned semantic contract.
    pub contract: String,
    /// Canonical encoding, write, and post-write authentication.
    pub encode_write_postwrite_authentication: graphforge_storage::PhaseIoTotals,
    /// Publication control preauthentication.
    pub publication_preauthentication: graphforge_storage::PhaseIoTotals,
    /// Content-addressed installation reads and writes.
    pub cas_install_read_write: graphforge_storage::PhaseIoTotals,
    /// Workspace hydration and verification.
    pub hydration_verification: graphforge_storage::PhaseIoTotals,
    /// File and directory durability barriers.
    pub fsync_synchronization: graphforge_storage::PhaseIoTotals,
    /// Checked sum of read calls, write calls, and fsync calls in the named rows.
    pub semantic_total_operations: u64,
}

impl PublicationWorkComponents {
    fn checked_operation_total(
        phases: [&graphforge_storage::PhaseIoTotals; 5],
    ) -> Result<u64, GfError> {
        let mut total = 0_u64;
        for phase in phases {
            total = total
                .checked_add(phase.read_calls)
                .and_then(|value| value.checked_add(phase.write_calls))
                .and_then(|value| value.checked_add(phase.fsync_calls))
                .ok_or_else(|| validation("publication work operation total overflowed"))?;
        }
        Ok(total)
    }

    fn from_application_io(
        application_io: &graphforge_storage::ConstructionPhaseAttribution,
    ) -> Result<Self, GfError> {
        use graphforge_storage::StorageIoPhase;
        application_io.validate_for_qualification()?;
        let phase = |name| {
            application_io
                .phases
                .get(&name)
                .cloned()
                .ok_or_else(|| validation("publication work phase is absent"))
        };
        let encode_write_postwrite_authentication =
            phase(StorageIoPhase::EncodeWritePostwriteAuthentication)?;
        let publication_preauthentication = phase(StorageIoPhase::PublicationPreauthentication)?;
        let cas_install_read_write = phase(StorageIoPhase::CasInstallReadWrite)?;
        let hydration_verification = phase(StorageIoPhase::HydrationVerification)?;
        let fsync_synchronization = phase(StorageIoPhase::FsyncSynchronization)?;
        let semantic_total_operations = Self::checked_operation_total([
            &encode_write_postwrite_authentication,
            &publication_preauthentication,
            &cas_install_read_write,
            &hydration_verification,
            &fsync_synchronization,
        ])?;
        Ok(Self {
            contract: "graphforge-publication-work/1".to_owned(),
            encode_write_postwrite_authentication,
            publication_preauthentication,
            cas_install_read_write,
            hydration_verification,
            fsync_synchronization,
            semantic_total_operations,
        })
    }

    /// Verify the version, arithmetic, and exact phase projection.
    fn validate_against(
        &self,
        application_io: &graphforge_storage::ConstructionPhaseAttribution,
    ) -> Result<(), GfError> {
        let expected = Self::from_application_io(application_io)?;
        if self != &expected {
            return Err(validation(
                "publication work components do not reconcile with construction phases",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceRecord {
    sequence: u64,
    kind: ImportSourceKind,
    name: String,
    bytes: u64,
    rows: u64,
    staged: bool,
    #[serde(default)]
    batches_staged: u64,
    #[serde(default)]
    inflight_batch: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionManifest {
    format_version: u32,
    session_uuid: Uuid,
    operation_uuid: Uuid,
    base_generation_uuid: Uuid,
    phase: ImportPhase,
    limits: ImportSessionLimits,
    progress: ImportProgress,
    sources: Vec<SourceRecord>,
    #[serde(default)]
    construction_session_uuid: Option<Uuid>,
    #[serde(default)]
    updated_unix_millis: u64,
}

/// Owned handle for a durable staged import. The handle contains no live rows.
pub struct GraphImportSession {
    root: PathBuf,
    manifest: SessionManifest,
    observed: Instant,
}

impl GraphForge {
    /// Begin a durable import pinned to the facade's current project generation.
    pub fn begin_import_session(
        &self,
        operation_uuid: OperationId,
        limits: ImportSessionLimits,
    ) -> Result<GraphImportSession, GfError> {
        let limits = limits.validate()?;
        let session_uuid = operation_uuid.0;
        let root = import_root(self, session_uuid)?;
        if root.exists() {
            return Err(validation(
                "import session already exists; resume it instead",
            ));
        }
        fs::create_dir_all(root.join("sources")).map_err(storage)?;
        let manifest = SessionManifest {
            format_version: FORMAT_VERSION,
            session_uuid,
            operation_uuid: operation_uuid.0,
            base_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            phase: ImportPhase::Open,
            limits,
            progress: ImportProgress {
                io_concurrency_limit: u64::try_from(limits.io_concurrency).unwrap_or(u64::MAX),
                ..ImportProgress::default()
            },
            sources: Vec::new(),
            construction_session_uuid: None,
            updated_unix_millis: unix_millis()?,
        };
        write_manifest(&root, &manifest)?;
        Ok(GraphImportSession {
            root,
            manifest,
            observed: Instant::now(),
        })
    }

    /// Resume one durable, non-terminal session after process interruption.
    pub fn resume_import_session(&self, session_uuid: Uuid) -> Result<GraphImportSession, GfError> {
        let root = import_root(self, session_uuid)?;
        let manifest = read_manifest(&root)?;
        if manifest.format_version != FORMAT_VERSION || manifest.session_uuid != session_uuid {
            return Err(validation("incompatible or mismatched import manifest"));
        }
        if matches!(
            manifest.phase,
            ImportPhase::Committed | ImportPhase::Aborted
        ) {
            return Err(validation("terminal import sessions cannot be resumed"));
        }
        Ok(GraphImportSession {
            root,
            manifest,
            observed: Instant::now(),
        })
    }

    /// Reopen the durable, content-free status of any import session, including terminal ones.
    pub fn import_session_status(
        &self,
        session_uuid: Uuid,
    ) -> Result<(ImportPhase, ImportProgress), GfError> {
        let root = import_root(self, session_uuid)?;
        let manifest = read_manifest(&root)?;
        if manifest.format_version != FORMAT_VERSION || manifest.session_uuid != session_uuid {
            return Err(validation("incompatible or mismatched import manifest"));
        }
        Ok((manifest.phase, manifest.progress))
    }

    /// Abort and remove durable staging for non-terminal sessions older than `max_age`.
    pub fn cleanup_stale_import_sessions(&self, max_age: Duration) -> Result<u64, GfError> {
        let sessions = self.resolved_generation.container_root().join(SESSION_DIR);
        let now = unix_millis()?;
        let threshold = u64::try_from(max_age.as_millis()).unwrap_or(u64::MAX);
        let mut cleaned = 0_u64;
        let entries = match fs::read_dir(&sessions) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(storage(error)),
        };
        for entry in entries {
            let entry = entry.map_err(storage)?;
            if entry.file_type().map_err(storage)?.is_symlink()
                || !entry.file_type().map_err(storage)?.is_dir()
            {
                continue;
            }
            let root = entry.path();
            let manifest = read_manifest(&root)?;
            if matches!(
                manifest.phase,
                ImportPhase::Committed | ImportPhase::Aborted
            ) || now.saturating_sub(manifest.updated_unix_millis) < threshold
            {
                continue;
            }
            GraphImportSession {
                root,
                manifest,
                observed: Instant::now(),
            }
            .abort(self)?;
            cleaned = cleaned.saturating_add(1);
        }
        Ok(cleaned)
    }
}

impl GraphImportSession {
    /// Durable identifier used for resume.
    #[must_use]
    pub const fn session_uuid(&self) -> Uuid {
        self.manifest.session_uuid
    }

    /// Current durable phase and counters.
    #[must_use]
    pub fn status(&self) -> (ImportPhase, ImportProgress) {
        (self.manifest.phase, self.manifest.progress.clone())
    }

    /// Append one Arrow partition by durably encoding it as IPC without retaining rows.
    pub fn append_arrow(
        &mut self,
        kind: BulkInputKind,
        batches: &[RecordBatch],
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        if batches.is_empty() {
            return Ok(());
        }
        let source_kind = match kind {
            BulkInputKind::Node => ImportSourceKind::ArrowNodes,
            BulkInputKind::Edge => ImportSourceKind::ArrowEdges,
        };
        let sequence = self.next_sequence()?;
        let name = format!("{sequence:020}.arrow");
        let destination = self.root.join("sources").join(&name);
        let temporary = self.root.join("sources").join(format!(".{name}.tmp"));
        let file = File::create(&temporary).map_err(storage)?;
        let mut writer = ArrowFileWriter::try_new(BufWriter::new(file), &batches[0].schema())
            .map_err(storage)?;
        let mut rows = 0_u64;
        for batch in batches {
            if batch.num_rows() > self.manifest.limits.batch_rows {
                return Err(limit("Arrow batch exceeds import batch_rows"));
            }
            writer.write(batch).map_err(storage)?;
            rows = rows.saturating_add(batch.num_rows() as u64);
            self.manifest.progress.peak_batch_rows = self
                .manifest
                .progress
                .peak_batch_rows
                .max(batch.num_rows() as u64);
        }
        writer.finish().map_err(storage)?;
        fs::rename(&temporary, &destination).map_err(storage)?;
        sync_file(&destination)?;
        let result = self.register_record(
            source_kind,
            name,
            destination.metadata().map_err(storage)?.len(),
            rows,
        );
        if result.is_err() {
            let _ = fs::remove_file(destination);
        }
        result
    }

    /// Register a local Parquet source by copying it into durable session ownership.
    pub fn register_parquet(&mut self, kind: BulkInputKind, source: &Path) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_unsafe_path(source)?;
        let metadata = fs::symlink_metadata(source).map_err(storage)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(validation(
                "Parquet source must be a regular non-symlink file",
            ));
        }
        if self
            .manifest
            .progress
            .bytes_accepted
            .saturating_add(metadata.len())
            > self.manifest.limits.max_source_bytes
        {
            return Err(limit("import max_source_bytes exceeded"));
        }
        let sequence = self.next_sequence()?;
        let name = format!("{sequence:020}.parquet");
        let destination = self.root.join("sources").join(&name);
        fs::copy(source, &destination).map_err(storage)?;
        sync_file(&destination)?;
        self.register_record(
            match kind {
                BulkInputKind::Node => ImportSourceKind::ParquetNodes,
                BulkInputKind::Edge => ImportSourceKind::ParquetEdges,
            },
            name,
            metadata.len(),
            0,
        )
    }

    /// Persist counters and source ordering without publishing graph state.
    pub fn checkpoint(&mut self) -> Result<ImportProgress, GfError> {
        self.manifest.progress.elapsed_millis =
            self.manifest.progress.elapsed_millis.saturating_add(
                u64::try_from(self.observed.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
        self.observed = Instant::now();
        self.manifest.updated_unix_millis = unix_millis()?;
        write_manifest(&self.root, &self.manifest)?;
        Ok(self.manifest.progress.clone())
    }

    /// Validate source readability and exact public Arrow schemas using bounded batches.
    pub fn validate(&mut self, graph: &GraphForge) -> Result<ImportProgress, GfError> {
        self.validate_with_cancellation(graph, None)
    }

    /// Validate and durably stage every source with cooperative per-batch cancellation.
    pub fn validate_with_cancellation(
        &mut self,
        graph: &GraphForge,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ImportProgress, GfError> {
        self.ensure_open()?;
        self.ensure_base(graph)?;
        let mut construction = self.open_construction(graph)?;
        let session_root = self.root.clone();
        let batch_rows = self.manifest.limits.batch_rows;
        for input_kind in [BulkInputKind::Node, BulkInputKind::Edge] {
            for source_index in 0..self.manifest.sources.len() {
                let source = self.manifest.sources[source_index].clone();
                if source.kind.input_kind() != input_kind || source.staged {
                    continue;
                }
                let mut batch_index = 0_u64;
                for_each_source_batch(&session_root, &source, batch_rows, |batch| {
                    if batch_index < source.batches_staged {
                        batch_index += 1;
                        return Ok(());
                    }
                    if cancellation.is_some_and(CancellationToken::is_cancelled) {
                        return Err(cancelled());
                    }
                    let operation = import_batch_operation(
                        self.manifest.operation_uuid,
                        source.sequence,
                        batch_index,
                    );
                    let batch = match input_kind {
                        BulkInputKind::Node => graph.normalize_import_node_chunk(operation, &batch),
                        BulkInputKind::Edge => graph.normalize_import_edge_chunk(operation, &batch),
                    }
                    .map_err(|error| validation(error.to_string()))?;
                    if batch.num_rows() == 0 {
                        batch_index += 1;
                        self.manifest.sources[source_index].batches_staged = batch_index;
                        self.manifest.sources[source_index].inflight_batch = None;
                        write_manifest(&self.root, &self.manifest)?;
                        return Ok(());
                    }
                    let recovering = source.inflight_batch == Some(batch_index);
                    if !recovering {
                        self.manifest.sources[source_index].inflight_batch = Some(batch_index);
                        write_manifest(&self.root, &self.manifest)?;
                    }
                    let chunk_id = format!("import-{:020}-{:020}", source.sequence, batch_index);
                    let staged = match (input_kind, cancellation) {
                        (BulkInputKind::Node, Some(token)) => {
                            construction.append_nodes_with_cancellation(&chunk_id, &batch, token)
                        }
                        (BulkInputKind::Node, None) => construction.append_nodes(&chunk_id, &batch),
                        (BulkInputKind::Edge, Some(token)) => {
                            construction.append_edges_with_cancellation(&chunk_id, &batch, token)
                        }
                        (BulkInputKind::Edge, None) => construction.append_edges(&chunk_id, &batch),
                    };
                    if let Err(error) = staged {
                        self.manifest.sources[source_index].inflight_batch = None;
                        let remaining = self
                            .manifest
                            .limits
                            .max_rejected_rows
                            .saturating_sub(self.manifest.progress.rows_rejected);
                        self.manifest.progress.rows_rejected = self
                            .manifest
                            .progress
                            .rows_rejected
                            .saturating_add((batch.num_rows() as u64).min(remaining));
                        write_manifest(&self.root, &self.manifest)?;
                        return Err(error);
                    }
                    batch_index += 1;
                    self.manifest.sources[source_index].batches_staged = batch_index;
                    self.manifest.sources[source_index].inflight_batch = None;
                    self.manifest.progress.rows_accepted = self
                        .manifest
                        .progress
                        .rows_accepted
                        .saturating_add(batch.num_rows() as u64);
                    self.manifest.progress.peak_batch_rows = self
                        .manifest
                        .progress
                        .peak_batch_rows
                        .max(batch.num_rows() as u64);
                    self.update_construction_progress(&construction.progress())?;
                    write_manifest(&self.root, &self.manifest)?;
                    Ok(())
                })?;
                self.manifest.sources[source_index].staged = true;
                self.manifest.progress.files_pending =
                    self.manifest.progress.files_pending.saturating_sub(1);
                write_manifest(&self.root, &self.manifest)?;
            }
        }
        construction.validate_and_seal(cancellation)?;
        self.update_construction_progress(&construction.progress())?;
        self.manifest.phase = ImportPhase::Validated;
        self.checkpoint()
    }

    /// Abort without changing CURRENT; removes staged sources or quarantines on cleanup failure.
    pub fn abort(mut self, graph: &GraphForge) -> Result<ImportProgress, GfError> {
        if self.manifest.phase == ImportPhase::Committed {
            return Err(validation("committed import cannot be aborted"));
        }
        let cleanup = (|| {
            if let Some(session_uuid) = self.manifest.construction_session_uuid {
                graph
                    .resume_graph_construction(session_uuid, self.construction_budgets())?
                    .discard()?;
                self.manifest.construction_session_uuid = None;
            }
            let sources = self.root.join("sources");
            if sources.exists() {
                fs::remove_dir_all(sources).map_err(storage)?;
            }
            Ok::<(), GfError>(())
        })();
        match cleanup {
            Ok(()) => {
                self.manifest.phase = ImportPhase::Aborted;
                self.checkpoint()
            }
            Err(error) => {
                self.manifest.phase = ImportPhase::Quarantined;
                let _ = write_manifest(&self.root, &self.manifest);
                Err(error)
            }
        }
    }

    /// Publish the fully staged graph, catalog, and membership indexes as one generation.
    pub fn commit(
        &mut self,
        graph: &GraphForge,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Uuid, GfError> {
        if self.manifest.phase != ImportPhase::Validated
            || self.manifest.progress.files_pending != 0
        {
            return Err(validation("import must be fully validated before commit"));
        }
        self.ensure_base(graph)?;
        let mut construction = self.open_construction(graph)?;
        let publication = match cancellation {
            Some(token) => construction.seal_and_publish_with_cancellation(token)?,
            None => construction.seal_and_publish()?,
        };
        self.update_construction_progress(&construction.progress())?;
        self.manifest.phase = ImportPhase::Committed;
        self.checkpoint()?;
        Ok(publication.generation_uuid)
    }

    fn ensure_open(&self) -> Result<(), GfError> {
        if matches!(
            self.manifest.phase,
            ImportPhase::Open | ImportPhase::Validated
        ) {
            Ok(())
        } else {
            Err(validation("import session is terminal"))
        }
    }

    fn ensure_base(&self, graph: &GraphForge) -> Result<(), GfError> {
        let current = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if current != self.manifest.base_generation_uuid {
            return Err(validation("project generation changed since import began"));
        }
        Ok(())
    }

    fn construction_budgets(&self) -> GraphConstructionBudgets {
        let mut budgets = GraphConstructionBudgets::default();
        budgets.max_batch_rows = self.manifest.limits.batch_rows;
        budgets.max_run_records = budgets
            .max_run_records
            .max(self.manifest.limits.batch_rows.saturating_mul(4));
        budgets
    }

    fn open_construction<'a>(
        &mut self,
        graph: &'a GraphForge,
    ) -> Result<crate::GraphConstructionSession<'a>, GfError> {
        let budgets = self.construction_budgets();
        if let Some(session_uuid) = self.manifest.construction_session_uuid {
            return graph.resume_graph_construction(session_uuid, budgets);
        }
        let session = graph.begin_graph_construction(budgets)?;
        self.manifest.construction_session_uuid = Some(session.session_uuid());
        write_manifest(&self.root, &self.manifest)?;
        Ok(session)
    }

    fn update_construction_progress(
        &mut self,
        progress: &crate::GraphConstructionProgress,
    ) -> Result<(), GfError> {
        let application_io =
            graphforge_storage::ConstructionPhaseAttribution::from_construction(&progress.evidence);
        application_io.validate_for_qualification()?;
        let publication_work = PublicationWorkComponents::from_application_io(&application_io)?;
        self.manifest.progress.construction = Some(ImportConstructionEvidence {
            configured_batch_rows: u64::try_from(self.manifest.limits.batch_rows)
                .unwrap_or(u64::MAX),
            accepted_chunks: progress.accepted_chunks,
            publication_committed: progress.publication_committed,
            application_io,
            publication_work,
            input_rows: progress.evidence.input_rows,
            input_batches: progress.evidence.input_batches,
            immutable_artifacts: progress.evidence.immutable_artifacts,
            write_bytes: progress.evidence.write_bytes,
            write_operations: progress.evidence.write_operations,
            fsync_operations: progress.evidence.fsync_operations,
            peak_batch_rows: progress.evidence.peak_batch_rows,
            peak_batch_bytes: progress.evidence.peak_batch_bytes,
            transient_peak_allocated_bytes: progress
                .evidence
                .storage_transient_peak_total_allocated_bytes,
        });
        Ok(())
    }

    fn next_sequence(&self) -> Result<u64, GfError> {
        if self.manifest.sources.len() as u64 >= self.manifest.limits.max_files {
            return Err(limit("import max_files exceeded"));
        }
        Ok(self.manifest.sources.len() as u64)
    }

    fn register_record(
        &mut self,
        kind: ImportSourceKind,
        name: String,
        bytes: u64,
        rows: u64,
    ) -> Result<(), GfError> {
        let total = self.manifest.progress.bytes_accepted.saturating_add(bytes);
        if total > self.manifest.limits.max_source_bytes {
            return Err(limit("import max_source_bytes exceeded"));
        }
        self.manifest.sources.push(SourceRecord {
            sequence: self.manifest.sources.len() as u64,
            kind,
            name,
            bytes,
            rows,
            staged: false,
            batches_staged: 0,
            inflight_batch: None,
        });
        self.manifest.phase = ImportPhase::Open;
        self.manifest.progress.bytes_accepted = total;
        self.manifest.progress.files_accepted += 1;
        self.manifest.progress.files_pending += 1;
        self.checkpoint().map(|_| ())
    }
}

fn unix_millis() -> Result<u64, GfError> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(storage)?
            .as_millis(),
    )
    .unwrap_or(u64::MAX))
}

fn for_each_source_batch(
    root: &Path,
    source: &SourceRecord,
    batch_rows: usize,
    mut consume: impl FnMut(RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let path = root.join("sources").join(&source.name);
    match source.kind {
        ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
            for batch in
                ArrowFileReader::try_new(BufReader::new(File::open(path).map_err(storage)?), None)
                    .map_err(storage)?
            {
                consume(batch.map_err(storage)?)?;
            }
        }
        ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(path).map_err(storage)?)
                    .map_err(storage)?
                    .with_batch_size(batch_rows)
                    .build()
                    .map_err(storage)?;
            for batch in reader {
                consume(canonicalize_parquet_batch(
                    source.kind.input_kind(),
                    &batch.map_err(storage)?,
                )?)?;
            }
        }
    }
    Ok(())
}

fn canonicalize_parquet_batch(
    kind: BulkInputKind,
    batch: &RecordBatch,
) -> Result<RecordBatch, GfError> {
    let required = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    if batch.num_columns() < required {
        return Err(validation("Parquet import schema lacks required columns"));
    }
    let properties = batch.schema().fields()[required..]
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let schema = match kind {
        BulkInputKind::Node => crate::bulk_node_input_schema(properties),
        BulkInputKind::Edge => crate::bulk_edge_input_schema(properties),
    }
    .map_err(|error| validation(error.to_string()))?;
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(storage)
}

fn import_root(graph: &GraphForge, session_uuid: Uuid) -> Result<PathBuf, GfError> {
    let container = graph.resolved_generation.container_root();
    let sessions = container.join(SESSION_DIR);
    fs::create_dir_all(&sessions).map_err(storage)?;
    Ok(sessions.join(session_uuid.hyphenated().to_string()))
}

fn write_manifest(root: &Path, manifest: &SessionManifest) -> Result<(), GfError> {
    let temporary = root.join("manifest.tmp");
    let mut file = BufWriter::new(File::create(&temporary).map_err(storage)?);
    serde_json::to_writer(&mut file, manifest).map_err(storage)?;
    file.flush().map_err(storage)?;
    file.get_ref().sync_all().map_err(storage)?;
    drop(file);
    fs::rename(temporary, root.join(MANIFEST)).map_err(storage)
}

fn read_manifest(root: &Path) -> Result<SessionManifest, GfError> {
    let mut manifest: SessionManifest = serde_json::from_reader(BufReader::new(
        File::open(root.join(MANIFEST)).map_err(storage)?,
    ))
    .map_err(storage)?;
    if let Some(construction) = manifest.progress.construction.as_mut()
        && construction.publication_work.contract.is_empty()
    {
        construction.publication_work =
            PublicationWorkComponents::from_application_io(&construction.application_io)?;
    }
    if let Some(construction) = manifest.progress.construction.as_ref() {
        construction
            .publication_work
            .validate_against(&construction.application_io)?;
    }
    Ok(manifest)
}

fn reject_unsafe_path(path: &Path) -> Result<(), GfError> {
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(validation("source path traversal is forbidden"));
    }
    Ok(())
}

fn import_batch_operation(base: Uuid, source: u64, batch: u64) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"graphforge.import.batch.v1\0");
    digest.update(base.as_bytes());
    digest.update(source.to_be_bytes());
    digest.update(batch.to_be_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&base.as_bytes()[..6]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    OperationId(Uuid::from_bytes(bytes))
}

fn sync_file(path: &Path) -> Result<(), GfError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(storage)
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn limit(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

fn cancelled() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::Cancelled,
        message: "graph import cancelled at a durable batch checkpoint".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use arrow::datatypes::DataType;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::{bulk_edge_input_schema, bulk_node_input_schema};

    fn uuid_array(values: &[Uuid]) -> Arc<FixedSizeBinaryArray> {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                values.iter().map(|value| value.as_bytes().as_slice()),
            )
            .unwrap(),
        )
    }

    fn nodes(values: &[Uuid]) -> RecordBatch {
        RecordBatch::try_new(
            bulk_node_input_schema(Vec::new()).unwrap(),
            vec![
                uuid_array(values),
                Arc::new(StringArray::from(vec!["Person"; values.len()])),
            ],
        )
        .unwrap()
    }

    fn edges(edge: Uuid, source: Uuid, target: Uuid) -> RecordBatch {
        RecordBatch::try_new(
            bulk_edge_input_schema(Vec::new()).unwrap(),
            vec![
                uuid_array(&[edge]),
                Arc::new(StringArray::from(vec!["KNOWS"])),
                uuid_array(&[source]),
                uuid_array(&[target]),
            ],
        )
        .unwrap()
    }

    fn fixture() -> (tempfile::TempDir, PathBuf, GraphForge) {
        let directory = tempfile::tempdir().unwrap();
        let project = directory.path().join("project");
        fs::create_dir(&project).unwrap();
        let graph = GraphForge::new(project.to_str()).unwrap();
        (directory, project, graph)
    }

    fn construction_root(graph: &GraphForge, session_uuid: Uuid) -> PathBuf {
        graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(session_uuid.simple().to_string())
    }

    #[test]
    fn arrow_session_resumes_stages_and_publishes_one_generation() {
        let (_directory, project, graph) = fixture();
        let node_ids = [Uuid::now_v7(), Uuid::now_v7()];
        let edge_id = Uuid::now_v7();
        let operation = OperationId(Uuid::now_v7());
        let before = *graph.current_generation_uuid.lock().unwrap();
        let mut session = graph
            .begin_import_session(operation, ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&node_ids)])
            .unwrap();
        session.checkpoint().unwrap();
        assert_eq!(session.status().1.files_pending, 1);
        let session_uuid = session.session_uuid();
        drop(session);

        let mut resumed = graph.resume_import_session(session_uuid).unwrap();
        resumed
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(edge_id, node_ids[0], node_ids[1])],
            )
            .unwrap();
        let progress = resumed.validate(&graph).unwrap();
        assert_eq!((progress.rows_accepted, progress.files_pending), (3, 0));
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
        let committed = resumed.commit(&graph, None).unwrap();
        assert_ne!(committed, before);

        drop(graph);
        let reopened = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), 2);
        assert_eq!(
            reopened
                .execute("MATCH ()-[r:KNOWS]->() RETURN r")
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn parquet_abort_and_missing_endpoint_preserve_prior_generation() {
        let (_directory, project, graph) = fixture();
        let source_dir = tempfile::tempdir().unwrap();
        let parquet = source_dir.path().join("nodes.parquet");
        let batch = nodes(&[Uuid::now_v7()]);
        let mut writer =
            ArrowWriter::try_new(File::create(&parquet).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let before = *graph.current_generation_uuid.lock().unwrap();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .register_parquet(BulkInputKind::Node, &parquet)
            .unwrap();
        session.validate(&graph).unwrap();
        let construction_uuid = session.manifest.construction_session_uuid.unwrap();
        assert!(construction_root(&graph, construction_uuid).exists());
        let progress = session.abort(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 1);
        assert!(!construction_root(&graph, construction_uuid).exists());
        assert!(
            graph
                .resume_graph_construction(construction_uuid, GraphConstructionBudgets::default())
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
        drop(graph);
        GraphForge::new(project.to_str()).unwrap();
    }

    #[test]
    fn cancellation_and_missing_endpoint_are_durable_fail_closed() {
        let (_directory, _project, graph) = fixture();
        let before = *graph.current_generation_uuid.lock().unwrap();
        let mut cancelled_session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        cancelled_session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            cancelled_session
                .validate_with_cancellation(&graph, Some(&cancellation))
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);

        let mut invalid = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        invalid
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7())],
            )
            .unwrap();
        assert!(invalid.validate(&graph).is_err());
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
    }

    #[test]
    fn corrupt_traversal_duplicate_and_resource_inputs_fail_closed() {
        let (_directory, _project, graph) = fixture();
        let before = *graph.current_generation_uuid.lock().unwrap();
        let mut limited = graph
            .begin_import_session(
                OperationId(Uuid::now_v7()),
                ImportSessionLimits {
                    max_source_bytes: 1,
                    ..ImportSessionLimits::default()
                },
            )
            .unwrap();
        assert!(
            limited
                .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
                .is_err()
        );

        let source_dir = tempfile::tempdir().unwrap();
        let corrupt = source_dir.path().join("corrupt.parquet");
        fs::write(&corrupt, b"not parquet").unwrap();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        assert!(
            session
                .register_parquet(BulkInputKind::Node, Path::new("../escape.parquet"))
                .is_err()
        );
        session
            .register_parquet(BulkInputKind::Node, &corrupt)
            .unwrap();
        assert!(session.validate(&graph).is_err());

        let duplicate = Uuid::now_v7();
        let mut duplicates = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        duplicates
            .append_arrow(BulkInputKind::Node, &[nodes(&[duplicate, duplicate])])
            .unwrap();
        assert!(duplicates.validate(&graph).is_err());
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
    }

    #[test]
    fn interrupted_batch_replays_and_stale_cleanup_removes_private_artifacts() {
        let (_directory, _project, graph) = fixture();
        let operation = OperationId(Uuid::now_v7());
        let batch = nodes(&[Uuid::now_v7()]);
        let mut session = graph
            .begin_import_session(operation, ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, std::slice::from_ref(&batch))
            .unwrap();
        let batch_operation = import_batch_operation(operation.0, 0, 0);
        let normalized = graph
            .normalize_import_node_chunk(batch_operation, &batch)
            .unwrap();
        let mut construction = session.open_construction(&graph).unwrap();
        construction
            .append_nodes(
                "import-00000000000000000000-00000000000000000000",
                &normalized,
            )
            .unwrap();
        drop(construction);
        session.manifest.sources[0].inflight_batch = Some(0);
        write_manifest(&session.root, &session.manifest).unwrap();
        let session_uuid = session.session_uuid();
        drop(session);

        let mut resumed = graph.resume_import_session(session_uuid).unwrap();
        let progress = resumed.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 1);
        let construction = progress.construction.unwrap();
        assert_eq!(construction.accepted_chunks, 1);
        assert_eq!(construction.input_batches, 1);
        let construction_uuid = resumed.manifest.construction_session_uuid.unwrap();
        drop(resumed);

        let mut manifest = read_manifest(&import_root(&graph, session_uuid).unwrap()).unwrap();
        manifest.updated_unix_millis = 0;
        write_manifest(&import_root(&graph, session_uuid).unwrap(), &manifest).unwrap();
        assert_eq!(
            graph
                .cleanup_stale_import_sessions(Duration::from_secs(1))
                .unwrap(),
            1
        );
        let root = import_root(&graph, session_uuid).unwrap();
        assert!(!root.join("sources").exists());
        assert!(!construction_root(&graph, construction_uuid).exists());
        assert_eq!(read_manifest(&root).unwrap().phase, ImportPhase::Aborted);
    }

    #[test]
    fn legacy_manifest_without_publication_work_backfills_from_application_io() {
        let (_directory, _project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap();
        session.validate(&graph).unwrap();

        let root = session.root.clone();
        let mut legacy = serde_json::to_value(&session.manifest).unwrap();
        let construction = legacy["progress"]["construction"].as_object_mut().unwrap();
        let application_io: graphforge_storage::ConstructionPhaseAttribution =
            serde_json::from_value(construction["application_io"].clone()).unwrap();
        assert!(construction.remove("publication_work").is_some());
        fs::write(root.join(MANIFEST), serde_json::to_vec(&legacy).unwrap()).unwrap();

        let restored = read_manifest(&root).unwrap();
        let evidence = restored.progress.construction.unwrap();
        assert_eq!(
            evidence.publication_work.contract,
            "graphforge-publication-work/1"
        );
        evidence
            .publication_work
            .validate_against(&application_io)
            .unwrap();
    }

    #[test]
    fn zero_row_node_and_edge_sources_are_canonical_and_publishable() {
        let (_directory, project, graph) = fixture();
        let empty_nodes = RecordBatch::new_empty(bulk_node_input_schema(Vec::new()).unwrap());
        let empty_edges = RecordBatch::new_empty(bulk_edge_input_schema(Vec::new()).unwrap());
        let normalized_nodes = graph
            .normalize_import_node_chunk(OperationId(Uuid::now_v7()), &empty_nodes)
            .unwrap();
        let normalized_edges = graph
            .normalize_import_edge_chunk(OperationId(Uuid::now_v7()), &empty_edges)
            .unwrap();
        assert_eq!(normalized_nodes.num_rows(), 0);
        assert_eq!(normalized_edges.num_rows(), 0);
        assert_eq!(
            normalized_nodes.column(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            normalized_edges.column(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );

        let retained_node = Uuid::now_v7();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[empty_nodes, nodes(&[retained_node])])
            .unwrap();
        session
            .append_arrow(BulkInputKind::Edge, &[empty_edges])
            .unwrap();
        let progress = session.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 1);
        assert_eq!(progress.files_pending, 0);
        assert_eq!(progress.construction.as_ref().unwrap().accepted_chunks, 1);
        let generation = session.commit(&graph, None).unwrap();

        drop(graph);
        let reopened = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(
            *reopened.current_generation_uuid.lock().unwrap(),
            generation
        );
        assert_eq!(reopened.node_count("Person").unwrap(), 1);
    }

    #[test]
    fn commit_rechecks_the_import_base_generation() {
        let (_directory, _project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap();
        session.validate(&graph).unwrap();
        graph.add_node("Other", &HashMap::new()).unwrap();
        let independent = *graph.current_generation_uuid.lock().unwrap();

        let error = session.commit(&graph, None).unwrap_err();
        assert!(
            matches!(error, GfError::Validation(message) if message == "project generation changed since import began")
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), independent);
    }

    #[test]
    fn stale_cleanup_quarantines_when_construction_authority_changed() {
        let (_directory, _project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap();
        session.validate(&graph).unwrap();
        let session_uuid = session.session_uuid();
        let construction_uuid = session.manifest.construction_session_uuid.unwrap();
        drop(session);

        graph.add_node("Other", &HashMap::new()).unwrap();
        let independent = *graph.current_generation_uuid.lock().unwrap();
        let root = import_root(&graph, session_uuid).unwrap();
        let mut manifest = read_manifest(&root).unwrap();
        manifest.updated_unix_millis = 0;
        write_manifest(&root, &manifest).unwrap();

        let error = graph
            .cleanup_stale_import_sessions(Duration::from_secs(1))
            .unwrap_err();
        assert!(matches!(
            error,
            GfError::Validation(_) | GfError::Storage(_)
        ));
        assert_eq!(
            read_manifest(&root).unwrap().phase,
            ImportPhase::Quarantined
        );
        assert!(construction_root(&graph, construction_uuid).exists());
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), independent);
    }

    #[test]
    fn parquet_construction_receipts_scale_linearly_and_survive_reopen() {
        assert_eq!(ImportSessionLimits::default().batch_rows, 65_536);

        fn run(multiplier: usize) -> ImportConstructionEvidence {
            let (_directory, project, graph) = fixture();
            let source_dir = tempfile::tempdir().unwrap();
            let parquet = source_dir.path().join("nodes.parquet");
            let ids = (0..(4 * multiplier))
                .map(|_| Uuid::now_v7())
                .collect::<Vec<_>>();
            let batch = nodes(&ids);
            let mut writer =
                ArrowWriter::try_new(File::create(&parquet).unwrap(), batch.schema(), None)
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let limits = ImportSessionLimits {
                batch_rows: 4,
                ..ImportSessionLimits::default()
            };
            let mut session = graph
                .begin_import_session(OperationId(Uuid::now_v7()), limits)
                .unwrap();
            session
                .register_parquet(BulkInputKind::Node, &parquet)
                .unwrap();
            let session_uuid = session.session_uuid();
            let validated = session.validate(&graph).unwrap();
            assert_eq!(
                validated.construction.as_ref().unwrap().accepted_chunks,
                multiplier as u64
            );
            session.commit(&graph, None).unwrap();
            let (_, durable) = graph.import_session_status(session_uuid).unwrap();
            let receipt = durable.construction.unwrap();
            assert!(receipt.publication_committed);
            assert_eq!(receipt.input_rows, (4 * multiplier) as u64);
            assert_eq!(receipt.input_batches, multiplier as u64);
            assert_eq!(receipt.peak_batch_rows, 4);
            assert_eq!(
                receipt.publication_work.contract,
                "graphforge-publication-work/1"
            );
            let named = &receipt.publication_work;
            let expected_total = [
                &named.encode_write_postwrite_authentication,
                &named.publication_preauthentication,
                &named.cas_install_read_write,
                &named.hydration_verification,
                &named.fsync_synchronization,
            ]
            .iter()
            .map(|phase| phase.read_calls + phase.write_calls + phase.fsync_calls)
            .sum::<u64>();
            assert_eq!(named.semantic_total_operations, expected_total);
            assert_eq!(
                named.publication_preauthentication,
                receipt.application_io.phases
                    [&graphforge_storage::StorageIoPhase::PublicationPreauthentication]
            );

            drop(graph);
            let reopened = GraphForge::new(project.to_str()).unwrap();
            let (phase, reopened_progress) = reopened.import_session_status(session_uuid).unwrap();
            assert_eq!(phase, ImportPhase::Committed);
            assert_eq!(reopened_progress.construction.as_ref(), Some(&receipt));
            receipt
        }

        let receipts = [run(1), run(2), run(4)];
        for (previous, next) in receipts.iter().zip(receipts.iter().skip(1)) {
            assert_eq!(next.accepted_chunks, previous.accepted_chunks * 2);
            assert_eq!(next.input_rows, previous.input_rows * 2);
            assert_eq!(next.input_batches, previous.input_batches * 2);
            for (smaller, larger) in [
                (previous.write_bytes, next.write_bytes),
                (previous.write_operations, next.write_operations),
                (previous.immutable_artifacts, next.immutable_artifacts),
                (previous.fsync_operations, next.fsync_operations),
                (
                    previous.application_io.totals.write_calls,
                    next.application_io.totals.write_calls,
                ),
            ] {
                assert!(larger >= smaller, "durable work must be monotonic");
                assert!(
                    larger <= smaller.saturating_mul(3),
                    "doubling rows exceeded the bounded linear work envelope"
                );
            }
        }
    }
}
