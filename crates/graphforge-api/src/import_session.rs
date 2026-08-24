//! Durable, bounded staged graph-import sessions (#738).

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::ipc::writer::FileWriter as ArrowFileWriter;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{BulkInputKind, CancellationToken, GraphForge, OperationId};

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
            batch_rows: 8_192,
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
        copy_tree(&self.dir, &root.join("graph"))?;
        if !graphforge_storage::uuid_membership_index_is_fresh(&root.join("graph"))? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                &root.join("graph"),
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        File::create(root.join("nodes.uuidx")).map_err(storage)?;
        File::create(root.join("edges.uuidx")).map_err(storage)?;
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
            let mut manifest = read_manifest(&root)?;
            if matches!(
                manifest.phase,
                ImportPhase::Committed | ImportPhase::Aborted
            ) || now.saturating_sub(manifest.updated_unix_millis) < threshold
            {
                continue;
            }
            manifest.phase = ImportPhase::Aborted;
            manifest.updated_unix_millis = now;
            write_manifest(&root, &manifest)?;
            for path in [root.join("sources"), root.join("graph")] {
                if path.exists() {
                    fs::remove_dir_all(path).map_err(storage)?;
                }
            }
            cleaned = cleaned.saturating_add(1);
        }
        Ok(cleaned)
    }

    fn publish_import_tree(
        &self,
        staged_graph: &Path,
        operation_uuid: Uuid,
        expected_parent: Uuid,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Uuid, GfError> {
        use graphforge_storage::{ProjectGenerationRequest, ProjectStageOutcome};

        let _visibility = self.graph_visibility.acquire(cancellation)?;
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let container = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(container)?;
        parent.validate_complete_participant_inventory()?;
        if parent.generation_uuid() != expected_parent {
            return Err(validation(
                "project generation changed before import commit",
            ));
        }
        if !graphforge_storage::uuid_membership_index_is_fresh(staged_graph)? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                staged_graph,
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        let (inventory, _) = graphforge_storage::capture_graph_files(staged_graph)?;
        let cas_lease = graphforge_storage::begin_graph_object_publication(container)?;
        let (graph_root, _) =
            graphforge_storage::migrate_graph_files_v1_to_v2(container, staged_graph, &inventory)?;
        let graph_files = graphforge_storage::graph_files_root_participant(&graph_root)?;
        let candidate_graph_state = super::GraphFilesPublicationState {
            root: Some(graph_root),
            live_entries: inventory
                .files
                .into_iter()
                .map(|entry| (entry.relative_path.clone(), entry))
                .collect(),
            unpublished_v1_migration_lease: None,
        };
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;
        let receipt = graphforge_exec::MutationReceipt::default();
        let participants = super::graph_publication_participants(
            &parent,
            graph_files,
            self.semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .as_ref(),
            parent.capability("provenance")?.is_some(),
            super::GraphPublicationContext {
                receipt: &receipt,
                operation_uuid,
                actor_uuid: None,
                recorded_at_micros: recorded_at,
            },
        )?;
        let generation_uuid = super::mutation_generation_uuid(operation_uuid, &participants);
        let request = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities: super::publication_capabilities(&parent),
            participants,
        };
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let staged = graphforge_storage::stage_project_generation_with_graph_tree_mode(
            container,
            &request,
            None,
            self.lifecycle_mode,
        )?;
        drop(cas_lease);
        let publication = match staged {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(validation(
                                "project generation changed before import publication",
                            ));
                        }
                        Ok(())
                    },
                )?
                .publish()?,
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = publication.generation_uuid;
        *self
            .graph_files_publication
            .lock()
            .expect("graph publication lock poisoned") = candidate_graph_state;
        let resolved = graphforge_storage::resolve_project_generation(container)?;
        super::rematerialize_graph_workspace(&resolved, &self.dir)?;
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = super::load_runtime_catalog(&self.dir)?;
        self.adjacency_provider.invalidate();
        Ok(publication.generation_uuid)
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
        for input_kind in [BulkInputKind::Node, BulkInputKind::Edge] {
            for source_index in 0..self.manifest.sources.len() {
                let source = self.manifest.sources[source_index].clone();
                if source.kind.input_kind() != input_kind || source.staged {
                    continue;
                }
                let mut batch_index = 0_u64;
                for_each_source_batch(
                    &self.root,
                    &source,
                    self.manifest.limits.batch_rows,
                    |batch| {
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
                        let recovering = source.inflight_batch == Some(batch_index);
                        if !recovering {
                            self.manifest.sources[source_index].inflight_batch = Some(batch_index);
                            write_manifest(&self.root, &self.manifest)?;
                        }
                        let staged = match input_kind {
                            BulkInputKind::Node => {
                                stage_node_batch(graph, &self.root, operation, &batch, recovering)
                            }
                            BulkInputKind::Edge => {
                                stage_edge_batch(graph, &self.root, operation, &batch, recovering)
                            }
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
                        write_manifest(&self.root, &self.manifest)?;
                        Ok(())
                    },
                )?;
                self.manifest.sources[source_index].staged = true;
                self.manifest.progress.files_pending =
                    self.manifest.progress.files_pending.saturating_sub(1);
                write_manifest(&self.root, &self.manifest)?;
            }
        }
        self.manifest.phase = ImportPhase::Validated;
        self.checkpoint()
    }

    /// Abort without changing CURRENT; removes staged sources or quarantines on cleanup failure.
    pub fn abort(mut self) -> Result<ImportProgress, GfError> {
        if self.manifest.phase == ImportPhase::Committed {
            return Err(validation("committed import cannot be aborted"));
        }
        self.manifest.phase = ImportPhase::Aborted;
        self.checkpoint()?;
        let progress = self.manifest.progress.clone();
        let cleanup = [self.root.join("sources"), self.root.join("graph")]
            .into_iter()
            .try_for_each(|path| {
                if path.exists() {
                    fs::remove_dir_all(path)
                } else {
                    Ok(())
                }
            });
        match cleanup {
            Ok(()) => Ok(progress),
            Err(error) => {
                self.manifest.phase = ImportPhase::Quarantined;
                let _ = write_manifest(&self.root, &self.manifest);
                Err(storage(error))
            }
        }
    }

    /// Publish the fully staged graph, catalog, and membership indexes as one generation.
    pub fn commit(
        &mut self,
        graph: &GraphForge,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Uuid, GfError> {
        if let Some(published) = graphforge_storage::published_project_transaction(
            graph.resolved_generation.container_root(),
            self.manifest.operation_uuid,
        )? {
            self.manifest.phase = ImportPhase::Committed;
            self.checkpoint()?;
            return Ok(published.generation_uuid);
        }
        self.ensure_base(graph)?;
        if self.manifest.phase != ImportPhase::Validated
            || self.manifest.progress.files_pending != 0
        {
            return Err(validation("import must be fully validated before commit"));
        }
        let generation = graph.publish_import_tree(
            &self.root.join("graph"),
            self.manifest.operation_uuid,
            self.manifest.base_generation_uuid,
            cancellation,
        )?;
        self.manifest.phase = ImportPhase::Committed;
        self.checkpoint()?;
        Ok(generation)
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

fn stage_node_batch(
    graph: &GraphForge,
    root: &Path,
    operation: OperationId,
    batch: &RecordBatch,
    allow_replay: bool,
) -> Result<(), GfError> {
    let graph_dir = root.join("graph");
    let normalized = graph
        .normalize_import_nodes(operation, std::slice::from_ref(batch))
        .map_err(|error| validation(error.to_string()))?;
    let candidates = normalized
        .rows()
        .iter()
        .map(|row| row.node_uuid)
        .collect::<Vec<_>>();
    let base_nodes = graph
        .import_base_membership(
            &candidates,
            graphforge_storage::UuidIndexKind::Node,
            BulkInputKind::Node,
        )
        .map_err(|error| validation(error.to_string()))?;
    let base_edges = graph
        .import_base_membership(
            &candidates,
            graphforge_storage::UuidIndexKind::Edge,
            BulkInputKind::Node,
        )
        .map_err(|error| validation(error.to_string()))?;
    let session_nodes = probe_session_index(&root.join("nodes.uuidx"), &candidates)?;
    let session_edges = probe_session_index(&root.join("edges.uuidx"), &candidates)?;
    if allow_replay
        && session_nodes.iter().all(|found| *found)
        && base_nodes.iter().all(|found| !*found)
        && base_edges.iter().all(|found| !*found)
        && session_edges.iter().all(|found| !*found)
    {
        return Ok(());
    }
    if base_nodes
        .iter()
        .chain(&base_edges)
        .chain(&session_nodes)
        .chain(&session_edges)
        .any(|found| *found)
    {
        return Err(validation(
            "import node UUID conflicts with staged graph identity",
        ));
    }
    let mut catalog = super::load_runtime_catalog(&graph_dir)?;
    let mut writer = graphforge_storage::GraphWriter::open(&graph_dir, graph.ontology_mode)?;
    for row in normalized.rows() {
        let type_id = graph
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(&row.label))
            .unwrap_or_else(|| {
                graphforge_ir::runtime_entity_type_id(catalog.intern_label(&row.label))
            });
        writer.create_node(row.node_uuid, type_id)?;
        let properties = row
            .properties
            .iter()
            .map(|(name, value)| {
                catalog.intern_property(name, Some(&row.label));
                Ok((name.clone(), crate::construction::prop_literal(value)?))
            })
            .collect::<Result<HashMap<_, _>, GfError>>()?;
        if !properties.is_empty() {
            writer.set_properties(&row.node_uuid, Some(&row.label), properties)?;
        }
    }
    writer.flush()?;
    super::persist_runtime_catalog(&graph_dir, &catalog)?;
    merge_session_index(&root.join("nodes.uuidx"), &candidates)?;
    Ok(())
}

fn stage_edge_batch(
    graph: &GraphForge,
    root: &Path,
    operation: OperationId,
    batch: &RecordBatch,
    allow_replay: bool,
) -> Result<(), GfError> {
    let graph_dir = root.join("graph");
    let endpoints = batch_endpoint_uuids(batch)?;
    let imported_found = probe_session_index(&root.join("nodes.uuidx"), &endpoints)?;
    let imported_endpoints = endpoints
        .iter()
        .zip(imported_found)
        .filter_map(|(candidate, found)| found.then_some(*candidate))
        .collect::<BTreeSet<_>>();
    let normalized = graph
        .normalize_import_edges(operation, std::slice::from_ref(batch), &imported_endpoints)
        .map_err(|error| validation(error.to_string()))?;
    let candidates = normalized
        .rows()
        .iter()
        .map(|row| row.edge_uuid)
        .collect::<Vec<_>>();
    let base_edges = graph
        .import_base_membership(
            &candidates,
            graphforge_storage::UuidIndexKind::Edge,
            BulkInputKind::Edge,
        )
        .map_err(|error| validation(error.to_string()))?;
    let base_nodes = graph
        .import_base_membership(
            &candidates,
            graphforge_storage::UuidIndexKind::Node,
            BulkInputKind::Edge,
        )
        .map_err(|error| validation(error.to_string()))?;
    let session_edges = probe_session_index(&root.join("edges.uuidx"), &candidates)?;
    let session_nodes = probe_session_index(&root.join("nodes.uuidx"), &candidates)?;
    if allow_replay
        && session_edges.iter().all(|found| *found)
        && base_edges.iter().all(|found| !*found)
        && base_nodes.iter().all(|found| !*found)
        && session_nodes.iter().all(|found| !*found)
    {
        return Ok(());
    }
    if base_edges
        .iter()
        .chain(&base_nodes)
        .chain(&session_edges)
        .chain(&session_nodes)
        .any(|found| *found)
    {
        return Err(validation(
            "import edge UUID conflicts with staged graph identity",
        ));
    }
    let mut catalog = super::load_runtime_catalog(&graph_dir)?;
    let mut writer = graphforge_storage::GraphWriter::open(&graph_dir, graph.ontology_mode)?;
    let endpoints = normalized
        .rows()
        .iter()
        .flat_map(|row| [row.source_uuid, row.target_uuid])
        .collect::<BTreeSet<_>>();
    crate::bulk_construction::register_existing_endpoints(&mut writer, &graph_dir, &endpoints)?;
    for row in normalized.rows() {
        catalog.intern_relation_type(&row.rel_type);
        writer.create_edge(
            row.edge_uuid,
            &row.rel_type,
            &row.source_uuid,
            &row.target_uuid,
        )?;
        let properties = row
            .properties
            .iter()
            .map(|(name, value)| {
                catalog.intern_property(name, Some(&row.rel_type));
                Ok((name.clone(), crate::construction::prop_literal(value)?))
            })
            .collect::<Result<HashMap<_, _>, GfError>>()?;
        if !properties.is_empty() {
            writer.set_edge_properties(&row.edge_uuid, Some(&row.rel_type), properties)?;
        }
    }
    writer.flush()?;
    super::persist_runtime_catalog(&graph_dir, &catalog)?;
    merge_session_index(&root.join("edges.uuidx"), &candidates)?;
    Ok(())
}

fn batch_endpoint_uuids(batch: &RecordBatch) -> Result<Vec<Uuid>, GfError> {
    let mut endpoints = Vec::with_capacity(batch.num_rows().saturating_mul(2));
    for name in ["source_uuid", "target_uuid"] {
        let column = batch
            .column_by_name(name)
            .ok_or_else(|| validation(format!("missing {name}")))?;
        let values = column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| validation(format!("{name} must be fixed-size binary")))?;
        for row in 0..values.len() {
            endpoints.push(Uuid::from_slice(values.value(row)).map_err(storage)?);
        }
    }
    endpoints.sort_unstable();
    endpoints.dedup();
    Ok(endpoints)
}

fn probe_session_index(path: &Path, candidates: &[Uuid]) -> Result<Vec<bool>, GfError> {
    let mut file = File::open(path).map_err(storage)?;
    let bytes = file.metadata().map_err(storage)?.len();
    if bytes % 16 != 0 {
        return Err(validation("corrupt import membership index"));
    }
    let count = bytes / 16;
    candidates
        .iter()
        .map(|candidate| {
            let mut low = 0_u64;
            let mut high = count;
            let needle = candidate.as_bytes();
            let mut slot = [0_u8; 16];
            while low < high {
                let mid = low + (high - low) / 2;
                file.seek(SeekFrom::Start(mid * 16)).map_err(storage)?;
                file.read_exact(&mut slot).map_err(storage)?;
                match slot.cmp(needle) {
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                    std::cmp::Ordering::Equal => return Ok(true),
                }
            }
            Ok(false)
        })
        .collect()
}

fn merge_session_index(path: &Path, candidates: &[Uuid]) -> Result<(), GfError> {
    let mut additions = candidates.to_vec();
    additions.sort_unstable();
    additions.dedup();
    let old_file = File::open(path).map_err(storage)?;
    if old_file.metadata().map_err(storage)?.len() % 16 != 0 {
        return Err(validation("corrupt import membership index"));
    }
    let temp = path.with_extension("uuidx.tmp");
    let mut output = BufWriter::new(File::create(&temp).map_err(storage)?);
    let mut old = BufReader::new(old_file);
    let mut old_value = read_index_uuid(&mut old)?;
    let mut new_values = additions.iter().peekable();
    while old_value.is_some() || new_values.peek().is_some() {
        match (old_value, new_values.peek()) {
            (Some(current), Some(new_value)) if current <= *new_value.as_bytes() => {
                output.write_all(&current).map_err(storage)?;
                old_value = read_index_uuid(&mut old)?;
            }
            (_, Some(new_value)) => {
                output.write_all(new_value.as_bytes()).map_err(storage)?;
                new_values.next();
            }
            (Some(current), None) => {
                output.write_all(&current).map_err(storage)?;
                old_value = read_index_uuid(&mut old)?;
            }
            (None, None) => break,
        }
    }
    output.flush().map_err(storage)?;
    output.get_ref().sync_all().map_err(storage)?;
    drop(output);
    fs::rename(temp, path).map_err(storage)
}

fn read_index_uuid(reader: &mut BufReader<File>) -> Result<Option<[u8; 16]>, GfError> {
    if reader.fill_buf().map_err(storage)?.is_empty() {
        return Ok(None);
    }
    let mut value = [0_u8; 16];
    reader.read_exact(&mut value).map_err(storage)?;
    Ok(Some(value))
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
    serde_json::from_reader(BufReader::new(
        File::open(root.join(MANIFEST)).map_err(storage)?,
    ))
    .map_err(storage)
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

fn copy_tree(source: &Path, destination: &Path) -> Result<(), GfError> {
    fs::create_dir_all(destination).map_err(storage)?;
    for entry in fs::read_dir(source).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let file_type = entry.file_type().map_err(storage)?;
        if file_type.is_symlink() {
            return Err(validation("staged graph copy refuses symlink entries"));
        }
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target).map_err(storage)?;
        } else {
            return Err(validation("staged graph copy refuses special files"));
        }
    }
    Ok(())
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
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryArray, StringArray};
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
        let progress = session.abort().unwrap();
        assert_eq!(progress.rows_accepted, 1);
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
        stage_node_batch(&graph, &session.root, batch_operation, &batch, false).unwrap();
        session.manifest.sources[0].inflight_batch = Some(0);
        write_manifest(&session.root, &session.manifest).unwrap();
        let session_uuid = session.session_uuid();
        drop(session);

        let mut resumed = graph.resume_import_session(session_uuid).unwrap();
        assert_eq!(resumed.validate(&graph).unwrap().rows_accepted, 1);
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
        assert!(!root.join("graph").exists());
        assert!(!root.join("sources").exists());
        assert_eq!(read_manifest(&root).unwrap().phase, ImportPhase::Aborted);
    }
}
