//! Durable, bounded staged graph-import sessions (#738).

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::ipc::writer::FileWriter as ArrowFileWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use graphforge_core::GfError;
use graphforge_storage::concurrency_attribution::ObservedSha256 as Sha256;
use graphforge_storage::concurrency_attribution::RegionScope;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{ChunkReader, Length};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

use crate::{BulkInputKind, CancellationToken, GraphConstructionBudgets, GraphForge, OperationId};

mod bounded_ipc;
pub(crate) mod bulk_source;
#[cfg(test)]
mod cpu_budget_report;
mod external_source;
mod inventory_budget;
mod journal;
mod memory_budget;
mod normalization;
mod parquet_admission;
mod parquet_brotli;
mod parquet_codec;
mod parquet_delta;
mod parquet_events;
mod parquet_levels;
mod parquet_page;
mod parquet_page_decode;
mod parquet_windows;
mod parquet_scan;
mod parquet_values;

/// Written by this version: a session may register Parquet sources that stay
/// where they are (#1898).
const FORMAT_VERSION: u32 = 3;
/// Sessions an earlier version began copied Parquet sources into the session;
/// they still resume, validate and append.
const OLDEST_FORMAT_VERSION: u32 = 2;
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
    /// Passes of the bulk builder when the generation was built from the
    /// registered sources rather than staged chunk by chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bulk_build: Option<graphforge_storage::BulkBuildReport>,
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
    /// Durability barriers for accepted construction artifacts. Full phase
    /// synchronization, including recovery checkpoints, is in `application_io`.
    pub fsync_operations: u64,
    /// Successful file-level page-cache release boundaries.
    #[serde(default)]
    pub cache_release_operations: u64,
    /// Page-cache release boundaries unsupported by the target platform.
    #[serde(default)]
    pub cache_release_unsupported_operations: u64,
    /// Clean file bytes covered by successful page-cache release requests.
    #[serde(default)]
    pub cache_released_bytes: u64,
    /// Largest synchronized file-cache window between release boundaries.
    #[serde(default)]
    pub peak_cache_release_window_bytes: u64,
    /// Largest retained Arrow row window.
    pub peak_batch_rows: u64,
    /// Largest retained Arrow byte window.
    pub peak_batch_bytes: u64,
    /// Provenance of every registered source once all are staged: what was read
    /// and the SHA-256 the build's own read pass computed. Empty before then.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_provenance: Vec<ImportSourceProvenance>,
    /// Exact transient allocation high-water retained across resume.
    pub transient_peak_allocated_bytes: u64,
    /// Receipt-owned construction staging/spill category totals.
    #[serde(default)]
    pub construction_staging: graphforge_storage::ArtifactStorageTotals,
    /// Peak allocated bytes for construction staging/spill specifically.
    #[serde(default)]
    pub construction_staging_transient_peak_allocated_bytes: u64,
    /// Partitions over the recorded resident budget that were sorted in
    /// bounded runs instead of refused (ADR 0047, #1585).
    #[serde(default)]
    pub external_partitions: u64,
    /// Sorted runs written for those partitions.
    #[serde(default)]
    pub external_runs: u64,
    /// Scratch bytes written to those runs.
    #[serde(default)]
    pub external_run_bytes: u64,
}

/// Provenance of one registered import source, from the read pass that staged it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ImportSourceProvenance {
    /// Registration order.
    pub sequence: u64,
    /// Encoding and role of the source.
    pub kind: ImportSourceKind,
    /// Registered size in bytes.
    pub bytes: u64,
    /// Whole-file SHA-256 of an in-place Parquet source, lowercase hex: the content
    /// read under the identity pin (device, inode, size and modification time
    /// unchanged from registration). It is folded from the bytes the build decoded;
    /// a range the held-byte bound dropped, and bytes no decode asks for, are read
    /// from the file when the digest completes. It does not detect a rewrite that
    /// preserves all four pinned fields, and a staged import resumed after a stop
    /// may pair batches decoded earlier with the digest of its final pass. Absent
    /// for session-owned Arrow encodings and for copies made by earlier versions.
    pub sha256: Option<String>,
    /// SHA-256 of the Parquet footer recorded at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_sha256: Option<String>,
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
    /// A Parquet source is read where it is; registration recorded its identity
    /// here. Arrow sources are session-owned encodings and have none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    external: Option<external_source::ExternalSource>,
    /// Whole-file SHA-256 of an external source, computed by the first complete
    /// read pass and required to match on every later one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionManifest {
    format_version: u32,
    journal_sequence: u64,
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
    /// How `validate` builds this session's generation, fixed by its first call
    /// and read back by every later one (ADR 0058). `None` until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    build_route: Option<BuildRoute>,
    /// Why an initial build staged instead of running on the bulk builder, when
    /// the plan said so (ADR 0058). `None` for builds that did not stage on a plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staged_reason: Option<graphforge_storage::BulkStagedReason>,
}

/// The two ways `validate` builds a generation (ADR 0058).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum BuildRoute {
    /// The bulk builder reads the registered sources and stages nothing.
    Bulk,
    /// Chunk-by-chunk staging, shaping and encoding.
    Staged,
}

/// Monotonic wall time and process CPU for attempted calls, including returned
/// errors. These observations are not durable progress or performance limits.
///
/// [`ImportCallTiming::effective_cores`] reports process CPU/wall for the
/// operation. It does not identify a serial fraction or throughput speedup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ImportCallTiming {
    /// Number of attempted calls.
    pub calls: u64,
    /// Calls that returned an error.
    pub errors: u64,
    /// Sum of elapsed nanoseconds around these calls.
    pub elapsed_ns: u64,
    /// Sum of process CPU nanoseconds consumed across these calls.
    pub cpu_ns: u64,
    /// Calls for which process CPU could not be read. Non-zero makes
    /// [`ImportCallTiming::effective_cores`] refuse rather than under-report.
    pub cpu_unmeasured_calls: u64,
}

impl ImportCallTiming {
    fn record(&mut self, started: CallStart, failed: bool) {
        self.elapsed_ns = self
            .elapsed_ns
            .saturating_add(u64::try_from(started.wall.elapsed().as_nanos()).unwrap_or(u64::MAX));
        match (
            started.cpu,
            graphforge_storage::concurrency_attribution::process_cpu_time(),
        ) {
            (Some(before), Some(after)) => {
                self.cpu_ns = self.cpu_ns.saturating_add(
                    u64::try_from(after.saturating_sub(before).as_nanos()).unwrap_or(u64::MAX),
                );
            }
            _ => self.cpu_unmeasured_calls = self.cpu_unmeasured_calls.saturating_add(1),
        }
        self.calls = self.calls.saturating_add(1);
        self.errors = self.errors.saturating_add(u64::from(failed));
    }

    /// Process CPU divided by elapsed wall across these calls: how many cores'
    /// worth the operation used. `1.0` means it ran on one core.
    ///
    /// `None` when no wall time elapsed or any call's CPU could not be read,
    /// rather than a fabricated zero — a reader must be able to tell "not
    /// measured" from "measured as idle".
    ///
    /// **This is a process-level ratio.** The CPU term counts every thread in
    /// the process, so it answers "how much of this machine did the process use
    /// during this operation", which is only the operation's own figure when the
    /// process is doing one thing. A ladder rung is; a busy embedding host is
    /// not. See `graphforge_storage::concurrency_attribution`.
    #[must_use]
    pub fn effective_cores(&self) -> Option<f64> {
        if self.elapsed_ns == 0 || self.cpu_unmeasured_calls > 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        Some(self.cpu_ns as f64 / self.elapsed_ns as f64)
    }
}

/// Wall and process CPU captured at the start of a timed call.
#[derive(Clone, Copy, Debug)]
struct CallStart {
    wall: Instant,
    cpu: Option<std::time::Duration>,
}

impl CallStart {
    fn now() -> Option<Self> {
        graphforge_storage::lifecycle_io::is_active().then(|| {
            #[cfg(test)]
            CALL_TIMING_SAMPLES.with(|samples| samples.set(samples.get() + 1));
            Self {
                wall: Instant::now(),
                cpu: graphforge_storage::concurrency_attribution::process_cpu_time(),
            }
        })
    }
}

#[cfg(test)]
thread_local! {
    static CALL_TIMING_SAMPLES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Disjoint call timings from the latest import validation or commit invocation.
/// Reset before either operation, including precondition errors; never persisted.
/// Lost-process work and source decoding, normalization, and checkpoints outside
/// these calls are not measured. The sum is not whole-import elapsed time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ImportOperationTimings {
    /// Fresh construction creation, excluding the import manifest checkpoint.
    pub begin: ImportCallTiming,
    /// Resumed construction authentication/reconciliation, excluding facade open.
    pub resume: ImportCallTiming,
    /// Construction append calls, including idempotent replay attempts.
    pub append: ImportCallTiming,
    /// Validation and sealing of construction artifacts.
    pub seal: ImportCallTiming,
    /// Publication, including work inside the public seal-and-publish call.
    pub publish: ImportCallTiming,
}

/// Owned handle for a durable staged import. The handle contains no live rows.
pub struct GraphImportSession {
    allocation_operation: Option<graphforge_storage::StorageAllocationOperation>,
    root: PathBuf,
    manifest: SessionManifest,
    observed: Instant,
    operation_timings: Option<ImportOperationTimings>,
    journal: journal::Journal,
}

impl GraphForge {
    /// Begin a durable import pinned to the facade's current project generation.
    pub fn begin_import_session(
        &self,
        operation_uuid: OperationId,
        limits: ImportSessionLimits,
    ) -> Result<GraphImportSession, GfError> {
        let _region = RegionScope::named("begin_import");
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
            journal_sequence: 0,
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
            build_route: None,
            staged_reason: None,
        };
        let journal = journal::Journal::open(&root, &manifest, self.allocation_operation.as_ref())?;
        write_manifest_with_allocation(&root, &manifest, self.allocation_operation.as_ref())?;
        Ok(GraphImportSession {
            allocation_operation: self.allocation_operation.clone(),
            journal,
            root,
            manifest,
            observed: Instant::now(),
            operation_timings: None,
        })
    }

    /// Resume one durable, non-terminal session after process interruption.
    pub fn resume_import_session(&self, session_uuid: Uuid) -> Result<GraphImportSession, GfError> {
        let _region = RegionScope::named("resume_import");
        let root = import_root(self, session_uuid)?;
        let manifest = read_manifest(&root)?;
        if !supported_format(manifest.format_version) || manifest.session_uuid != session_uuid {
            return Err(validation("incompatible or mismatched import manifest"));
        }
        if matches!(
            manifest.phase,
            ImportPhase::Committed | ImportPhase::Aborted
        ) {
            return Err(validation("terminal import sessions cannot be resumed"));
        }
        Ok(GraphImportSession {
            allocation_operation: self.allocation_operation.clone(),
            journal: journal::Journal::open(&root, &manifest, self.allocation_operation.as_ref())?,
            root,
            manifest,
            observed: Instant::now(),
            operation_timings: None,
        })
    }

    /// Reopen the durable, content-free status of any import session, including terminal ones.
    pub fn import_session_status(
        &self,
        session_uuid: Uuid,
    ) -> Result<(ImportPhase, ImportProgress), GfError> {
        let root = import_root(self, session_uuid)?;
        let manifest = read_manifest(&root)?;
        if !supported_format(manifest.format_version) || manifest.session_uuid != session_uuid {
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
                allocation_operation: self.allocation_operation.clone(),
                journal: journal::Journal::open(
                    &root,
                    &manifest,
                    self.allocation_operation.as_ref(),
                )?,
                root,
                manifest,
                observed: Instant::now(),
                operation_timings: None,
            }
            .abort(self)?;
            cleaned = cleaned.saturating_add(1);
        }
        Ok(cleaned)
    }
}

impl GraphImportSession {
    /// Timings from the latest validation or commit on this handle, including
    /// calls that returned errors. `None` means no optional capture was requested.
    /// Reading these observations performs no I/O.
    #[must_use]
    pub fn operation_timings(&self) -> Option<ImportOperationTimings> {
        self.operation_timings
    }
    fn publish_source(
        &mut self,
        temporary: &Path,
        destination: &Path,
        seal: graphforge_storage::durable_commit::FileSeal,
    ) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        let result = journal::publish_source(
            temporary,
            destination,
            seal,
            self.allocation_operation.as_ref(),
        );
        if result.is_err() {
            self.journal.poison();
        }
        result
    }

    fn persist_manifest(&mut self) -> Result<(), GfError> {
        self.persist_manifest_with_source_cleanup(None)
    }

    fn persist_manifest_with_source_cleanup(
        &mut self,
        unpublished_source: Option<&Path>,
    ) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        if let Err(error) = self.journal.sync(self.allocation_operation.as_ref()) {
            return Err(self.poison_checkpoint_failure(error, false, unpublished_source));
        }
        let mut publication_started = false;
        let result = journal::write_checkpoint(
            &self.root,
            &self.manifest,
            self.allocation_operation.as_ref(),
            &mut publication_started,
        );
        if let Err(error) = result {
            return Err(self.poison_checkpoint_failure(
                error,
                publication_started,
                unpublished_source,
            ));
        }
        Ok(())
    }

    fn poison_checkpoint_failure(
        &mut self,
        error: GfError,
        publication_started: bool,
        unpublished_source: Option<&Path>,
    ) -> GfError {
        // Only the failing registration operation can supply its new source.
        // A failed journal barrier has poisoned the writer, but this source is
        // still absent from the prior checkpoint: cleaning it is completion of
        // the failed operation, never admission of a new session mutation.
        // Once replacement was attempted, the source may be authoritative.
        let cleanup = if publication_started {
            Ok(None)
        } else {
            unpublished_source
                .map(|source| journal::cleanup_source(source, self.allocation_operation.as_ref()))
                .transpose()
        };
        self.journal.poison();
        match cleanup {
            Ok(_) => error,
            Err(cleanup) => storage(format!("{error}; source cleanup failed: {cleanup}")),
        }
    }

    fn persist_progress(&mut self, source_index: usize) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        self.journal.append(
            &mut self.manifest,
            source_index,
            self.allocation_operation.as_ref(),
        )?;
        #[cfg(test)]
        journal::failure("append_before_fsync")?;
        #[cfg(test)]
        if self.manifest.sources[source_index].inflight_batch.is_none() {
            journal::failure("completed_before_fsync")?;
        }
        Ok(())
    }

    fn cleanup_source(&self, destination: &Path) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        journal::cleanup_source(destination, self.allocation_operation.as_ref())
    }

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
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("register_arrow");
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
        let cache_writer =
            graphforge_filesystem::DurableFileCacheWriter::new(file).map_err(storage)?;
        let mut writer =
            ArrowFileWriter::try_new(BufWriter::new(cache_writer), &batches[0].schema())
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
        writer.flush().map_err(storage)?;
        let seal = graphforge_storage::durable_commit::seal_cache_writer_witness(
            writer.get_mut().get_mut(),
        )
        .map_err(storage)?;
        drop(writer);
        self.publish_source(&temporary, &destination, seal)?;
        let result = self.register_record(
            source_kind,
            name,
            destination.metadata().map_err(storage)?.len(),
            rows,
            None,
        );
        if result.is_err() {
            if self.journal.ensure_writable().is_ok() {
                self.cleanup_source(&destination)?;
            }
        } else {
            RegionScope::record_work("rows", rows);
        }
        result
    }

    /// Register a local Parquet source, which stays where it is.
    ///
    /// Registration records the file's canonical path, native identity, size,
    /// modification time and Parquet footer; it copies and writes nothing. Each
    /// later read refuses a source that no longer matches (see
    /// [`external_source`]).
    pub fn register_parquet(&mut self, kind: BulkInputKind, source: &Path) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("register_parquet");
        self.ensure_open()?;
        reject_unsafe_path(source)?;
        // One open of the file decides everything recorded about it.
        let external = external_source::ExternalSource::capture(source)?;
        if self
            .manifest
            .progress
            .bytes_accepted
            .saturating_add(external.size)
            > self.manifest.limits.max_source_bytes
        {
            return Err(limit("import max_source_bytes exceeded"));
        }
        let sequence = self.next_sequence()?;
        let name = format!("{sequence:020}.parquet");
        let bytes = external.size;
        let result = self.register_record(
            match kind {
                BulkInputKind::Node => ImportSourceKind::ParquetNodes,
                BulkInputKind::Edge => ImportSourceKind::ParquetEdges,
            },
            name,
            bytes,
            0,
            Some(external),
        );
        if result.is_ok() {
            RegionScope::record_work("bytes", bytes);
        }
        result
    }

    /// Persist counters and source ordering without publishing graph state.
    pub fn checkpoint(&mut self) -> Result<ImportProgress, GfError> {
        self.checkpoint_with_source_cleanup(None)
    }

    fn checkpoint_with_source_cleanup(
        &mut self,
        unpublished_source: Option<&Path>,
    ) -> Result<ImportProgress, GfError> {
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("checkpoint");
        self.manifest.progress.elapsed_millis =
            self.manifest.progress.elapsed_millis.saturating_add(
                u64::try_from(self.observed.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
        self.observed = Instant::now();
        self.manifest.updated_unix_millis = unix_millis()?;
        self.persist_manifest_with_source_cleanup(unpublished_source)?;
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
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("stage+seal");
        self.operation_timings =
            graphforge_storage::lifecycle_io::is_active().then(ImportOperationTimings::default);
        self.ensure_open()?;
        self.ensure_base(graph)?;
        let mut construction = self.open_construction(graph)?;
        let session_root = self.root.clone();
        let batch_rows = self.manifest.limits.batch_rows;
        // Pass 0 routing. The route is a function of durable state: the first
        // `validate` decides it from the session and the footers, writes it to
        // the manifest, and every later call reads it back. Live memory is
        // consulted exactly once, so a refused, cancelled or killed bulk
        // attempt followed by a memory drop cannot send a sealed session to
        // the staged path, which would refuse every retry. An initial build
        // that has staged nothing runs on the bulk builder, in memory or, when
        // its estimate exceeds the budget, on scratch files (ADR 0058). An
        // append, a session an earlier binary began staging, and an initial
        // build the builder cannot hold the node tables or edge properties of
        // stage; the last records its typed reason.
        let refusals = bulk_source::Refusals::default();
        let digests = bulk_source::Digests::default();
        let route = if let Some(route) = self.manifest.build_route {
            route
        } else {
            let initial = {
                let progress = construction.progress();
                progress.parent_topology_generation == 0
                    && progress.accepted_chunks == 0
                    && self
                        .manifest
                        .sources
                        .iter()
                        .all(|source| !source.staged && source.batches_staged == 0)
            };
            let route = if initial {
                match self
                    .plan_bulk_build(graph, cancellation, &refusals, &digests)?
                    .route()
                {
                    graphforge_storage::BulkRoute::Staged(reason) => {
                        self.manifest.staged_reason = Some(reason);
                        BuildRoute::Staged
                    }
                    graphforge_storage::BulkRoute::Memory
                    | graphforge_storage::BulkRoute::Scratch => BuildRoute::Bulk,
                }
            } else {
                BuildRoute::Staged
            };
            self.manifest.build_route = Some(route);
            self.persist_manifest()?;
            route
        };
        if route == BuildRoute::Bulk {
            // An initial build restarts rather than resumes (#1881). An encoded
            // inventory is reused only if the digest of every in-place source it
            // was built from is already recorded, which binds the two: otherwise
            // a crash after the inventory was pinned and before the digests were
            // recorded would pair that graph with the digest of whatever the
            // file holds now.
            let reused = construction.progress().state
                != graphforge_storage::GraphConstructionState::Staging;
            if reused && self.in_place_digest_missing() {
                construction = self.restart_construction(graph, construction)?;
            }
            let reused = reused && !self.in_place_digest_missing();
            // The routing plan above read only footers; a fresh one reads the
            // sources, so its digests are the build's.
            let digests = bulk_source::Digests::default();
            let plan = self.plan_bulk_build(graph, cancellation, &refusals, &digests)?;
            let built =
                self.build_initial(&mut construction, &plan, &digests, reused, cancellation);
            if built.is_err() {
                self.record_bulk_refusal(&refusals)?;
            }
            return built;
        }
        for input_kind in [BulkInputKind::Node, BulkInputKind::Edge] {
            for source_index in 0..self.manifest.sources.len() {
                let source = self.manifest.sources[source_index].clone();
                if source.kind.input_kind() != input_kind || source.staged {
                    continue;
                }
                let digest = normalization::for_each(
                    graph,
                    &session_root,
                    &source,
                    batch_rows,
                    self.manifest.operation_uuid,
                    cancellation,
                    |index, batch| {
                        self.append_source_batch(
                            &mut construction,
                            source_index,
                            index,
                            &batch,
                            cancellation,
                        )
                    },
                )?;
                self.record_source_digest(source_index, digest)?;
                self.manifest.sources[source_index].staged = true;
                self.manifest.progress.files_pending =
                    self.manifest.progress.files_pending.saturating_sub(1);
                self.persist_progress(source_index)?;
            }
            // Construction refuses new nodes after accepting its first edge.
            // Make the node prefix a durable checkpoint before that boundary,
            // so losing the edge-phase tail never replays a node after edges.
            if input_kind == BulkInputKind::Node {
                self.persist_manifest()?;
            }
        }
        self.seal_construction(&mut construction, cancellation)
    }

    /// Count the refused batch's rows as rejected, as the staged path does.
    fn record_bulk_refusal(&mut self, refusals: &bulk_source::Refusals) -> Result<(), GfError> {
        let Some(rows) = refusals.take_rows() else {
            return Ok(());
        };
        let remaining = self
            .manifest
            .limits
            .max_rejected_rows
            .saturating_sub(self.manifest.progress.rows_rejected);
        self.manifest.progress.rows_rejected = self
            .manifest
            .progress
            .rows_rejected
            .saturating_add(rows.min(remaining));
        // A refusal is an explicit operation boundary: retain diagnostics even
        // if the caller never checkpoints.
        self.persist_manifest()
    }

    /// Pass 0: plan every registered source from its footer.
    fn plan_bulk_build<'a>(
        &self,
        graph: &'a GraphForge,
        cancellation: Option<&'a CancellationToken>,
        refusals: &'a bulk_source::Refusals,
        digests: &'a bulk_source::Digests,
    ) -> Result<graphforge_storage::BulkBuildPlan<'a>, GfError> {
        let mut plan = graphforge_storage::BulkBuildPlan {
            memory_budget: Some(bulk_source::bulk_build_memory_budget()?),
            ..Default::default()
        };
        for source in &self.manifest.sources {
            let planned = bulk_source::plan(
                graph,
                &self.root,
                source,
                self.manifest.limits.batch_rows,
                self.manifest.operation_uuid,
                cancellation,
                refusals,
                digests,
                u64::try_from(self.construction_budgets().max_batch_bytes).unwrap_or(u64::MAX),
            )?;
            match source.kind.input_kind() {
                BulkInputKind::Node => plan.nodes.push(planned),
                BulkInputKind::Edge => plan.edges.push(planned),
            }
        }
        Ok(plan)
    }

    /// Build an initial generation from every registered source (#1883).
    ///
    /// Sources are read in place and in parallel; nothing is staged, so a
    /// crash leaves nothing to resume and the next `validate` reruns the build.
    fn build_initial(
        &mut self,
        construction: &mut crate::GraphConstructionSession<'_>,
        plan: &graphforge_storage::BulkBuildPlan<'_>,
        digests: &bulk_source::Digests,
        reused_inventory: bool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ImportProgress, GfError> {
        let region = RegionScope::named("bulk_build");
        let started = CallStart::now();
        let built = construction.build_initial(plan, cancellation);
        if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
            timings.seal.record(started, built.is_err());
        }
        drop(region);
        let report = built?;
        // A reused inventory decoded nothing: the digests recorded with it stand,
        // and the file is not read again to produce another.
        let mut source_digests = if reused_inventory {
            std::collections::BTreeMap::new()
        } else {
            digests.finish()?
        };
        let (nodes, edges) = (report.nodes, report.edges);
        for source_index in 0..self.manifest.sources.len() {
            if self.manifest.sources[source_index].external.is_some() {
                // A successful build read every task of every in-place source.
                match source_digests.remove(&self.manifest.sources[source_index].sequence) {
                    Some(digest) => self.record_source_digest(source_index, Some(digest))?,
                    None if self.manifest.sources[source_index].sha256.is_some() => {}
                    None => return Err(storage("the build finished without a source digest")),
                }
            }
            self.manifest.sources[source_index].staged = true;
        }
        self.manifest.progress.rows_accepted = nodes.saturating_add(edges);
        self.manifest.progress.files_pending = 0;
        self.manifest.progress.peak_batch_rows = self
            .manifest
            .progress
            .peak_batch_rows
            .max(self.manifest.limits.batch_rows as u64);
        self.update_construction_progress(&construction.progress())?;
        if let Some(evidence) = self.manifest.progress.construction.as_mut() {
            evidence.bulk_build = Some(report);
        }
        self.manifest.phase = ImportPhase::Validated;
        self.checkpoint()
    }

    /// Whether an in-place source has no recorded digest.
    fn in_place_digest_missing(&self) -> bool {
        self.manifest
            .sources
            .iter()
            .any(|source| source.external.is_some() && source.sha256.is_none())
    }

    /// Replace the construction session with an empty one. The manifest forgets
    /// the old session before it is discarded, so a crash in between leaves an
    /// unreferenced session rather than a manifest naming one that is gone.
    fn restart_construction<'a>(
        &mut self,
        graph: &'a GraphForge,
        old: crate::GraphConstructionSession<'a>,
    ) -> Result<crate::GraphConstructionSession<'a>, GfError> {
        self.manifest.construction_session_uuid = None;
        self.persist_manifest()?;
        old.discard()?;
        self.open_construction(graph)
    }

    /// Keep the first complete read's SHA-256 as the source's provenance and
    /// refuse any later read that disagrees with it.
    fn record_source_digest(
        &mut self,
        source_index: usize,
        digest: Option<String>,
    ) -> Result<(), GfError> {
        let Some(digest) = digest else {
            return Ok(());
        };
        let source = &mut self.manifest.sources[source_index];
        match &source.sha256 {
            Some(recorded) if *recorded != digest => Err(external_source::source_changed(
                source
                    .external
                    .as_ref()
                    .map_or_else(|| Path::new(&source.name), |external| &external.path),
                external_source::SourceChange::DigestChanged,
                &format!("recorded {recorded}, read {digest}"),
            )),
            Some(_) => Ok(()),
            None => {
                source.sha256 = Some(digest);
                Ok(())
            }
        }
    }

    fn append_source_batch(
        &mut self,
        construction: &mut crate::GraphConstructionSession<'_>,
        source_index: usize,
        index: u64,
        batch: &RecordBatch,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        let input_kind = self.manifest.sources[source_index].kind.input_kind();
        let mut batch_index = index;
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        if batch.num_rows() == 0 {
            batch_index += 1;
            self.manifest.sources[source_index].batches_staged = batch_index;
            self.manifest.sources[source_index].inflight_batch = None;
            self.persist_progress(source_index)?;
            return Ok(());
        }
        let recovering = self.manifest.sources[source_index].inflight_batch == Some(batch_index);
        if !recovering {
            self.manifest.sources[source_index].inflight_batch = Some(batch_index);
            self.persist_progress(source_index)?;
        }
        let chunk_id = format!(
            "import-{:020}-{:020}",
            self.manifest.sources[source_index].sequence, batch_index
        );
        let region = RegionScope::named(append_region_name(input_kind));
        let started = CallStart::now();
        let staged = match (input_kind, cancellation) {
            (BulkInputKind::Node, Some(token)) => {
                construction.append_nodes_with_cancellation(&chunk_id, batch, token)
            }
            (BulkInputKind::Node, None) => construction.append_nodes(&chunk_id, batch),
            (BulkInputKind::Edge, Some(token)) => {
                construction.append_edges_with_cancellation(&chunk_id, batch, token)
            }
            (BulkInputKind::Edge, None) => construction.append_edges(&chunk_id, batch),
        };
        self.record_append_timing(started, staged.is_err());
        if staged.is_ok() {
            RegionScope::record_work("rows", batch.num_rows() as u64);
        }
        drop(region);
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
            self.persist_progress(source_index)?;
            // Refusal is an explicit operation boundary: retain
            // diagnostics even if the caller never checkpoints.
            self.persist_manifest()?;
            return Err(error);
        }
        #[cfg(test)]
        journal::failure("accepted_before_progress")?;
        #[cfg(test)]
        if input_kind == BulkInputKind::Edge {
            journal::failure("edge_accepted_before_progress")?;
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
        self.persist_progress(source_index)?;
        Ok(())
    }

    fn record_append_timing(&mut self, started: Option<CallStart>, failed: bool) {
        if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
            timings.append.record(started, failed);
        }
    }

    fn seal_construction(
        &mut self,
        construction: &mut crate::GraphConstructionSession<'_>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ImportProgress, GfError> {
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("seal");
        self.persist_manifest()?;
        #[cfg(test)]
        journal::failure("fsync_before_seal")?;
        let started = CallStart::now();
        let sealed = construction.validate_and_seal(cancellation);
        if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
            timings.seal.record(started, sealed.is_err());
        }
        sealed?;
        self.update_construction_progress(&construction.progress())?;
        self.manifest.phase = ImportPhase::Validated;
        self.checkpoint()
    }

    /// Abort without changing CURRENT; removes staged sources or quarantines on cleanup failure.
    pub fn abort(mut self, graph: &GraphForge) -> Result<ImportProgress, GfError> {
        self.journal.ensure_writable()?;
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
                let _ = self.persist_manifest();
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
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("commit");
        self.operation_timings =
            graphforge_storage::lifecycle_io::is_active().then(ImportOperationTimings::default);
        if self.manifest.phase != ImportPhase::Validated
            || self.manifest.progress.files_pending != 0
        {
            return Err(validation("import must be fully validated before commit"));
        }
        self.ensure_base(graph)?;
        let mut construction = self.open_construction(graph)?;
        let started = CallStart::now();
        let region = RegionScope::named("publish");
        let publication = match cancellation {
            Some(token) => construction.seal_and_publish_with_cancellation(token),
            None => construction.seal_and_publish(),
        };
        if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
            timings.publish.record(started, publication.is_err());
        }
        drop(region);
        let publication = publication?;
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
        self.journal.ensure_writable()?;
        let _region = RegionScope::named("open_construction");
        let budgets = self.construction_budgets();
        let started = CallStart::now();
        if let Some(session_uuid) = self.manifest.construction_session_uuid {
            let resumed = graph.resume_graph_construction(session_uuid, budgets);
            if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
                timings.resume.record(started, resumed.is_err());
            }
            return resumed;
        }
        let session = graph.begin_staged_graph_construction(budgets);
        if let (Some(timings), Some(started)) = (&mut self.operation_timings, started) {
            timings.begin.record(started, session.is_err());
        }
        let session = session?;
        self.manifest.construction_session_uuid = Some(session.session_uuid());
        self.persist_manifest()?;
        Ok(session)
    }

    fn update_construction_progress(
        &mut self,
        progress: &crate::GraphConstructionProgress,
    ) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        let application_io = graphforge_storage::ConstructionPhaseAttribution::from_construction(
            &progress.evidence,
        )?;
        application_io.validate_for_qualification()?;
        let publication_work = PublicationWorkComponents::from_application_io(&application_io)?;
        let construction_staging = progress
            .evidence
            .storage_current
            .get(&graphforge_storage::ArtifactCategory::ConstructionStaging)
            .cloned()
            .unwrap_or_default();
        let construction_staging_transient_peak_allocated_bytes = progress
            .evidence
            .storage_transient_peak_allocated_bytes
            .get(&graphforge_storage::ArtifactCategory::ConstructionStaging)
            .copied()
            .unwrap_or_default();
        // Recorded once the last source is staged, so the journal frames written
        // for every batch before then stay small however many sources there are.
        let source_provenance = if self.manifest.progress.files_pending == 0 {
            self.manifest
                .sources
                .iter()
                .map(|source| ImportSourceProvenance {
                    sequence: source.sequence,
                    kind: source.kind,
                    bytes: source.bytes,
                    sha256: source.sha256.clone(),
                    footer_sha256: source
                        .external
                        .as_ref()
                        .map(|external| external.footer_sha256.clone()),
                })
                .collect()
        } else {
            Vec::new()
        };
        self.manifest.progress.construction = Some(ImportConstructionEvidence {
            configured_batch_rows: u64::try_from(self.manifest.limits.batch_rows)
                .unwrap_or(u64::MAX),
            accepted_chunks: progress.accepted_chunks,
            publication_committed: progress.publication_committed,
            application_io,
            publication_work,
            bulk_build: self
                .manifest
                .progress
                .construction
                .as_ref()
                .and_then(|evidence| evidence.bulk_build.clone()),
            input_rows: progress.evidence.input_rows,
            input_batches: progress.evidence.input_batches,
            immutable_artifacts: progress.evidence.immutable_artifacts,
            write_bytes: progress.evidence.write_bytes,
            write_operations: progress.evidence.write_operations,
            fsync_operations: progress.evidence.fsync_operations,
            cache_release_operations: progress.evidence.cache_release_operations,
            cache_release_unsupported_operations: progress
                .evidence
                .cache_release_unsupported_operations,
            cache_released_bytes: progress.evidence.cache_released_bytes,
            peak_cache_release_window_bytes: progress.evidence.peak_cache_release_window_bytes,
            peak_batch_rows: progress.evidence.peak_batch_rows,
            peak_batch_bytes: progress.evidence.peak_batch_bytes,
            source_provenance,
            transient_peak_allocated_bytes: progress
                .evidence
                .storage_transient_peak_total_allocated_bytes,
            construction_staging,
            construction_staging_transient_peak_allocated_bytes,
            external_partitions: progress.evidence.external_partitions,
            external_runs: progress.evidence.external_runs,
            external_run_bytes: progress.evidence.external_run_bytes,
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
        external: Option<external_source::ExternalSource>,
    ) -> Result<(), GfError> {
        self.journal.ensure_writable()?;
        // Only a session-owned source can be left behind by a failed checkpoint;
        // an external source is never ours to remove.
        let owned = external
            .is_none()
            .then(|| self.root.join("sources").join(&name));
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
            external,
            sha256: None,
        });
        if self
            .manifest
            .sources
            .last()
            .is_some_and(|source| source.external.is_some())
        {
            // A session an earlier version began cannot be read by that version
            // once it holds a source that stays where it is.
            self.manifest.format_version = FORMAT_VERSION;
        }
        self.manifest.phase = ImportPhase::Open;
        self.manifest.progress.bytes_accepted = total;
        self.manifest.progress.files_accepted += 1;
        self.manifest.progress.files_pending += 1;
        self.checkpoint_with_source_cleanup(owned.as_deref())
            .map(|_| ())
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

fn append_region_name(kind: BulkInputKind) -> &'static str {
    match kind {
        BulkInputKind::Node => "append_nodes",
        BulkInputKind::Edge => "append_edges",
    }
}

/// Stream one source's batches to `consume`. A Parquet source is read where it
/// is; the result is its whole-file SHA-256 when the pass read all of it.
fn for_each_source_batch(
    root: &Path,
    source: &SourceRecord,
    batch_rows: usize,
    mut consume: impl FnMut(Option<RecordBatch>) -> Result<(), GfError>,
) -> Result<Option<String>, GfError> {
    let tracker = graphforge_filesystem::FileCacheReleaseTracker::default();
    match source.kind {
        ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
            let path = root.join("sources").join(&source.name);
            let result = (|| {
                let file = File::open(path).map_err(storage)?;
                let reader = graphforge_filesystem::FileCacheReleasingReader::with_tracker(
                    file,
                    tracker.clone(),
                )
                .map_err(storage)?;
                consume_source_batches(
                    ArrowFileReader::try_new(BufReader::new(reader), None)
                        .map_err(storage)?
                        .map(|batch| batch.map_err(storage)),
                    &mut consume,
                )
            })();
            finish_source_cache_release(result, &tracker, "Arrow source")?;
            Ok(None)
        }
        ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
            let result = (|| {
                // A source registered before sources stayed in place was copied
                // into the session, which owns it: nothing to pin or digest.
                let Some(external) = source.external.as_ref() else {
                    let file =
                        File::open(root.join("sources").join(&source.name)).map_err(storage)?;
                    let chunk_reader = ImportChunkReader::new(file, tracker.clone(), None)?;
                    let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
                        .map_err(storage)?
                        .with_batch_size(batch_rows)
                        .build()
                        .map_err(storage)?;
                    consume_source_batches(
                        reader.map(|batch| {
                            canonicalize_parquet_batch(
                                source.kind.input_kind(),
                                &batch.map_err(storage)?,
                            )
                        }),
                        &mut consume,
                    )?;
                    return Ok(None);
                };
                let digest = external_source::SourceDigest::new(external.size);
                let file = external.open_observed(&digest)?;
                #[cfg(test)]
                external_source::pass_hook(&external.path, "opened", 0);
                let guard = file.try_clone().map_err(storage)?;
                let chunk_reader =
                    ImportChunkReader::new(file, tracker.clone(), Some(digest.clone()))?;
                // A read that fails because the file changed under it is that
                // change, not an I/O or format error.
                let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
                    .and_then(|builder| builder.with_batch_size(batch_rows).build())
                    .map_err(|error| external.reclassify(&guard, storage(error)))?;
                #[cfg(test)]
                let mut seen = 0_u64;
                consume_source_batches(
                    reader.map(|batch| {
                        #[cfg(test)]
                        {
                            external_source::pass_hook(&external.path, "batch", seen);
                            seen += 1;
                        }
                        // The source can change between any two batches.
                        external.check(&guard)?;
                        canonicalize_parquet_batch(
                            source.kind.input_kind(),
                            &batch.map_err(|error| external.reclassify(&guard, storage(error)))?,
                        )
                    }),
                    &mut consume,
                )?;
                let sha256 = digest.finish(external, &guard)?;
                external.check(&guard)?;
                Ok(Some(sha256))
            })();
            finish_source_cache_release(result, &tracker, "Parquet source")
        }
    }
}

/// Drain admitted earlier batches before a later decode error, while still
/// inside the source reader's cache-cleanup scope. None marks the drain point.
fn consume_source_batches(
    batches: impl Iterator<Item = Result<RecordBatch, GfError>>,
    consume: &mut impl FnMut(Option<RecordBatch>) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let mut batches = batches;
    loop {
        let batch = {
            let _region = RegionScope::named("source_read");
            batches.next()
        };
        let Some(batch) = batch else {
            break;
        };
        match batch {
            Ok(batch) => consume(Some(batch))?,
            Err(error) => {
                consume(None)?;
                return Err(error);
            }
        }
    }
    consume(None)
}

#[derive(Clone)]
struct ImportChunkReader {
    file: std::sync::Arc<File>,
    length: u64,
    tracker: graphforge_filesystem::FileCacheReleaseTracker,
    /// The digest an in-place source's reads feed; none for a session-owned copy.
    digest: Option<external_source::SourceDigest>,
}

impl ImportChunkReader {
    fn new(
        file: File,
        tracker: graphforge_filesystem::FileCacheReleaseTracker,
        digest: Option<external_source::SourceDigest>,
    ) -> Result<Self, GfError> {
        let length = file.metadata().map_err(storage)?.len();
        Ok(Self {
            file: std::sync::Arc::new(file),
            length,
            tracker,
            digest,
        })
    }
}

impl Length for ImportChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for ImportChunkReader {
    type T = BufReader<
        external_source::DigestingReader<graphforge_filesystem::FileCacheReleasingReader>,
    >;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let file = self.file.try_clone()?;
        let mut reader = graphforge_filesystem::FileCacheReleasingReader::with_tracker(
            file,
            self.tracker.clone(),
        )?;
        reader.seek(SeekFrom::Start(start))?;
        Ok(BufReader::with_capacity(
            external_source::PAGE_HEADER_BUFFER_BYTES,
            external_source::DigestingReader::new(reader, start, self.digest.clone()),
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let file = self.file.try_clone()?;
        let mut reader = graphforge_filesystem::FileCacheReleasingReader::with_tracker(
            file,
            self.tracker.clone(),
        )?;
        reader.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0_u8; length];
        reader.read_exact(&mut bytes)?;
        reader.finish()?;
        if let Some(digest) = &self.digest {
            digest.observe(start, &bytes);
        }
        Ok(Bytes::from(bytes))
    }
}

fn finish_source_cache_release<T>(
    primary: Result<T, GfError>,
    tracker: &graphforge_filesystem::FileCacheReleaseTracker,
    source_kind: &str,
) -> Result<T, GfError> {
    match (primary, tracker.check_error()) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(release)) => Err(storage(release)),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(release)) => Err(storage(format!(
            "{primary}; {source_kind} cache release also failed: {release}"
        ))),
    }
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

#[cfg(test)]
fn write_manifest(root: &Path, manifest: &SessionManifest) -> Result<(), GfError> {
    write_manifest_with_allocation(root, manifest, None)
}

fn write_manifest_with_allocation(
    root: &Path,
    manifest: &SessionManifest,
    allocation: Option<&graphforge_storage::StorageAllocationOperation>,
) -> Result<(), GfError> {
    journal::write_checkpoint(root, manifest, allocation, &mut false)
}

const fn supported_format(version: u32) -> bool {
    version >= OLDEST_FORMAT_VERSION && version <= FORMAT_VERSION
}

fn read_manifest(root: &Path) -> Result<SessionManifest, GfError> {
    let mut manifest: SessionManifest = serde_json::from_reader(BufReader::new(
        File::open(root.join(MANIFEST)).map_err(storage)?,
    ))
    .map_err(storage)?;
    if !supported_format(manifest.format_version) {
        return Err(validation("incompatible import manifest format"));
    }
    journal::replay(root, &mut manifest)?;
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

fn normalize_batch(
    graph: &GraphForge,
    operation: OperationId,
    kind: BulkInputKind,
    batch: &RecordBatch,
) -> Result<RecordBatch, GfError> {
    match kind {
        BulkInputKind::Node => graph.normalize_import_node_chunk(operation, batch),
        BulkInputKind::Edge => graph.normalize_import_edge_chunk(operation, batch),
    }
    .map_err(|error| validation(error.to_string()))
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
mod test_fixtures {
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryArray, StringArray};

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

    pub(super) fn nodes(values: &[Uuid]) -> RecordBatch {
        RecordBatch::try_new(
            bulk_node_input_schema(Vec::new()).unwrap(),
            vec![
                uuid_array(values),
                Arc::new(StringArray::from(vec!["Person"; values.len()])),
            ],
        )
        .unwrap()
    }

    pub(super) fn edges(edge: Uuid, source: Uuid, target: Uuid) -> RecordBatch {
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

    pub(super) fn fixture() -> (tempfile::TempDir, PathBuf, GraphForge) {
        let directory = tempfile::tempdir().unwrap();
        let project = directory.path().join("project");
        fs::create_dir(&project).unwrap();
        let graph = GraphForge::new(project.to_str()).unwrap();
        (directory, project, graph)
    }

    /// A project with one committed generation. An initial import runs on the
    /// bulk builder, so tests of the staged path (journal replay, chunk
    /// receipts, per-batch progress) import on top of this generation: an
    /// append stages chunk by chunk.
    pub(super) fn seeded_fixture() -> (tempfile::TempDir, PathBuf, GraphForge) {
        let (directory, project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        let seed = RecordBatch::try_new(
            bulk_node_input_schema(Vec::new()).unwrap(),
            vec![
                uuid_array(&[Uuid::now_v7()]),
                Arc::new(StringArray::from(vec!["Seed"])),
            ],
        )
        .unwrap();
        session.append_arrow(BulkInputKind::Node, &[seed]).unwrap();
        session.validate(&graph).unwrap();
        session.commit(&graph, None).unwrap();
        (directory, project, graph)
    }
}

#[cfg(all(test, feature = "portable"))]
mod bulk_tests;

#[cfg(all(test, feature = "portable"))]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;
    use parquet::arrow::ArrowWriter;

    use super::test_fixtures::{edges, fixture, nodes, seeded_fixture};
    use super::*;
    use crate::{bulk_edge_input_schema, bulk_node_input_schema};

    fn construction_root(graph: &GraphForge, session_uuid: Uuid) -> PathBuf {
        graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(session_uuid.simple().to_string())
    }

    #[test]
    fn default_import_has_no_optional_timing_samples_and_preserves_progress() {
        let (_directory, _project, graph) = fixture();
        let before = CALL_TIMING_SAMPLES.with(std::cell::Cell::get);
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        let ids = [Uuid::now_v7()];
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
            .unwrap();
        let prior = *graph.current_generation_uuid.lock().unwrap();
        let progress = session.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 1);
        assert!(session.operation_timings().is_none());
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            session.commit(&graph, Some(&cancelled)).unwrap_err().code(),
            "GF_CANCELLED"
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), prior);
        assert!(session.operation_timings().is_none());
        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 1);
        assert!(session.operation_timings().is_none());
        assert_eq!(CALL_TIMING_SAMPLES.with(std::cell::Cell::get), before);
    }

    #[test]
    fn operation_timings_carry_process_cpu_for_every_measured_call() {
        let _capture = graphforge_storage::lifecycle_io::CaptureScope::install();
        // Real import operations carry process CPU in ordinary receipts;
        // CPU/wall is effective cores, never an inferred serial fraction.
        let (_directory, _project, graph) = seeded_fixture();
        let mut session = graph
            .begin_import_session(
                OperationId(Uuid::now_v7()),
                ImportSessionLimits {
                    batch_rows: 1,
                    ..ImportSessionLimits::default()
                },
            )
            .unwrap();
        let ids = [Uuid::now_v7(), Uuid::now_v7()];
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&ids[..1]), nodes(&ids[1..])])
            .unwrap();
        session
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), ids[0], ids[1])],
            )
            .unwrap();
        session.validate(&graph).unwrap();
        let timing = session
            .operation_timings()
            .expect("requested import timings");

        for (name, call) in [
            ("begin", timing.begin),
            ("append", timing.append),
            ("seal", timing.seal),
        ] {
            assert!(call.calls > 0, "{name} should have been called");
            assert_eq!(
                call.cpu_unmeasured_calls, 0,
                "{name}: process CPU unavailable on this platform"
            );
            assert!(
                call.effective_cores().is_some(),
                "{name}: effective cores must be reported once CPU is measured"
            );
        }

        // An operation never invoked reports nothing rather than zero cores,
        // so a reader cannot mistake "not run" for "ran on no CPU".
        assert_eq!(timing.publish.calls, 0);
        assert_eq!(timing.publish.cpu_ns, 0);
        assert!(timing.publish.effective_cores().is_none());
    }

    #[test]
    fn operation_timings_are_scoped_non_durable_and_preserve_cancelled_commit() {
        let _capture = graphforge_storage::lifecycle_io::CaptureScope::install();
        let (_directory, _project, graph) = seeded_fixture();
        let mut session = graph
            .begin_import_session(
                OperationId(Uuid::now_v7()),
                ImportSessionLimits {
                    batch_rows: 1,
                    ..ImportSessionLimits::default()
                },
            )
            .unwrap();
        let ids = [Uuid::now_v7(), Uuid::now_v7()];
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&ids[..1]), nodes(&ids[1..])])
            .unwrap();
        session
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), ids[0], ids[1])],
            )
            .unwrap();
        let prior = *graph.current_generation_uuid.lock().unwrap();
        session.validate(&graph).unwrap();
        let timing = session
            .operation_timings()
            .expect("requested import timings");
        assert_eq!(
            (
                timing.begin.calls,
                timing.resume.calls,
                timing.append.calls,
                timing.seal.calls,
                timing.publish.calls
            ),
            (1, 0, 3, 1, 0)
        );
        for call in [timing.begin, timing.append, timing.seal] {
            assert_eq!(call.errors, 0);
        }
        session.validate(&graph).unwrap();
        let timing = session
            .operation_timings()
            .expect("requested import timings");
        assert_eq!(
            (
                timing.begin.calls,
                timing.resume.calls,
                timing.append.calls,
                timing.seal.calls
            ),
            (0, 1, 0, 1)
        );
        let manifest_path = session.root.join(MANIFEST);
        let persisted = fs::read(&manifest_path).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&persisted).unwrap();
        assert!(!value.to_string().contains("elapsed_ns"));
        assert!(!value.to_string().contains("operation_timings"));
        let session_uuid = session.session_uuid();
        drop(session);
        let mut session = graph.resume_import_session(session_uuid).unwrap();
        assert!(session.operation_timings().is_none());
        assert_eq!(fs::read(&manifest_path).unwrap(), persisted);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = session.commit(&graph, Some(&cancelled)).unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        let timing = session
            .operation_timings()
            .expect("requested import timings");
        assert_eq!(
            (
                timing.resume.calls,
                timing.publish.calls,
                timing.publish.errors
            ),
            (1, 1, 1)
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), prior);
        assert_eq!(session.status().0, ImportPhase::Validated);
        assert_eq!(fs::read(&manifest_path).unwrap(), persisted);
        session.commit(&graph, None).unwrap();
        let timing = session
            .operation_timings()
            .expect("requested import timings");
        assert_eq!(
            (
                timing.begin.calls,
                timing.resume.calls,
                timing.append.calls,
                timing.seal.calls,
                timing.publish.calls,
                timing.publish.errors
            ),
            (0, 1, 0, 0, 1, 0)
        );
        assert_eq!(graph.node_count("Person").unwrap(), 2);
        let result = graph
            .execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS n")
            .unwrap();
        assert_eq!(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(session.commit(&graph, None).is_err());
        assert_eq!(
            session.operation_timings(),
            Some(ImportOperationTimings::default())
        );
    }

    #[test]
    fn initial_import_publishes_label_encoding_and_reopen_avoids_legacy_scan() {
        const CHILD: &str = "GF_TEST_IMPORT_ENCODING_REOPEN";
        if std::env::var_os(CHILD).is_none() {
            // Storage read counters are process-global: isolate this admission
            // measurement from unrelated API tests running concurrently.
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "import_session::tests::initial_import_publishes_label_encoding_and_reopen_avoids_legacy_scan",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let (directory, project, graph) = fixture();
        let initial_nodes = [Uuid::now_v7(), Uuid::now_v7()];
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&initial_nodes)])
            .unwrap();
        session
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), initial_nodes[0], initial_nodes[1])],
            )
            .unwrap();
        session.validate(&graph).unwrap();
        session.commit(&graph, None).unwrap();
        let inventory = graphforge_storage::resolve_project_generation(&project)
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap();
        let marked = inventory
            .files
            .iter()
            .any(|entry| entry.relative_path == "topology/runtime_entity_label_encoding.json");
        drop(graph);
        let _io_capture = graphforge_storage::io_stats::CaptureScope::install();
        let before = graphforge_storage::io_stats::snapshot().expect("requested I/O statistics");
        let reopened = GraphForge::new(project.to_str()).unwrap();
        let reads = graphforge_storage::io_stats::snapshot().expect("requested I/O statistics");
        assert_eq!(
            reads.node_full_reads - before.node_full_reads,
            0,
            "committed encoding marker={marked}; reopen decoded {} node rows for legacy reconciliation",
            reads.node_full_rows
        );
        assert!(
            marked,
            "the encoding guarantee must be part of committed authority"
        );
        assert_eq!(reopened.node_count("Person").unwrap(), 2);
        let appended = reopened;
        let package = directory.path().join("marked.gfpb");
        appended
            .export_portable_v2(
                &crate::PortableV2ExportRequest {
                    selection: crate::PortableSelection::Current,
                    output_path: package.clone(),
                    representation: graphforge_storage::PortableV2Output::Bundle,
                    profile: graphforge_storage::PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits: graphforge_storage::PortableV2Limits::default(),
                },
                None,
                |_| {},
            )
            .unwrap();
        let imported = directory.path().join("imported");
        GraphForge::import_portable_v2(
            &imported,
            &crate::PortableV2ImportRequest {
                input: package,
                operation_id: OperationId(Uuid::now_v7()),
                limits: graphforge_storage::PortableV2Limits::default(),
            },
            None,
        )
        .unwrap();
        let _io_capture = graphforge_storage::io_stats::CaptureScope::install();
        let before = graphforge_storage::io_stats::snapshot().expect("requested I/O statistics");
        let portable = GraphForge::new(imported.to_str()).unwrap();
        assert_eq!(
            graphforge_storage::io_stats::snapshot()
                .expect("requested I/O statistics")
                .node_full_reads
                - before.node_full_reads,
            0
        );
        assert_eq!(portable.node_count("Person").unwrap(), 2);
    }

    #[test]
    fn ordinary_reopened_append_reconciles_preseal_progress() {
        check_reopened_append_progress(false);
    }

    #[test]
    fn resumed_reopened_append_reconciles_preseal_progress() {
        check_reopened_append_progress(true);
    }

    fn check_reopened_append_progress(resume: bool) {
        let (_directory, project, graph) = fixture();
        let ids = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
        let mut initial = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        initial
            .append_arrow(BulkInputKind::Node, &[nodes(&ids[..2])])
            .unwrap();
        initial
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), ids[0], ids[1])],
            )
            .unwrap();
        initial.validate(&graph).unwrap();
        initial.commit(&graph, None).unwrap();
        drop(initial);
        drop(graph);
        let graph = GraphForge::new(project.to_str()).unwrap();
        let mut append = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        append
            .append_arrow(BulkInputKind::Node, &[nodes(&ids[2..])])
            .unwrap();
        append
            .append_arrow(
                BulkInputKind::Edge,
                &[edges(Uuid::now_v7(), ids[1], ids[2])],
            )
            .unwrap();
        let progress = append.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 2);
        let phases = &progress.construction.as_ref().unwrap().application_io;
        phases.validate_for_qualification().unwrap();
        for phase in [
            graphforge_storage::StorageIoPhase::SealAuthentication,
            graphforge_storage::StorageIoPhase::ShapeConsumeReauthentication,
        ] {
            assert!(phases.phases[&phase].read_bytes > 0);
            assert!(phases.phases[&phase].read_calls > 0);
        }
        if resume {
            append.checkpoint().unwrap();
            let id = append.session_uuid();
            drop(append);
            append = graph.resume_import_session(id).unwrap();
            let resumed = append.validate(&graph).unwrap();
            resumed
                .construction
                .as_ref()
                .unwrap()
                .application_io
                .validate_for_qualification()
                .unwrap();
            assert_eq!(resumed.rows_accepted, 2);
        }
        append.commit(&graph, None).unwrap();
        drop(append);
        drop(graph);
        let graph = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 3);
        assert_eq!(
            graph
                .execute("MATCH ()-[r:KNOWS]->() RETURN r")
                .unwrap()
                .stats
                .rows_produced,
            2
        );
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
        let construction = progress.construction.as_ref().unwrap();
        assert!(
            construction.peak_cache_release_window_bytes
                <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
        #[cfg(target_os = "linux")]
        {
            assert!(construction.cache_release_operations > 0);
            assert!(construction.cache_released_bytes > 0);
            assert_eq!(construction.cache_release_unsupported_operations, 0);
        }
        #[cfg(not(target_os = "linux"))]
        assert!(construction.cache_release_unsupported_operations > 0);
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
        let committed = resumed.commit(&graph, None).unwrap();
        assert_ne!(committed, before);
        let expected_inventory = graphforge_storage::resolve_project_generation(
            &graph.resolved_generation.container_root(),
        )
        .unwrap()
        .graph_files_inventory()
        .unwrap();

        drop(graph);
        let reopened = GraphForge::new(project.to_str()).unwrap();
        let reopened_inventory = graphforge_storage::resolve_project_generation(
            reopened.resolved_generation.container_root(),
        )
        .unwrap()
        .graph_files_inventory()
        .unwrap();
        assert_eq!(reopened_inventory, expected_inventory);
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
    fn in_memory_import_reports_bounded_cache_release_evidence() {
        let graph = GraphForge::new(None).unwrap();
        let node_ids = [Uuid::now_v7(), Uuid::now_v7()];
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&node_ids)])
            .unwrap();
        let validated = session.validate(&graph).unwrap();
        let evidence = validated.construction.unwrap();
        assert!(
            evidence.peak_cache_release_window_bytes
                <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
        #[cfg(target_os = "linux")]
        {
            assert!(evidence.cache_release_operations > 0);
            assert!(evidence.cache_released_bytes > 0);
            assert_eq!(evidence.cache_release_unsupported_operations, 0);
        }
        #[cfg(not(target_os = "linux"))]
        assert!(evidence.cache_release_unsupported_operations > 0);

        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 2);
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
        assert!(
            session
                .register_parquet(BulkInputKind::Node, &corrupt)
                .is_err(),
            "registration reads the footer, so a non-Parquet file is refused up front"
        );
        // A file with Parquet's framing but an undecodable footer registers and
        // is refused when it is read.
        let framed = source_dir.path().join("framed.parquet");
        let mut bytes = b"PAR1".to_vec();
        bytes.extend_from_slice(&[0xff; 16]);
        bytes.extend_from_slice(&10_u32.to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        fs::write(&framed, bytes).unwrap();
        session
            .register_parquet(BulkInputKind::Node, &framed)
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
        // The bulk builder stages no chunk; it reports its passes instead.
        let construction = progress.construction.as_ref().unwrap();
        assert_eq!(construction.accepted_chunks, 0);
        let built = construction.bulk_build.as_ref().unwrap();
        assert_eq!((built.nodes, built.edges), (1, 0));
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
            let (_directory, project, graph) = seeded_fixture();
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
            assert!(receipt.construction_staging.logical_references > 0);
            assert!(receipt.construction_staging.logical_bytes > 0);
            assert!(receipt.construction_staging.physical_objects > 0);
            assert!(receipt.construction_staging.physical_logical_bytes > 0);
            assert!(receipt.construction_staging.allocated_bytes > 0);
            assert!(
                receipt.construction_staging_transient_peak_allocated_bytes
                    >= receipt.construction_staging.allocated_bytes
            );
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
            let reopened_receipt = reopened_progress.construction.as_ref().unwrap();
            assert!(
                reopened_receipt.construction_staging_transient_peak_allocated_bytes
                    >= reopened_receipt.construction_staging.allocated_bytes
            );
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
