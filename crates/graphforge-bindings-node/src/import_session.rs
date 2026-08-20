//! Thin Node bindings for durable staged graph-import sessions (#744 / #738).

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use graphforge_api::{
    BulkInputKind, CancellationToken, GfError, GraphImportSession as ApiSession, ImportPhase,
    ImportProgress, ImportSessionLimits, OperationId,
};
use napi::bindgen_prelude::{AbortSignal, BigInt, Buffer};
use napi_derive::napi;
use uuid::Uuid;

use crate::error::to_napi_err;
use crate::{Result, napi_validation};

fn phase_name(phase: ImportPhase) -> &'static str {
    match phase {
        ImportPhase::Open => "open",
        ImportPhase::Validated => "validated",
        ImportPhase::Committed => "committed",
        ImportPhase::Aborted => "aborted",
        ImportPhase::Quarantined => "quarantined",
    }
}

fn progress_output(progress: ImportProgress) -> ImportProgressOutput {
    ImportProgressOutput {
        rows_accepted: BigInt::from(progress.rows_accepted),
        rows_rejected: BigInt::from(progress.rows_rejected),
        bytes_accepted: BigInt::from(progress.bytes_accepted),
        files_accepted: BigInt::from(progress.files_accepted),
        files_pending: BigInt::from(progress.files_pending),
        elapsed_millis: BigInt::from(progress.elapsed_millis),
        peak_batch_rows: BigInt::from(progress.peak_batch_rows),
        io_concurrency_limit: BigInt::from(progress.io_concurrency_limit),
    }
}

fn parse_kind(kind: &str) -> Result<BulkInputKind> {
    match kind {
        "node" | "nodes" => Ok(BulkInputKind::Node),
        "edge" | "edges" => Ok(BulkInputKind::Edge),
        _ => Err(napi_validation("kind must be node or edge")),
    }
}

#[napi(object)]
pub struct ImportSessionLimitsInput {
    pub batch_rows: Option<u32>,
    pub max_source_bytes: Option<BigInt>,
    pub max_files: Option<BigInt>,
    pub max_rejected_rows: Option<BigInt>,
    pub io_concurrency: Option<u32>,
}

#[napi(object)]
pub struct ImportProgressOutput {
    pub rows_accepted: BigInt,
    pub rows_rejected: BigInt,
    pub bytes_accepted: BigInt,
    pub files_accepted: BigInt,
    pub files_pending: BigInt,
    pub elapsed_millis: BigInt,
    pub peak_batch_rows: BigInt,
    pub io_concurrency_limit: BigInt,
}

#[napi(object)]
pub struct ImportSessionStatusOutput {
    pub phase: String,
    pub progress: ImportProgressOutput,
}

fn parse_limits(input: Option<ImportSessionLimitsInput>) -> Result<ImportSessionLimits> {
    let defaults = ImportSessionLimits::default();
    let Some(input) = input else {
        return Ok(defaults);
    };
    Ok(ImportSessionLimits {
        batch_rows: input
            .batch_rows
            .map_or(defaults.batch_rows, |value| value as usize),
        max_source_bytes: match input.max_source_bytes {
            Some(value) => crate::node_u64(Some(value), "maxSourceBytes")?,
            None => defaults.max_source_bytes,
        },
        max_files: match input.max_files {
            Some(value) => crate::node_u64(Some(value), "maxFiles")?,
            None => defaults.max_files,
        },
        max_rejected_rows: match input.max_rejected_rows {
            Some(value) => crate::node_u64(Some(value), "maxRejectedRows")?,
            None => defaults.max_rejected_rows,
        },
        io_concurrency: input
            .io_concurrency
            .map_or(defaults.io_concurrency, |value| value as usize),
    })
}

/// Owned durable import-session handle. Contains no live rows.
#[napi(js_name = "GraphImportSession")]
pub struct GraphImportSession {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
    inner: Mutex<Option<ApiSession>>,
}

impl GraphImportSession {
    fn with_mut<R>(
        &self,
        f: impl FnOnce(&mut ApiSession) -> std::result::Result<R, GfError>,
    ) -> Result<R> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| to_napi_err(&GfError::Execution("import session lock poisoned".into())))?;
        let session = guard.as_mut().ok_or_else(|| {
            to_napi_err(&GfError::Lifecycle(
                "import session handle is closed".into(),
            ))
        })?;
        f(session).map_err(|error| to_napi_err(&error))
    }
}

#[napi]
impl GraphImportSession {
    /// Durable identifier used for resume.
    #[napi(getter)]
    pub fn session_uuid(&self) -> Result<String> {
        self.with_mut(|session| Ok(session.session_uuid().to_string()))
    }

    /// Current durable phase and counters.
    #[napi]
    pub fn status(&self) -> Result<ImportSessionStatusOutput> {
        let (phase, progress) = self.with_mut(|session| Ok(session.status()))?;
        Ok(ImportSessionStatusOutput {
            phase: phase_name(phase).to_owned(),
            progress: progress_output(progress),
        })
    }

    /// Append one Arrow IPC buffer without retaining live rows.
    #[napi]
    pub fn append_arrow(&self, kind: String, ipc: Buffer) -> Result<()> {
        let kind = parse_kind(&kind)?;
        let reader =
            StreamReader::try_new(std::io::Cursor::new(ipc.to_vec()), None).map_err(|error| {
                to_napi_err(&GfError::Validation(format!("invalid Arrow IPC: {error}")))
            })?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                to_napi_err(&GfError::Validation(format!("invalid Arrow IPC: {error}")))
            })?;
        self.with_mut(|session| session.append_arrow(kind, &batches))
    }

    /// Register a local Parquet source by copying it into durable ownership.
    #[napi]
    pub fn register_parquet(&self, kind: String, path: String) -> Result<()> {
        let kind = parse_kind(&kind)?;
        let path = PathBuf::from(path);
        self.with_mut(|session| session.register_parquet(kind, &path))
    }

    /// Persist counters and source ordering without publishing graph state.
    #[napi]
    pub fn checkpoint(&self) -> Result<ImportProgressOutput> {
        let progress = self.with_mut(ApiSession::checkpoint)?;
        Ok(progress_output(progress))
    }

    /// Validate and durably stage every source with optional cancellation.
    #[napi]
    pub fn validate(&self, signal: Option<AbortSignal>) -> Result<ImportProgressOutput> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        let cancellation = CancellationToken::new();
        if let Some(signal) = signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let graph = self
            .engine
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        let progress = self
            .with_mut(|session| session.validate_with_cancellation(&graph, Some(&cancellation)))?;
        Ok(progress_output(progress))
    }

    /// Publish the fully staged graph as one generation.
    #[napi]
    pub fn commit(&self, signal: Option<AbortSignal>) -> Result<String> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        let cancellation = CancellationToken::new();
        if let Some(signal) = signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let graph = self
            .engine
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        self.with_mut(|session| {
            session
                .commit(&graph, Some(&cancellation))
                .map(|uuid| uuid.to_string())
        })
    }

    /// Abort without changing CURRENT.
    #[napi]
    pub fn abort(&self) -> Result<ImportProgressOutput> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| to_napi_err(&GfError::Execution("import session lock poisoned".into())))?;
        let session = guard.take().ok_or_else(|| {
            to_napi_err(&GfError::Lifecycle(
                "import session handle is closed".into(),
            ))
        })?;
        let progress = session.abort().map_err(|error| to_napi_err(&error))?;
        Ok(progress_output(progress))
    }
}

pub(crate) fn begin_import_session(
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
    operation_uuid: String,
    limits: Option<ImportSessionLimitsInput>,
) -> Result<GraphImportSession> {
    if closed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(to_napi_err(&GfError::Lifecycle(
            "operation on a closed GraphForge instance".into(),
        )));
    }
    let operation = crate::canonical_operation_id(&operation_uuid)?;
    let limits = parse_limits(limits)?;
    let graph = engine
        .read()
        .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
    let session = graph
        .begin_import_session(operation, limits)
        .map_err(|error| to_napi_err(&error))?;
    Ok(GraphImportSession {
        engine: Arc::clone(&engine),
        closed,
        inner: Mutex::new(Some(session)),
    })
}

pub(crate) fn resume_import_session(
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
    session_uuid: String,
) -> Result<GraphImportSession> {
    if closed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(to_napi_err(&GfError::Lifecycle(
            "operation on a closed GraphForge instance".into(),
        )));
    }
    let uuid = Uuid::parse_str(&session_uuid).map_err(|_| {
        to_napi_err(&GfError::Validation(
            "sessionUuid must be a canonical UUID string".into(),
        ))
    })?;
    let graph = engine
        .read()
        .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
    let session = graph
        .resume_import_session(uuid)
        .map_err(|error| to_napi_err(&error))?;
    Ok(GraphImportSession {
        engine: Arc::clone(&engine),
        closed,
        inner: Mutex::new(Some(session)),
    })
}

pub(crate) fn cleanup_stale_import_sessions(
    graph: &graphforge_api::GraphForge,
    max_age_secs: BigInt,
) -> Result<BigInt> {
    let max_age_secs = crate::node_u64(Some(max_age_secs), "maxAgeSecs")?;
    let cleaned = graph
        .cleanup_stale_import_sessions(Duration::from_secs(max_age_secs))
        .map_err(|error| to_napi_err(&error))?;
    Ok(BigInt::from(cleaned))
}

#[allow(dead_code)]
fn _keep_operation_id(_: OperationId) {}
