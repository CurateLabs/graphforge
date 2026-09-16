//! Recorded bindings and native task ownership.

use crate::AbortSignal;
use crate::Arc;
use crate::AsyncTask;
use crate::AttachResolvedRunRequest;
use crate::Buffer;
use crate::ClassInstance;
use crate::Env;
use crate::GfError;
use crate::GraphForge;
use crate::InvocationDescriptorHandle;
use crate::OperationId;
use crate::ResolvedAttachmentOutcome;
use crate::ResolvedBeliefProjection;
use crate::ResolvedBeliefProjectionHandle;
use crate::ResolvedRecordedAlgorithmRequest;
use crate::Result;
use crate::RwLock;
use crate::Task;
use crate::WriteContext;
use crate::cancelled_error;
use crate::canonical_operation_id;
use crate::napi;
use crate::parse_algorithm_id;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;

/// Thin Node request for one recorded algorithm invocation.
#[napi(object, object_to_js = false)]
pub struct RecordedAlgorithmInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: String,
    /// Opaque Rust-owned neutral descriptor.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// First-time recorded dispatch output.
#[napi(object)]
pub struct RecordedAlgorithmOutput {
    /// Durable run UUID.
    pub run_uuid: String,
    /// Canonical Arrow IPC result.
    pub result: Buffer,
}

/// Execute a Bazel-migration0 descriptor against a resolved projection and attach epistemic context.
#[napi(object, object_to_js = false)]
pub struct ResolvedRecordedAlgorithmInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: String,
    /// Caller-supplied UUIDv7 attachment identity.
    pub attachment_uuid: String,
    /// Opaque Rust-owned neutral descriptor.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Recorded result plus the separately observable attachment outcome.
#[napi(object)]
pub struct ResolvedRecordedAlgorithmOutput {
    /// Durable knowledge run UUID.
    pub run_uuid: String,
    /// Canonical knowledge result as Arrow IPC.
    pub result: Buffer,
    /// Stable epistemic attachment UUID.
    pub attachment_uuid: String,
    /// `attached` or `attachment_failed`.
    pub attachment_state: String,
    /// Attached row as Arrow IPC when publication succeeded.
    pub attachment: Option<Buffer>,
    /// Stable public failure code when publication failed.
    pub attachment_error_code: Option<String>,
}

/// Retry only the epistemic attachment for an already completed knowledge run.
#[napi(object, object_to_js = false)]
pub struct AttachResolvedRunInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Stable attachment retry UUID.
    pub attachment_uuid: String,
    /// Existing completed knowledge run UUID.
    pub run_uuid: String,
    /// Exact descriptor used by the completed run.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional abort signal. Settles JavaScript scheduling early but cannot
    /// cancel or roll back an attachment publication that has already started.
    pub signal: Option<AbortSignal>,
}

/// Thin algorithm-run list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListAlgorithmRunsInput {
    /// Optional exact `verb.name` algorithm ID.
    pub algorithm: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin algorithm-run event page request.
#[napi(object, object_to_js = false)]
pub struct AlgorithmRunEventsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Rust-side transport used to materialize the public napi output on the main thread.
pub struct ResolvedRecordedOutputData {
    run_uuid: String,
    result: Vec<u8>,
    attachment_uuid: String,
    attachment_state: String,
    attachment: Option<Vec<u8>>,
    attachment_error_code: Option<String>,
}

/// Worker task for one resolved recorded dispatch and its attachment outcome.
pub struct ResolvedRecordedAlgorithmTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    projection: Arc<ResolvedBeliefProjection>,
    request: ResolvedRecordedAlgorithmRequest,
}

impl Task for ResolvedRecordedAlgorithmTask {
    type Output = std::result::Result<ResolvedRecordedOutputData, GfError>;
    type JsValue = ResolvedRecordedAlgorithmOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let attachment_uuid = self.request.attachment_uuid.to_string();
            let resolved =
                graph.invoke_resolved_recorded(&self.projection, self.request.clone())?;
            let (attachment_state, attachment, attachment_error_code) = match resolved.attachment {
                ResolvedAttachmentOutcome::Attached(result) => {
                    ("attached".to_owned(), Some(result_to_ipc(&result)?), None)
                }
                ResolvedAttachmentOutcome::Failed { error_code, .. } => {
                    ("attachment_failed".to_owned(), None, Some(error_code))
                }
            };
            Ok(ResolvedRecordedOutputData {
                run_uuid: resolved.recorded.run_uuid.to_string(),
                result: result_to_ipc(&resolved.recorded.result)?,
                attachment_uuid,
                attachment_state,
                attachment,
                attachment_error_code,
            })
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedRecordedAlgorithmOutput> {
        output
            .map(|value| ResolvedRecordedAlgorithmOutput {
                run_uuid: value.run_uuid,
                result: Buffer::from(value.result),
                attachment_uuid: value.attachment_uuid,
                attachment_state: value.attachment_state,
                attachment: value.attachment.map(Buffer::from),
                attachment_error_code: value.attachment_error_code,
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact attachment retry without algorithm redispatch.
pub struct AttachResolvedRunTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    projection: Arc<ResolvedBeliefProjection>,
    request: AttachResolvedRunRequest,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for AttachResolvedRunTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.attach_resolved_run(&self.projection, self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one recorded algorithm dispatch.
pub struct RecordedAlgorithmTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RecordedAlgorithmRequest,
}

impl Task for RecordedAlgorithmTask {
    type Output = std::result::Result<(String, Vec<u8>), GfError>;
    type JsValue = RecordedAlgorithmOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let recorded = graph.invoke_recorded(self.request.clone())?;
            Ok((
                recorded.run_uuid.to_string(),
                result_to_ipc(&recorded.result)?,
            ))
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<RecordedAlgorithmOutput> {
        output
            .map(|(run_uuid, result)| RecordedAlgorithmOutput {
                run_uuid,
                result: Buffer::from(result),
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact algorithm-run identity.
pub struct AlgorithmRunTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    run_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for AlgorithmRunTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.algorithm_run(self.run_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one algorithm-run identity page.
pub struct ListAlgorithmRunsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListAlgorithmRunsRequest,
}

impl Task for ListAlgorithmRunsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_algorithm_runs(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one algorithm-run lifecycle page.
pub struct AlgorithmRunEventsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    run_uuid: OperationId,
    page: graphforge_api::PageRequest,
}

impl Task for AlgorithmRunEventsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.algorithm_run_events(self.run_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

#[napi]
impl GraphForge {
    /// Execute one recorded descriptor on a resolved projection, then attach it.
    #[napi(ts_return_type = "Promise<ResolvedRecordedAlgorithmOutput>")]
    pub fn invoke_resolved_recorded(
        &self,
        projection: ClassInstance<'_, ResolvedBeliefProjectionHandle>,
        request: ResolvedRecordedAlgorithmInput<'_>,
    ) -> Result<AsyncTask<ResolvedRecordedAlgorithmTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let attachment_uuid = canonical_operation_id(&request.attachment_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolvedRecordedAlgorithmTask {
            engine: Arc::clone(&self.inner),
            projection: Arc::clone(&projection.inner),
            request: ResolvedRecordedAlgorithmRequest {
                recorded: graphforge_api::RecordedAlgorithmRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    run_uuid,
                    descriptor: request.descriptor.inner.clone(),
                    cancellation: Some(cancellation),
                },
                attachment_uuid,
            },
        }))
    }

    /// Retry only the attachment for an already-completed resolved run.
    ///
    /// Cancellation is cooperative before publication starts; an already-started
    /// durable epistemic publication still runs to its atomic outcome.
    #[napi(ts_return_type = "Promise<Buffer>")]
    pub fn attach_resolved_run(
        &self,
        projection: ClassInstance<'_, ResolvedBeliefProjectionHandle>,
        request: AttachResolvedRunInput<'_>,
    ) -> Result<AsyncTask<AttachResolvedRunTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let attachment_uuid = canonical_operation_id(&request.attachment_uuid)?.0;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AttachResolvedRunTask {
            engine: Arc::clone(&self.inner),
            projection: Arc::clone(&projection.inner),
            request: AttachResolvedRunRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                attachment_uuid,
                run_uuid,
                descriptor: request.descriptor.inner.clone(),
            },
            cancellation,
        }))
    }

    /// Durably record a lifecycle around the unchanged descriptor dispatch.
    #[napi(ts_return_type = "Promise<RecordedAlgorithmOutput>")]
    pub fn invoke_recorded(
        &self,
        request: RecordedAlgorithmInput<'_>,
    ) -> Result<AsyncTask<RecordedAlgorithmTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(RecordedAlgorithmTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RecordedAlgorithmRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                run_uuid,
                descriptor: request.descriptor.inner.clone(),
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Return one immutable algorithm-run identity as Arrow IPC.
    #[napi]
    pub fn algorithm_run(
        &self,
        run_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<AlgorithmRunTask>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(&run_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AlgorithmRunTask {
            engine: Arc::clone(&self.inner),
            run_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic generation-bound run page as Arrow IPC.
    #[napi]
    pub fn list_algorithm_runs(
        &self,
        request: Option<ListAlgorithmRunsInput>,
    ) -> Result<AsyncTask<ListAlgorithmRunsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAlgorithmRunsInput {
            algorithm: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let algorithm = request
            .algorithm
            .as_deref()
            .map(parse_algorithm_id)
            .transpose()?;
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        Ok(AsyncTask::new(ListAlgorithmRunsTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListAlgorithmRunsRequest {
                algorithm,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one deterministic generation-bound lifecycle page as Arrow IPC.
    #[napi]
    pub fn algorithm_run_events(
        &self,
        run_uuid: String,
        request: Option<AlgorithmRunEventsInput>,
    ) -> Result<AsyncTask<AlgorithmRunEventsTask>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(&run_uuid)?;
        let request = request.unwrap_or(AlgorithmRunEventsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        Ok(AsyncTask::new(AlgorithmRunEventsTask {
            engine: Arc::clone(&self.inner),
            run_uuid,
            page: graphforge_api::PageRequest {
                limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }
}
