//! Source and Artifact lifecycle Node bindings (#1349).

use crate::AbortSignal;
use crate::Arc;
use crate::AsyncTask;
use crate::Buffer;
use crate::Env;
use crate::GfError;
use crate::GraphForge;
use crate::Result;
use crate::RwLock;
use crate::Task;
use crate::WriteContext;
use crate::canonical_operation_id;
use crate::napi;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;

fn source_kind(value: &str) -> Result<graphforge_api::SourceKind> {
    match value {
        "manuscript" => Ok(graphforge_api::SourceKind::Manuscript),
        "edition" => Ok(graphforge_api::SourceKind::Edition),
        "pdf" => Ok(graphforge_api::SourceKind::Pdf),
        "epub" => Ok(graphforge_api::SourceKind::Epub),
        "web" => Ok(graphforge_api::SourceKind::Web),
        "photograph" => Ok(graphforge_api::SourceKind::Photograph),
        "recording" => Ok(graphforge_api::SourceKind::Recording),
        "database_export" => Ok(graphforge_api::SourceKind::DatabaseExport),
        "other" => Ok(graphforge_api::SourceKind::Other),
        _ => Err(to_napi_err(&GfError::Validation(
            "unknown source kind".into(),
        ))),
    }
}

fn artifact_kind(value: &str) -> Result<graphforge_api::ArtifactKind> {
    match value {
        "raw_scan" => Ok(graphforge_api::ArtifactKind::RawScan),
        "processed_scan" => Ok(graphforge_api::ArtifactKind::ProcessedScan),
        "ocr_text" => Ok(graphforge_api::ArtifactKind::OcrText),
        "normalized_text" => Ok(graphforge_api::ArtifactKind::NormalizedText),
        "passage_extract" => Ok(graphforge_api::ArtifactKind::PassageExtract),
        "other" => Ok(graphforge_api::ArtifactKind::Other),
        _ => Err(to_napi_err(&GfError::Validation(
            "unknown artifact kind".into(),
        ))),
    }
}

fn derivation_subject_kind(value: &str) -> Result<graphforge_api::DerivationSubjectKind> {
    match value {
        "source" => Ok(graphforge_api::DerivationSubjectKind::Source),
        "artifact" => Ok(graphforge_api::DerivationSubjectKind::Artifact),
        "node" => Ok(graphforge_api::DerivationSubjectKind::Node),
        "edge" => Ok(graphforge_api::DerivationSubjectKind::Edge),
        "assertion" => Ok(graphforge_api::DerivationSubjectKind::Assertion),
        "evidence_link" => Ok(graphforge_api::DerivationSubjectKind::EvidenceLink),
        "algorithm_run" => Ok(graphforge_api::DerivationSubjectKind::AlgorithmRun),
        _ => Err(to_napi_err(&GfError::Validation(
            "unknown derivation subject kind".into(),
        ))),
    }
}

fn lineage_direction(value: &str) -> Result<graphforge_api::LineageDirection> {
    match value {
        "backward" => Ok(graphforge_api::LineageDirection::Backward),
        "forward" => Ok(graphforge_api::LineageDirection::Forward),
        _ => Err(to_napi_err(&GfError::Validation(
            "unknown lineage direction".into(),
        ))),
    }
}

fn parse_fingerprint(value: Option<&Buffer>) -> Result<Option<[u8; 32]>> {
    match value {
        None => Ok(None),
        Some(buffer) => {
            let bytes = buffer.as_ref();
            if bytes.len() != 32 {
                return Err(to_napi_err(&GfError::Validation(
                    "fingerprint must be exactly 32 bytes".into(),
                )));
            }
            let mut fingerprint = [0_u8; 32];
            fingerprint.copy_from_slice(bytes);
            Ok(Some(fingerprint))
        }
    }
}

fn artifact_payload(
    input: &ArtifactPayloadInput,
) -> Result<graphforge_api::ArtifactPayloadRequest> {
    match input.kind.as_str() {
        "local_bytes" => {
            let bytes = input.bytes.as_ref().ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "local_bytes payload requires bytes".into(),
                ))
            })?;
            Ok(graphforge_api::ArtifactPayloadRequest::LocalBytes(
                bytes.to_vec(),
            ))
        }
        "external_reference" => {
            let uri = input.uri.clone().ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "external_reference payload requires uri".into(),
                ))
            })?;
            Ok(graphforge_api::ArtifactPayloadRequest::ExternalReference {
                uri,
                fingerprint: parse_fingerprint(input.fingerprint.as_ref())?,
            })
        }
        "absent" => Ok(graphforge_api::ArtifactPayloadRequest::Absent),
        _ => Err(to_napi_err(&GfError::Validation(
            "unknown artifact payload kind".into(),
        ))),
    }
}

fn write_context(operation_uuid: &str, actor_uuid: Option<&str>) -> Result<WriteContext> {
    Ok(WriteContext {
        operation_uuid: canonical_operation_id(operation_uuid)?,
        actor_uuid: actor_uuid
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0),
    })
}

/// One ordered derivation input for Artifact registration.
#[napi(object)]
pub struct DerivationInputJs {
    /// Input subject UUID.
    pub input_uuid: String,
    /// Closed input subject kind.
    pub input_kind: String,
}

/// Closed Artifact payload reference.
#[napi(object)]
pub struct ArtifactPayloadInput {
    /// `local_bytes`, `external_reference`, or `absent`.
    pub kind: String,
    /// Local bytes when `kind` is `local_bytes`.
    pub bytes: Option<Buffer>,
    /// External URI when `kind` is `external_reference`.
    pub uri: Option<String>,
    /// Optional 32-byte fingerprint when `kind` is `external_reference`.
    pub fingerprint: Option<Buffer>,
}

/// Thin Node request for one immutable Source registration.
#[napi(object)]
pub struct RegisterSourceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 Source identity.
    pub source_uuid: String,
    /// Bounded human-readable label.
    pub label: String,
    /// Closed source kind.
    pub source_kind: String,
    /// Optional stable external identity URI.
    pub identity_uri: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for one immutable Artifact registration.
#[napi(object)]
pub struct RegisterArtifactInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 Artifact identity.
    pub artifact_uuid: String,
    /// Parent Source identity.
    pub source_uuid: String,
    /// Closed artifact kind.
    pub artifact_kind: String,
    /// MIME-like media type label.
    pub media_type: String,
    /// Closed payload reference.
    pub payload: ArtifactPayloadInput,
    /// Ordered derivation inputs.
    pub derivation_inputs: Option<Vec<DerivationInputJs>>,
    /// Optional algorithm-run identity.
    pub run_uuid: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Source-list page request.
#[napi(object, object_to_js = false)]
pub struct ListSourcesInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Artifact-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListArtifactsInput {
    /// Optional Source UUID filter.
    pub source_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request to set the preferred Artifact for one Source.
#[napi(object)]
pub struct SetPreferredArtifactInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 preference-event identity.
    pub preference_event_uuid: String,
    /// Parent Source identity.
    pub source_uuid: String,
    /// Newly preferred Artifact identity.
    pub artifact_uuid: String,
    /// Bounded human-readable reason.
    pub reason: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin read-only replacement-impact request.
#[napi(object, object_to_js = false)]
pub struct ReplacementImpactInput {
    /// Parent Source identity.
    pub source_uuid: String,
    /// Proposed preferred Artifact identity.
    pub artifact_uuid: String,
}

/// Thin bounded lineage request.
#[napi(object, object_to_js = false)]
pub struct ResearchLineageInput {
    /// Closed subject kind.
    pub subject_kind: String,
    /// `backward` or `forward`.
    pub direction: String,
    /// Maximum hop depth (inclusive).
    pub max_depth: u32,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin retention-closure page request.
#[napi(object, object_to_js = false)]
pub struct RetentionDependencyClosureInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

pub struct RegisterSourceTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RegisterSourceRequest,
}

impl Task for RegisterSourceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.register_source(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct RegisterArtifactTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RegisterArtifactRequest,
}

impl Task for RegisterArtifactTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.register_artifact(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct SourceTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: uuid::Uuid,
}

impl Task for SourceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.source(self.request)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct ArtifactTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: uuid::Uuid,
}

impl Task for ArtifactTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.artifact(self.request)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct ListSourcesTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListSourcesRequest,
}

impl Task for ListSourcesTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_sources(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct ListArtifactsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListArtifactsRequest,
}

impl Task for ListArtifactsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_artifacts(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct SetPreferredArtifactTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::SetPreferredArtifactRequest,
}

impl Task for SetPreferredArtifactTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.set_preferred_artifact(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct ReplacementImpactTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ReplacementImpactRequest,
}

impl Task for ReplacementImpactTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.replacement_impact(self.request)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct ResearchLineageTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ResearchLineageRequest,
}

impl Task for ResearchLineageTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.research_lineage(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

pub struct RetentionDependencyClosureTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RetentionDependencyClosureRequest,
}

impl Task for RetentionDependencyClosureTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.retention_dependency_closure(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

fn page_request(
    limit: Option<u32>,
    after: Option<&str>,
    signal: Option<&AbortSignal>,
) -> Result<graphforge_api::PageRequest> {
    let after = after
        .map(graphforge_api::PageToken::parse)
        .transpose()
        .map_err(|error| to_napi_err(&error))?;
    let cancellation = graphforge_api::CancellationToken::new();
    if let Some(signal) = signal {
        let cancellation = cancellation.clone();
        signal.on_abort(move || cancellation.cancel());
    }
    Ok(graphforge_api::PageRequest {
        limit: limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
        after,
        cancellation: Some(cancellation),
    })
}

#[napi]
impl GraphForge {
    /// Atomically register one immutable research Source.
    #[napi]
    pub fn register_source(
        &self,
        request: RegisterSourceInput,
    ) -> Result<AsyncTask<RegisterSourceTask>> {
        self.ensure_open()?;
        let source_uuid = canonical_operation_id(&request.source_uuid)?.0;
        Ok(AsyncTask::new(RegisterSourceTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RegisterSourceRequest {
                context: write_context(&request.operation_uuid, request.actor_uuid.as_deref())?,
                source_uuid,
                label: request.label,
                source_kind: source_kind(&request.source_kind)?,
                identity_uri: request.identity_uri,
            },
        }))
    }

    /// Atomically register one immutable research Artifact.
    #[napi]
    pub fn register_artifact(
        &self,
        request: RegisterArtifactInput,
    ) -> Result<AsyncTask<RegisterArtifactTask>> {
        self.ensure_open()?;
        let artifact_uuid = canonical_operation_id(&request.artifact_uuid)?.0;
        let source_uuid = canonical_operation_id(&request.source_uuid)?.0;
        let derivation_inputs = request
            .derivation_inputs
            .unwrap_or_default()
            .into_iter()
            .map(|input| {
                Ok(graphforge_api::DerivationInput {
                    input_uuid: canonical_operation_id(&input.input_uuid)?.0,
                    input_kind: derivation_subject_kind(&input.input_kind)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let run_uuid = request
            .run_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        Ok(AsyncTask::new(RegisterArtifactTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RegisterArtifactRequest {
                context: write_context(&request.operation_uuid, request.actor_uuid.as_deref())?,
                artifact_uuid,
                source_uuid,
                artifact_kind: artifact_kind(&request.artifact_kind)?,
                media_type: request.media_type,
                payload: artifact_payload(&request.payload)?,
                derivation_inputs,
                run_uuid,
            },
        }))
    }

    /// Return one exact immutable Source as Arrow IPC.
    #[napi]
    pub fn source(&self, source_uuid: String) -> Result<AsyncTask<SourceTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(SourceTask {
            engine: Arc::clone(&self.inner),
            request: canonical_operation_id(&source_uuid)?.0,
        }))
    }

    /// Return one exact immutable Artifact as Arrow IPC.
    #[napi]
    pub fn artifact(&self, artifact_uuid: String) -> Result<AsyncTask<ArtifactTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ArtifactTask {
            engine: Arc::clone(&self.inner),
            request: canonical_operation_id(&artifact_uuid)?.0,
        }))
    }

    /// Return one deterministic generation-bound Source page as Arrow IPC.
    #[napi]
    pub fn list_sources(
        &self,
        request: Option<ListSourcesInput>,
    ) -> Result<AsyncTask<ListSourcesTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListSourcesInput {
            limit: None,
            after: None,
            signal: None,
        });
        Ok(AsyncTask::new(ListSourcesTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListSourcesRequest {
                page: page_request(
                    request.limit,
                    request.after.as_deref(),
                    request.signal.as_ref(),
                )?,
            },
        }))
    }

    /// Return one deterministic generation-bound Artifact page as Arrow IPC.
    #[napi]
    pub fn list_artifacts(
        &self,
        request: Option<ListArtifactsInput>,
    ) -> Result<AsyncTask<ListArtifactsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListArtifactsInput {
            source_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let source_uuid = request
            .source_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        Ok(AsyncTask::new(ListArtifactsTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListArtifactsRequest {
                source_uuid,
                page: page_request(
                    request.limit,
                    request.after.as_deref(),
                    request.signal.as_ref(),
                )?,
            },
        }))
    }

    /// Set the preferred Artifact for one Source.
    #[napi]
    pub fn set_preferred_artifact(
        &self,
        request: SetPreferredArtifactInput,
    ) -> Result<AsyncTask<SetPreferredArtifactTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(SetPreferredArtifactTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::SetPreferredArtifactRequest {
                context: write_context(&request.operation_uuid, request.actor_uuid.as_deref())?,
                preference_event_uuid: canonical_operation_id(&request.preference_event_uuid)?.0,
                source_uuid: canonical_operation_id(&request.source_uuid)?.0,
                artifact_uuid: canonical_operation_id(&request.artifact_uuid)?.0,
                reason: request.reason,
            },
        }))
    }

    /// Return read-only replacement impact for one Source preference change.
    #[napi]
    pub fn replacement_impact(
        &self,
        request: ReplacementImpactInput,
    ) -> Result<AsyncTask<ReplacementImpactTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ReplacementImpactTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ReplacementImpactRequest {
                source_uuid: canonical_operation_id(&request.source_uuid)?.0,
                artifact_uuid: canonical_operation_id(&request.artifact_uuid)?.0,
            },
        }))
    }

    /// Return one deterministic page of derivation edges for a research subject.
    #[napi]
    pub fn research_lineage(
        &self,
        subject_uuid: String,
        request: ResearchLineageInput,
    ) -> Result<AsyncTask<ResearchLineageTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ResearchLineageTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ResearchLineageRequest {
                subject_uuid: canonical_operation_id(&subject_uuid)?.0,
                subject_kind: derivation_subject_kind(&request.subject_kind)?,
                direction: lineage_direction(&request.direction)?,
                max_depth: request.max_depth,
                page: page_request(
                    request.limit,
                    request.after.as_deref(),
                    request.signal.as_ref(),
                )?,
            },
        }))
    }

    /// Return read-only retention dependency closure for one scope.
    #[napi]
    pub fn retention_dependency_closure(
        &self,
        scope_uuid: String,
        request: Option<RetentionDependencyClosureInput>,
    ) -> Result<AsyncTask<RetentionDependencyClosureTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(RetentionDependencyClosureInput {
            limit: None,
            after: None,
            signal: None,
        });
        Ok(AsyncTask::new(RetentionDependencyClosureTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RetentionDependencyClosureRequest {
                scope_uuid: canonical_operation_id(&scope_uuid)?.0,
                page: page_request(
                    request.limit,
                    request.after.as_deref(),
                    request.signal.as_ref(),
                )?,
            },
        }))
    }
}
