//! Assertions bindings and native task ownership.

use crate::AbortSignal;
use crate::Arc;
use crate::AssertionGraphRefInput;
use crate::AssertionGraphRole;
use crate::AsyncTask;
use crate::Buffer;
use crate::CapabilityId;
use crate::Env;
use crate::GfError;
use crate::GraphForge;
use crate::GraphObjectKind;
use crate::OperationId;
use crate::Result;
use crate::RwLock;
use crate::Task;
use crate::WriteContext;
use crate::canonical_operation_id;
use crate::napi;
use crate::napi_validation;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;

/// One thin assertion-to-graph reference.
#[napi(object)]
pub struct AssertionGraphRefInputJs {
    /// Canonical graph UUID.
    pub graph_uuid: String,
    /// `node` or `edge`.
    pub graph_kind: String,
    /// `subject`, `object`, or `context`.
    pub role: String,
    /// Contiguous position within the role.
    pub ordinal: u32,
}

/// Thin Node request for atomic assertion creation.
#[napi(object)]
pub struct CreateAssertionInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListAssertionsInput {
    /// Optional referenced graph UUID.
    pub graph_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin page request for one assertion's graph references.
#[napi(object, object_to_js = false)]
pub struct AssertionGraphRefsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for atomic confidence assessment.
#[napi(object)]
pub struct AssessConfidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 confidence identity.
    pub confidence_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// `explicit` or `conservative_min`.
    pub policy: String,
    /// Required only by `explicit`.
    pub value: Option<f64>,
    /// Requested immutable inputs for `conservative_min`.
    pub input_confidence_uuids: Option<Vec<String>>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin confidence-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListConfidenceAssessmentsInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin page request for one confidence input snapshot.
#[napi(object, object_to_js = false)]
pub struct ConfidenceInputsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable evidence link.
#[napi(object)]
pub struct AttachEvidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Caller-managed source UUID.
    pub source_uuid: String,
    /// `document`, `observation`, `graph_node`, or `graph_edge`.
    pub source_kind: String,
    /// `supports`, `contradicts`, or `context`.
    pub role: String,
    /// Optional finite metadata weight in `[0, 1]`.
    pub weight: Option<f64>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// One evidence row in an atomic assertion bundle.
#[napi(object)]
pub struct EvidenceInputJs {
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: String,
    /// Caller-managed source UUID.
    pub source_uuid: String,
    /// Closed evidence source kind.
    pub source_kind: String,
    /// Closed evidence role.
    pub role: String,
    /// Optional finite metadata weight.
    pub weight: Option<f64>,
}

/// Thin Node request for atomic assertion-plus-evidence creation.
#[napi(object)]
pub struct CreateAssertionWithEvidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Non-empty evidence bundle.
    pub evidence: Vec<EvidenceInputJs>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin evidence-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListEvidenceLinksInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Optional source UUID filter.
    pub source_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable epistemic reasoning record.
#[napi(object)]
pub struct RecordReasoningInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 reasoning identity.
    pub reasoning_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Closed reasoning kind.
    pub kind: String,
    /// Closed content media type.
    pub content_format: String,
    /// Exact UTF-8 content bytes.
    pub content: Buffer,
    /// Existing provenance event UUID.
    pub provenance_uuid: String,
    /// Optional prior reasoning record amended by this record.
    pub supersedes_reasoning_uuid: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin reasoning-history filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListReasoningInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one explicit assertion-status event.
#[napi(object)]
pub struct RecordAssertionStatusInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 event identity.
    pub status_event_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Closed explicit status.
    pub status: String,
    /// Existing producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional immutable confidence UUID.
    pub confidence_uuid: Option<String>,
    /// Optional immutable reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-status history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionStatusInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin atomic assertion-plus-first-status request.
#[napi(object)]
pub struct CreateAssertionWithStatusInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Caller-supplied UUIDv7 first-status identity.
    pub status_event_uuid: String,
    /// Explicit first status.
    pub status: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

pub(crate) fn assertion_status(value: &str) -> Result<graphforge_api::AssertionStatus> {
    match value {
        "hypothesis" => Ok(graphforge_api::AssertionStatus::Hypothesis),
        "supported" => Ok(graphforge_api::AssertionStatus::Supported),
        "refuted" => Ok(graphforge_api::AssertionStatus::Refuted),
        "disputed" => Ok(graphforge_api::AssertionStatus::Disputed),
        "retracted" => Ok(graphforge_api::AssertionStatus::Retracted),
        "superseded" => Ok(graphforge_api::AssertionStatus::Superseded),
        _ => Err(napi_validation("unknown assertion status")),
    }
}

pub(super) fn parse_capability_id(value: &str) -> Result<CapabilityId> {
    match value {
        "graph" => Ok(CapabilityId::Graph),
        "provenance" => Ok(CapabilityId::Provenance),
        "knowledge" => Ok(CapabilityId::Knowledge),
        "epistemic" => Ok(CapabilityId::Epistemic),
        "valid_time" => Ok(CapabilityId::ValidTime),
        _ => Err(to_napi_err(&GfError::Validation(format!(
            "unknown capability {value:?}"
        )))),
    }
}

fn assertion_graph_ref(value: AssertionGraphRefInputJs) -> Result<AssertionGraphRefInput> {
    let graph_uuid = canonical_operation_id(&value.graph_uuid)?.0;
    let graph_kind = match value.graph_kind.as_str() {
        "node" => GraphObjectKind::Node,
        "edge" => GraphObjectKind::Edge,
        _ => {
            return Err(to_napi_err(&GfError::Validation(
                "graphKind must be 'node' or 'edge'".into(),
            )));
        }
    };
    let role = match value.role.as_str() {
        "subject" => AssertionGraphRole::Subject,
        "object" => AssertionGraphRole::Object,
        "context" => AssertionGraphRole::Context,
        _ => {
            return Err(to_napi_err(&GfError::Validation(
                "role must be 'subject', 'object', or 'context'".into(),
            )));
        }
    };
    Ok(AssertionGraphRefInput {
        graph_uuid,
        graph_kind,
        role,
        ordinal: value.ordinal,
    })
}

fn evidence_input(value: EvidenceInputJs) -> Result<graphforge_api::EvidenceInput> {
    let source_kind = match value.source_kind.as_str() {
        "document" => graphforge_api::EvidenceSourceKind::Document,
        "observation" => graphforge_api::EvidenceSourceKind::Observation,
        "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
        "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
        "source" => graphforge_api::EvidenceSourceKind::Source,
        "artifact" => graphforge_api::EvidenceSourceKind::Artifact,
        _ => return Err(napi_validation("unknown evidence source kind")),
    };
    let role = match value.role.as_str() {
        "supports" => graphforge_api::EvidenceRole::Supports,
        "contradicts" => graphforge_api::EvidenceRole::Contradicts,
        "context" => graphforge_api::EvidenceRole::Context,
        _ => return Err(napi_validation("unknown evidence role")),
    };
    Ok(graphforge_api::EvidenceInput {
        evidence_uuid: canonical_operation_id(&value.evidence_uuid)?.0,
        source_uuid: canonical_operation_id(&value.source_uuid)?.0,
        source_kind,
        role,
        weight: value.weight,
    })
}

/// Worker task for one atomic assertion publication.
pub struct CreateAssertionTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::CreateAssertionRequest,
}

impl Task for CreateAssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion-plus-evidence publication.
pub struct CreateAssertionWithEvidenceTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::CreateAssertionWithEvidenceRequest,
}

impl Task for CreateAssertionWithEvidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion_with_evidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact assertion.
pub struct AssertionTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    assertion_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for AssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion(self.assertion_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion page.
pub struct ListAssertionsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListAssertionsRequest,
}

impl Task for ListAssertionsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertions(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion graph-reference page.
pub struct AssertionGraphRefsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    assertion_uuid: OperationId,
    page: graphforge_api::PageRequest,
}

impl Task for AssertionGraphRefsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion_graph_refs(self.assertion_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic confidence publication.
pub struct AssessConfidenceTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::AssessConfidenceRequest,
}

impl Task for AssessConfidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assess_confidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact confidence assessment.
pub struct ConfidenceAssessmentTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    confidence_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for ConfidenceAssessmentTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(
                &graph.confidence_assessment(
                    self.confidence_uuid.0,
                    Some(self.cancellation.clone()),
                )?,
            )
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one confidence-assessment page.
pub struct ListConfidenceAssessmentsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListConfidenceAssessmentsRequest,
}

impl Task for ListConfidenceAssessmentsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_confidence_assessments(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one confidence input page.
pub struct ConfidenceInputsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    confidence_uuid: OperationId,
    page: graphforge_api::PageRequest,
}

impl Task for ConfidenceInputsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.confidence_inputs(self.confidence_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic evidence publication.
pub struct AttachEvidenceTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::AttachEvidenceRequest,
}

impl Task for AttachEvidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.attach_evidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact evidence link.
pub struct EvidenceLinkTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    evidence_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for EvidenceLinkTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(
                &graph.evidence_link(self.evidence_uuid.0, Some(self.cancellation.clone()))?,
            )
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one evidence-link page.
pub struct ListEvidenceLinksTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListEvidenceLinksRequest,
}

impl Task for ListEvidenceLinksTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_evidence_links(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic reasoning publication.
pub struct RecordReasoningTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RecordReasoningRequest,
}

impl Task for RecordReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_reasoning(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact reasoning record.
pub struct ReasoningTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    reasoning_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for ReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.reasoning(self.reasoning_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one reasoning-history page.
pub struct ListReasoningTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListReasoningRequest,
}

impl Task for ListReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_reasoning(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion-plus-first-status publication.
pub struct CreateAssertionWithStatusTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::CreateAssertionWithStatusRequest,
}

impl Task for CreateAssertionWithStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion_with_status(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-status append.
pub struct RecordAssertionStatusTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RecordAssertionStatusRequest,
}

impl Task for RecordAssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_assertion_status(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion's current explicit status.
pub struct AssertionStatusTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    assertion_uuid: OperationId,
}

impl Task for AssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion_status(self.assertion_uuid.0)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-status history page.
pub struct ListAssertionStatusTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListAssertionStatusRequest,
}

impl Task for ListAssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_status(self.request.clone())?)
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
    /// Atomically create one immutable assertion.
    #[napi]
    pub fn create_assertion(
        &self,
        request: CreateAssertionInput,
    ) -> Result<AsyncTask<CreateAssertionTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                assertion_uuid,
                claim: request.claim,
                graph_refs,
            },
        }))
    }

    /// Atomically create one assertion and a non-empty evidence bundle.
    #[napi]
    pub fn create_assertion_with_evidence(
        &self,
        request: CreateAssertionWithEvidenceInput,
    ) -> Result<AsyncTask<CreateAssertionWithEvidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        let evidence = request
            .evidence
            .into_iter()
            .map(evidence_input)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionWithEvidenceTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::CreateAssertionWithEvidenceRequest {
                assertion: graphforge_api::CreateAssertionRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    assertion_uuid,
                    claim: request.claim,
                    graph_refs,
                },
                evidence,
            },
        }))
    }

    /// Atomically create one assertion and its first explicit status.
    #[napi]
    pub fn create_assertion_with_status(
        &self,
        request: CreateAssertionWithStatusInput,
    ) -> Result<AsyncTask<CreateAssertionWithStatusTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let status_event_uuid = canonical_operation_id(&request.status_event_uuid)?.0;
        let status = assertion_status(&request.status)?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionWithStatusTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::CreateAssertionWithStatusRequest {
                assertion: graphforge_api::CreateAssertionRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    assertion_uuid,
                    claim: request.claim,
                    graph_refs,
                },
                first_status: graphforge_api::FirstAssertionStatusInput {
                    status_event_uuid,
                    status,
                },
            },
        }))
    }

    /// Return one exact immutable assertion as Arrow IPC.
    #[napi]
    pub fn assertion(
        &self,
        assertion_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<AssertionTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AssertionTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic assertion page as Arrow IPC.
    #[napi]
    pub fn list_assertions(
        &self,
        request: Option<ListAssertionsInput>,
    ) -> Result<AsyncTask<ListAssertionsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionsInput {
            graph_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let graph_uuid = request
            .graph_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionsTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListAssertionsRequest {
                graph_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one assertion's graph references as Arrow IPC.
    #[napi]
    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: String,
        request: Option<AssertionGraphRefsInput>,
    ) -> Result<AsyncTask<AssertionGraphRefsTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        let request = request.unwrap_or(AssertionGraphRefsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AssertionGraphRefsTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
            page: graphforge_api::PageRequest {
                limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Atomically record one immutable confidence assessment.
    #[napi]
    pub fn assess_confidence(
        &self,
        request: AssessConfidenceInput,
    ) -> Result<AsyncTask<AssessConfidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let confidence_uuid = canonical_operation_id(&request.confidence_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let policy = match request.policy.as_str() {
            "explicit" => graphforge_api::ConfidencePolicyRequest::Explicit {
                value: request
                    .value
                    .ok_or_else(|| napi_validation("explicit requires value"))?,
            },
            "conservative_min" => {
                if request.value.is_some() {
                    return Err(napi_validation(
                        "conservative_min does not accept explicit value",
                    ));
                }
                let input_confidence_uuids = request
                    .input_confidence_uuids
                    .unwrap_or_default()
                    .iter()
                    .map(|value| canonical_operation_id(value).map(|id| id.0))
                    .collect::<Result<Vec<_>>>()?;
                graphforge_api::ConfidencePolicyRequest::ConservativeMin {
                    input_confidence_uuids,
                }
            }
            _ => return Err(napi_validation("unknown confidence policy")),
        };
        Ok(AsyncTask::new(AssessConfidenceTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                confidence_uuid,
                assertion_uuid,
                policy,
            },
        }))
    }

    /// Return one exact confidence assessment as Arrow IPC.
    #[napi]
    pub fn confidence_assessment(
        &self,
        confidence_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ConfidenceAssessmentTask>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(&confidence_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ConfidenceAssessmentTask {
            engine: Arc::clone(&self.inner),
            confidence_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic confidence-assessment page as Arrow IPC.
    #[napi]
    pub fn list_confidence_assessments(
        &self,
        request: Option<ListConfidenceAssessmentsInput>,
    ) -> Result<AsyncTask<ListConfidenceAssessmentsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListConfidenceAssessmentsInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListConfidenceAssessmentsTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListConfidenceAssessmentsRequest {
                assertion_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one assessment's immutable input snapshot as Arrow IPC.
    #[napi]
    pub fn confidence_inputs(
        &self,
        confidence_uuid: String,
        request: Option<ConfidenceInputsInput>,
    ) -> Result<AsyncTask<ConfidenceInputsTask>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(&confidence_uuid)?;
        let request = request.unwrap_or(ConfidenceInputsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ConfidenceInputsTask {
            engine: Arc::clone(&self.inner),
            confidence_uuid,
            page: graphforge_api::PageRequest {
                limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Atomically attach one immutable evidence link.
    #[napi]
    pub fn attach_evidence(
        &self,
        request: AttachEvidenceInput,
    ) -> Result<AsyncTask<AttachEvidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let evidence_uuid = canonical_operation_id(&request.evidence_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let source_uuid = canonical_operation_id(&request.source_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let source_kind = match request.source_kind.as_str() {
            "document" => graphforge_api::EvidenceSourceKind::Document,
            "observation" => graphforge_api::EvidenceSourceKind::Observation,
            "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
            "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
            "source" => graphforge_api::EvidenceSourceKind::Source,
            "artifact" => graphforge_api::EvidenceSourceKind::Artifact,
            _ => return Err(napi_validation("unknown evidence source kind")),
        };
        let role = match request.role.as_str() {
            "supports" => graphforge_api::EvidenceRole::Supports,
            "contradicts" => graphforge_api::EvidenceRole::Contradicts,
            "context" => graphforge_api::EvidenceRole::Context,
            _ => return Err(napi_validation("unknown evidence role")),
        };
        Ok(AsyncTask::new(AttachEvidenceTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::AttachEvidenceRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                evidence_uuid,
                assertion_uuid,
                source_uuid,
                source_kind,
                role,
                weight: request.weight,
            },
        }))
    }

    /// Return one exact immutable evidence link as Arrow IPC.
    #[napi]
    pub fn evidence_link(
        &self,
        evidence_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<EvidenceLinkTask>> {
        self.ensure_open()?;
        let evidence_uuid = canonical_operation_id(&evidence_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(EvidenceLinkTask {
            engine: Arc::clone(&self.inner),
            evidence_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic evidence-link page as Arrow IPC.
    #[napi]
    pub fn list_evidence_links(
        &self,
        request: Option<ListEvidenceLinksInput>,
    ) -> Result<AsyncTask<ListEvidenceLinksTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListEvidenceLinksInput {
            assertion_uuid: None,
            source_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let source_uuid = request
            .source_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListEvidenceLinksTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListEvidenceLinksRequest {
                assertion_uuid,
                source_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Atomically append one immutable epistemic reasoning record.
    #[napi]
    pub fn record_reasoning(
        &self,
        request: RecordReasoningInput,
    ) -> Result<AsyncTask<RecordReasoningTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let reasoning_uuid = canonical_operation_id(&request.reasoning_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let provenance_uuid = canonical_operation_id(&request.provenance_uuid)?.0;
        let supersedes_reasoning_uuid = request
            .supersedes_reasoning_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let kind = match request.kind.as_str() {
            "evidence_interpretation" => graphforge_api::ReasoningKind::EvidenceInterpretation,
            "logical_inference" => graphforge_api::ReasoningKind::LogicalInference,
            "methodological_note" => graphforge_api::ReasoningKind::MethodologicalNote,
            "decision_rationale" => graphforge_api::ReasoningKind::DecisionRationale,
            _ => return Err(napi_validation("unknown reasoning kind")),
        };
        let content_format = match request.content_format.as_str() {
            "text/plain" => graphforge_api::ReasoningContentFormat::TextPlain,
            "text/markdown" => graphforge_api::ReasoningContentFormat::TextMarkdown,
            "application/json" => graphforge_api::ReasoningContentFormat::ApplicationJson,
            _ => return Err(napi_validation("unknown reasoning content format")),
        };
        Ok(AsyncTask::new(RecordReasoningTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                reasoning_uuid,
                assertion_uuid,
                kind,
                content_format,
                content: request.content.to_vec(),
                supersedes_reasoning_uuid,
                provenance_uuid,
            },
        }))
    }

    /// Return one exact immutable reasoning record as Arrow IPC.
    #[napi]
    pub fn reasoning(
        &self,
        reasoning_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ReasoningTask>> {
        self.ensure_open()?;
        let reasoning_uuid = canonical_operation_id(&reasoning_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ReasoningTask {
            engine: Arc::clone(&self.inner),
            reasoning_uuid,
            cancellation,
        }))
    }

    /// Return deterministic immutable reasoning history as Arrow IPC.
    #[napi]
    pub fn list_reasoning(
        &self,
        request: Option<ListReasoningInput>,
    ) -> Result<AsyncTask<ListReasoningTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListReasoningInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListReasoningTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListReasoningRequest {
                assertion_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Append one explicit assertion-status event.
    #[napi]
    pub fn record_assertion_status(
        &self,
        request: RecordAssertionStatusInput,
    ) -> Result<AsyncTask<RecordAssertionStatusTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let status_event_uuid = canonical_operation_id(&request.status_event_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let provenance_uuid = canonical_operation_id(&request.provenance_uuid)?.0;
        let confidence_uuid = request
            .confidence_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let reasoning_uuid = request
            .reasoning_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        Ok(AsyncTask::new(RecordAssertionStatusTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RecordAssertionStatusRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                status_event_uuid,
                assertion_uuid,
                status: assertion_status(&request.status)?,
                confidence_uuid,
                reasoning_uuid,
                provenance_uuid,
            },
        }))
    }

    /// Return the current explicit status, or an empty Arrow table when statusless.
    #[napi]
    pub fn assertion_status(
        &self,
        assertion_uuid: String,
    ) -> Result<AsyncTask<AssertionStatusTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        Ok(AsyncTask::new(AssertionStatusTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
        }))
    }

    /// Return deterministic immutable assertion-status history as Arrow IPC.
    #[napi]
    pub fn list_assertion_status(
        &self,
        request: Option<ListAssertionStatusInput>,
    ) -> Result<AsyncTask<ListAssertionStatusTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionStatusInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionStatusTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListAssertionStatusRequest {
                assertion_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }
}
