//! Thin projections of Rust-owned immutable research Version contracts.
use crate::Task;
use crate::{Buffer, GraphForge, Result, canonical_operation_id, napi, result_to_ipc, to_napi_err};
use napi::JsValue;
use napi::bindgen_prelude::{FromNapiValue, JsObjectValue};

fn json_error(_error: serde_json::Error) -> napi::Error<String> {
    to_napi_err(&graphforge_api::GfError::Validation(
        "invalid research Version JSON contract".into(),
    ))
}

#[napi]
impl GraphForge {
    /// Freeze a complete-Project capture with owner-derived evidence.
    #[napi]
    pub fn prepare_research_version(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let request = serde_json::from_value(request).map_err(json_error)?;
        let result = self
            .open_guard()?
            .prepare_research_version(request)
            .map_err(|error| to_napi_err(&error))?;
        serde_json::to_value(result).map_err(json_error)
    }

    /// Commit an exact prepared request; preserve it unchanged for retries.
    #[napi]
    pub fn commit_research_version_operation(
        &self,
        operation: serde_json::Value,
        env: crate::Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<CommitResearchVersionTask>> {
        self.ensure_open()?;
        let operation = serde_json::from_value(operation).map_err(json_error)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = signal {
            if signal.get_named_property::<bool>("aborted").map_err(|_| {
                crate::type_error(env, "signal must expose a boolean aborted property")
            })? {
                cancellation.cancel();
            } else {
                let signal = native_abort_signal(env, &signal)
                    .map_err(|_| crate::type_error(env, "signal must be an AbortSignal"))?;
                let token = cancellation.clone();
                signal.on_abort(move || token.cancel());
            }
        }
        Ok(crate::AsyncTask::new(CommitResearchVersionTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation,
            cancellation,
        }))
    }

    /// Inspect frozen citation and content identity.
    #[napi]
    pub fn research_version(&self, version_uuid: String) -> Result<serde_json::Value> {
        let id = canonical_operation_id(&version_uuid)?.0;
        let result = self
            .open_guard()?
            .research_version(id)
            .map_err(|error| to_napi_err(&error))?;
        serde_json::to_value(result).map_err(json_error)
    }

    /// List immutable Version identities and labels as Arrow IPC.
    #[napi]
    pub fn list_research_versions(&self) -> Result<Buffer> {
        let result = self
            .open_guard()?
            .list_research_versions()
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect current retention dependencies and durable receipts.
    #[napi]
    pub fn research_version_retention(&self) -> Result<serde_json::Value> {
        let result = self
            .open_guard()?
            .research_version_retention()
            .map_err(|error| to_napi_err(&error))?;
        serde_json::to_value(result).map_err(json_error)
    }

    /// Run read-only native Cypher against exact retained historical research.
    #[napi]
    pub fn query_research_version(&self, version_uuid: String, query: String) -> Result<Buffer> {
        let id = canonical_operation_id(&version_uuid)?.0;
        let result = self
            .open_guard()?
            .open_research_version(id)
            .and_then(|view| view.execute(&query))
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Read exact historical Artifact metadata and availability as Arrow IPC.
    #[napi]
    pub fn research_version_artifact(
        &self,
        version_uuid: String,
        artifact_uuid: String,
    ) -> Result<Buffer> {
        let version = canonical_operation_id(&version_uuid)?.0;
        let artifact = canonical_operation_id(&artifact_uuid)?.0;
        let result = self
            .open_guard()?
            .open_research_version(version)
            .and_then(|view| view.artifact(artifact))
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }
    /// Read exact historical local Artifact bytes as Arrow IPC.
    #[napi]
    pub fn research_version_artifact_payload(
        &self,
        version_uuid: String,
        artifact_uuid: String,
    ) -> Result<Buffer> {
        let version = canonical_operation_id(&version_uuid)?.0;
        let artifact = canonical_operation_id(&artifact_uuid)?.0;
        let result = self
            .open_guard()?
            .open_research_version(version)
            .and_then(|view| view.artifact_payload(artifact))
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }
    /// Read frozen ontology metadata, independent of current composition.
    #[napi]
    pub fn research_version_ontology(
        &self,
        version_uuid: String,
    ) -> Result<crate::WorkspaceOntologyOutput> {
        let id = canonical_operation_id(&version_uuid)?.0;
        let result = self
            .open_guard()?
            .open_research_version(id)
            .and_then(|view| view.workspace_ontology())
            .map_err(|error| to_napi_err(&error))?;
        Ok(result.into())
    }

    /// Read frozen Project research metadata.
    #[napi]
    pub fn research_version_metadata(&self, version_uuid: String) -> Result<serde_json::Value> {
        let id = canonical_operation_id(&version_uuid)?.0;
        let result = self
            .open_guard()?
            .open_research_version(id)
            .and_then(|view| view.research_project_metadata())
            .map_err(|error| to_napi_err(&error))?;
        serde_json::to_value(result).map_err(json_error)
    }
}

pub struct CommitResearchVersionTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    operation: graphforge_api::ResearchOperation,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for CommitResearchVersionTask {
    type Output =
        std::result::Result<graphforge_api::ResearchOperationReceipt, graphforge_api::GfError>;
    type JsValue = ResearchOperationReceiptOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            graph.commit_research_version_operation(self.operation.clone(), &self.cancellation)
        })())
    }

    fn resolve(&mut self, env: crate::Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(|receipt| ResearchOperationReceiptOutput {
                operation_uuid: receipt.operation_uuid.to_string(),
                request_sha256: receipt.request_sha256.into_iter().map(u32::from).collect(),
                generation_uuid: receipt.generation_uuid.to_string(),
                version_uuid: receipt.version_uuid.map(|id| id.to_string()),
            })
            .map_err(|error| crate::to_napi_deferred_err(env, &error))
    }
}

/// Native durable receipt, with canonical JSON field names.
#[napi(object)]
pub struct ResearchOperationReceiptOutput {
    /// Durable operation identity.
    #[napi(js_name = "operation_uuid")]
    pub operation_uuid: String,
    /// Exact canonical request digest.
    #[napi(js_name = "request_sha256")]
    pub request_sha256: Vec<u32>,
    /// Generation published by this operation, not current at retry time.
    #[napi(js_name = "generation_uuid")]
    pub generation_uuid: String,
    /// New Version identity when this operation created one.
    #[napi(js_name = "version_uuid")]
    pub version_uuid: Option<String>,
}

// SAFETY: `signal` is a validated N-API Object belonging to the injected Env;
// AbortSignal conversion validates and installs its native callback state.
#[allow(unsafe_code)]
pub(crate) fn native_abort_signal(
    env: crate::Env,
    signal: &crate::Object<'_>,
) -> napi::Result<crate::AbortSignal> {
    unsafe { crate::AbortSignal::from_napi_value(env.raw(), signal.raw()) }
}
