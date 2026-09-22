//! Async transport for native Branch publication and immutable Arrow reads.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
use graphforge_api::CancellationToken;
enum Mutation {
    Update(graphforge_api::UpdateResearchBranchRequest),
    Claim(graphforge_api::ChangeResearchBranchClaimRequest),
    Create(graphforge_api::CreateResearchBranchRequest),
    Execute(graphforge_api::ExecuteResearchBranchRequest),
    Restore(graphforge_api::RestoreResearchBranchRequest),
    Ontology(graphforge_api::ChangeResearchBranchOntologyRequest),
    Reference(graphforge_api::ReferenceResearchBranchRequest),
    Bring(graphforge_api::BringResearchBranchRequest),
    SuppressAssertion(graphforge_api::SuppressResearchBranchAssertionRequest),
}
#[napi]
impl GraphForge {
    /// Inspect the permanent exact-membership creation selector as Arrow IPC.
    #[napi]
    pub fn research_branch_selection(
        &self,
        branch_uuid: String,
    ) -> Result<crate::AsyncTask<BranchReadTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(BranchReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            branch: crate::canonical_operation_id(&branch_uuid)?.0,
            operation: Read::Selection,
        }))
    }

    /// Execute the native ChangeResearchBranchClaimRequest contract.
    #[napi]
    pub fn change_research_branch_claim(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Claim(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Publish an explicitly reviewed native upstream update.
    #[napi]
    pub fn update_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid upstream update JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Update(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native CreateResearchBranchRequest contract.
    #[napi]
    pub fn create_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Create(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ExecuteResearchBranchRequest contract.
    #[napi]
    pub fn execute_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Execute(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native RestoreResearchBranchRequest contract.
    #[napi]
    pub fn restore_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Restore(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ChangeResearchBranchOntologyRequest contract.
    #[napi]
    pub fn change_research_branch_ontology(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Ontology(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ReferenceResearchBranchRequest contract.
    #[napi]
    pub fn reference_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Reference(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native BringResearchBranchRequest contract.
    #[napi]
    pub fn bring_research_branch(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::Bring(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native SuppressResearchBranchAssertionRequest contract.
    #[napi]
    pub fn suppress_research_branch_assertion(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<BranchMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Branch JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(BranchMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::SuppressAssertion(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Inspect immutable genealogy and exact opened current Version.
    #[napi]
    pub fn research_branch(&self, branch_uuid: String) -> Result<crate::AsyncTask<BranchInfoTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(BranchInfoTask {
            engine: std::sync::Arc::clone(&self.inner),
            branch: crate::canonical_operation_id(&branch_uuid)?.0,
        }))
    }
    /// Read exact native Branch state as Arrow IPC.
    #[napi]
    pub fn research_branch_fields(
        &self,
        branch_uuid: String,
    ) -> Result<crate::AsyncTask<BranchReadTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(BranchReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            branch: crate::canonical_operation_id(&branch_uuid)?.0,
            operation: Read::Fields,
        }))
    }
    /// Read exact native Branch state as Arrow IPC.
    #[napi]
    pub fn research_branch_references(
        &self,
        branch_uuid: String,
    ) -> Result<crate::AsyncTask<BranchReadTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(BranchReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            branch: crate::canonical_operation_id(&branch_uuid)?.0,
            operation: Read::References,
        }))
    }
    /// Read exact native Branch state as Arrow IPC.
    #[napi]
    pub fn query_research_branch(
        &self,
        branch_uuid: String,
        query: String,
    ) -> Result<crate::AsyncTask<BranchReadTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(BranchReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            branch: crate::canonical_operation_id(&branch_uuid)?.0,
            operation: Read::Query(query),
        }))
    }
}
pub struct BranchMutationTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    operation: Mutation,
    cancellation: CancellationToken,
}
impl Task for BranchMutationTask {
    type Output = std::result::Result<serde_json::Value, graphforge_api::GfError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let receipt = match &self.operation {
                Mutation::Update(request) => {
                    graph.update_research_branch(request, &self.cancellation)
                }
                Mutation::Claim(request) => {
                    graph.change_research_branch_claim(request, &self.cancellation)
                }
                Mutation::Create(request) => {
                    graph.create_research_branch(request, &self.cancellation)
                }
                Mutation::Execute(request) => {
                    graph.execute_research_branch(request, &self.cancellation)
                }
                Mutation::Restore(request) => {
                    graph.restore_research_branch(request, &self.cancellation)
                }
                Mutation::Ontology(request) => {
                    graph.change_research_branch_ontology(request, &self.cancellation)
                }
                Mutation::Reference(request) => {
                    graph.reference_research_branch(request, &self.cancellation)
                }
                Mutation::Bring(request) => {
                    graph.bring_research_branch(request, &self.cancellation)
                }
                Mutation::SuppressAssertion(request) => {
                    graph.suppress_research_branch_assertion(request, &self.cancellation)
                }
            }?;
            serde_json::to_value(receipt).map_err(|_| {
                graphforge_api::GfError::Execution("Branch receipt serialization failed".into())
            })
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|e| crate::to_napi_deferred_err(env, &e))?)
    }
}
pub struct BranchInfoTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    branch: uuid::Uuid,
}
impl Task for BranchInfoTask {
    type Output = std::result::Result<serde_json::Value, graphforge_api::GfError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let view = graph.open_research_branch(self.branch)?;
            Ok(serde_json::json!({"record": view.record(), "version_uuid": view.version_uuid()}))
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|e| crate::to_napi_deferred_err(env, &e))?)
    }
}
enum Read {
    Selection,
    Fields,
    References,
    Query(String),
}
pub struct BranchReadTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    branch: uuid::Uuid,
    operation: Read,
}
impl Task for BranchReadTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            if matches!(self.operation, Read::Selection) {
                return crate::result_to_ipc(&graph.research_branch_selection(self.branch)?);
            }
            let view = graph.open_research_branch(self.branch)?;
            let result = match &self.operation {
                Read::Selection => unreachable!("handled above"),
                Read::Fields => view.fields(),
                Read::References => view.references(),
                Read::Query(query) => view.graph().execute(query),
            }?;
            crate::result_to_ipc(&result)
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}
