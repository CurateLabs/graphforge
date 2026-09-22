//! Async thin transport for native contextual claim Arrow results.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
use graphforge_api::CancellationToken;
enum Mutation {
    CreateResearchClaim(graphforge_api::CreateResearchClaimRequest),
    RelateResearchClaims(graphforge_api::RelateResearchClaimsRequest),
    RecordResearchDecisions(graphforge_api::RecordResearchDecisionsRequest),
}
enum Read {
    InspectResearchClaims(graphforge_api::InspectResearchClaimsRequest),
    ResearchClaimHistory(graphforge_api::ResearchClaimHistoryRequest),
    ResearchDecisionHistory(graphforge_api::ResearchAuthorityQuery),
    ResearchCanonicalChoices(graphforge_api::ResearchAuthorityQuery),
}
#[napi]
impl GraphForge {
    /// Execute the native CreateResearchClaimRequest contract as Arrow IPC.
    #[napi]
    pub fn create_research_claim(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ClaimMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::CreateResearchClaim(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native RelateResearchClaimsRequest contract as Arrow IPC.
    #[napi]
    pub fn relate_research_claims(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ClaimMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::RelateResearchClaims(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native RecordResearchDecisionsRequest contract as Arrow IPC.
    #[napi]
    pub fn record_research_decisions(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ClaimMutationTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimMutationTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Mutation::RecordResearchDecisions(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native InspectResearchClaimsRequest contract as Arrow IPC.
    #[napi]
    pub fn inspect_research_claims(
        &self,
        request: serde_json::Value,
    ) -> Result<crate::AsyncTask<ClaimReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Read::InspectResearchClaims(request),
        }))
    }
    /// Execute the native ResearchClaimHistoryRequest contract as Arrow IPC.
    #[napi]
    pub fn research_claim_history(
        &self,
        request: serde_json::Value,
    ) -> Result<crate::AsyncTask<ClaimReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Read::ResearchClaimHistory(request),
        }))
    }
    /// Execute the native ResearchAuthorityQuery contract as Arrow IPC.
    #[napi]
    pub fn research_decision_history(
        &self,
        request: serde_json::Value,
    ) -> Result<crate::AsyncTask<ClaimReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Read::ResearchDecisionHistory(request),
        }))
    }
    /// Execute the native ResearchAuthorityQuery contract as Arrow IPC.
    #[napi]
    pub fn research_canonical_choices(
        &self,
        request: serde_json::Value,
    ) -> Result<crate::AsyncTask<ClaimReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid contextual research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ClaimReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Read::ResearchCanonicalChoices(request),
        }))
    }
}
pub struct ClaimMutationTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    operation: Mutation,
    cancellation: CancellationToken,
}
impl Task for ClaimMutationTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let result = match &self.operation {
                Mutation::CreateResearchClaim(request) => {
                    graph.create_research_claim(request, &self.cancellation)
                }
                Mutation::RelateResearchClaims(request) => {
                    graph.relate_research_claims(request, &self.cancellation)
                }
                Mutation::RecordResearchDecisions(request) => {
                    graph.record_research_decisions(request, &self.cancellation)
                }
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
pub struct ClaimReadTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    operation: Read,
}
impl Task for ClaimReadTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let result = match &self.operation {
                Read::InspectResearchClaims(request) => graph.inspect_research_claims(request),
                Read::ResearchClaimHistory(request) => graph.research_claim_history(request),
                Read::ResearchDecisionHistory(request) => {
                    graph.research_decision_history(&request.context, request.community_uuid)
                }
                Read::ResearchCanonicalChoices(request) => {
                    graph.research_canonical_choices(&request.context, request.community_uuid)
                }
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
