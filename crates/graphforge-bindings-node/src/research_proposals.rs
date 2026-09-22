//! Asynchronous native proposal transport.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
#[napi]
impl GraphForge {
    /// Execute the native SubmitResearchProposalRequest contract.
    #[napi]
    pub fn submit_research_proposal(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SubmitProposalTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Proposal JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(SubmitProposalTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct SubmitProposalTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::SubmitResearchProposalRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for SubmitProposalTask {
    type Output = std::result::Result<serde_json::Value, graphforge_api::GfError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let receipt = graph.submit_research_proposal(&self.request, &self.cancellation)?;
            serde_json::to_value(receipt).map_err(|_| {
                graphforge_api::GfError::Execution("Proposal receipt serialization failed".into())
            })
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|e| crate::to_napi_deferred_err(env, &e))?)
    }
}
#[napi]
impl GraphForge {
    /// Execute the native PreviewResearchProposalRequest contract.
    #[napi]
    pub fn preview_research_proposal(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<PreviewProposalTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Proposal JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(PreviewProposalTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct PreviewProposalTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::PreviewResearchProposalRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for PreviewProposalTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            crate::result_to_ipc(
                &graph.preview_research_proposal(&self.request, &self.cancellation)?,
            )
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}
#[napi]
impl GraphForge {
    /// Execute the native ReviewResearchProposalRequest contract.
    #[napi]
    pub fn review_research_proposal(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ReviewProposalTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Proposal JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ReviewProposalTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct ReviewProposalTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ReviewResearchProposalRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for ReviewProposalTask {
    type Output = std::result::Result<serde_json::Value, graphforge_api::GfError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let receipt = graph.review_research_proposal(&self.request, &self.cancellation)?;
            serde_json::to_value(receipt).map_err(|_| {
                graphforge_api::GfError::Execution("Proposal receipt serialization failed".into())
            })
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|e| crate::to_napi_deferred_err(env, &e))?)
    }
}
#[napi]
impl GraphForge {
    /// Execute the native ReleaseResearchProposalRequest contract.
    #[napi]
    pub fn release_research_proposal(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ReleaseProposalTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Proposal JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ReleaseProposalTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct ReleaseProposalTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ReleaseResearchProposalRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for ReleaseProposalTask {
    type Output = std::result::Result<serde_json::Value, graphforge_api::GfError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let mut graph = self.engine.write().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let receipt = graph.release_research_proposal(&self.request, &self.cancellation)?;
            serde_json::to_value(receipt).map_err(|_| {
                graphforge_api::GfError::Execution("Proposal receipt serialization failed".into())
            })
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|e| crate::to_napi_deferred_err(env, &e))?)
    }
}
#[napi]
impl GraphForge {
    /// Execute the native ResearchProposalHistoryRequest contract.
    #[napi]
    pub fn research_proposal_history(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ProposalHistoryTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid Proposal JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ProposalHistoryTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct ProposalHistoryTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ResearchProposalHistoryRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for ProposalHistoryTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            crate::result_to_ipc(
                &graph.research_proposal_history(&self.request, &self.cancellation)?,
            )
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}
