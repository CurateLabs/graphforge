//! Native asynchronous Arrow transport for upstream preview and history.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
enum Read {
    Preview(graphforge_api::PreviewResearchUpstreamRequest),
    History(graphforge_api::ResearchUpstreamHistoryRequest),
}
#[napi]
impl GraphForge {
    /// Execute the native PreviewResearchUpstreamRequest contract and return Arrow IPC.
    #[napi]
    pub fn preview_research_upstream(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<UpstreamReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid upstream research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(UpstreamReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            request: Read::Preview(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ResearchUpstreamHistoryRequest contract and return Arrow IPC.
    #[napi]
    pub fn research_upstream_history(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<UpstreamReadTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid upstream research JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(UpstreamReadTask {
            engine: std::sync::Arc::clone(&self.inner),
            request: Read::History(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct UpstreamReadTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: Read,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for UpstreamReadTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let result = match &self.request {
                Read::Preview(request) => {
                    graph.preview_research_upstream(request, &self.cancellation)?
                }
                Read::History(request) => {
                    graph.research_upstream_history(request, &self.cancellation)?
                }
            };
            crate::result_to_ipc(&result)
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|error| crate::to_napi_deferred_err(env, &error))
    }
}
