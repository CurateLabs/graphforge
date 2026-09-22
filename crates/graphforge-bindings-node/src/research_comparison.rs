//! Thin asynchronous transport for native semantic comparison Arrow results.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
#[napi]
impl GraphForge {
    /// Compare exact research endpoints and return Arrow IPC.
    #[napi]
    pub fn compare_research(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ComparisonTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid research comparison JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ComparisonTask {
            engine: std::sync::Arc::clone(&self.inner),
            request,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct ComparisonTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ResearchComparisonRequest,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for ComparisonTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            crate::result_to_ipc(&graph.compare_research(&self.request, &self.cancellation)?)
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}
