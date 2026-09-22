//! Asynchronous transport; all research identity and lifecycle behavior is native.
use crate::{Env, GraphForge, Result, Task, napi};
enum Request {
    Reference(graphforge_api::ResearchReferenceTarget),
    Export(graphforge_api::ExportResearchRequest),
    Fork(Box<graphforge_api::ForkResearchRequest>),
}
#[napi]
impl GraphForge {
    /// Execute the native ResearchReferenceTarget contract.
    #[napi]
    pub fn research_reference(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ResearchInterchangeTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid research interchange JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ResearchInterchangeTask {
            engine: std::sync::Arc::clone(&self.inner),
            request: Request::Reference(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ExportResearchRequest contract.
    #[napi]
    pub fn export_research(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ResearchInterchangeTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid research interchange JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ResearchInterchangeTask {
            engine: std::sync::Arc::clone(&self.inner),
            request: Request::Export(request),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
    /// Execute the native ForkResearchRequest contract.
    #[napi]
    pub fn fork_research(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<ResearchInterchangeTask>> {
        self.ensure_open()?;
        let request = serde_json::from_value(request).map_err(|_| {
            crate::to_napi_err(&graphforge_api::GfError::Validation(
                "invalid research interchange JSON contract".into(),
            ))
        })?;
        Ok(crate::AsyncTask::new(ResearchInterchangeTask {
            engine: std::sync::Arc::clone(&self.inner),
            request: Request::Fork(Box::new(request)),
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub enum ResearchInterchangeError {
    Native(graphforge_api::GfError),
    Portable(graphforge_api::PortableV2Error),
}
pub struct ResearchInterchangeTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    request: Request,
    cancellation: graphforge_api::CancellationToken,
}
impl Task for ResearchInterchangeTask {
    type Output = std::result::Result<serde_json::Value, ResearchInterchangeError>;
    type JsValue = crate::Unknown<'static>;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                ResearchInterchangeError::Native(graphforge_api::GfError::Execution(
                    "GraphForge lock poisoned".into(),
                ))
            })?;
            let value = match &self.request {
                Request::Reference(request) => serde_json::to_value(
                    graph
                        .research_reference(request, &self.cancellation)
                        .map_err(ResearchInterchangeError::Native)?,
                ),
                Request::Export(request) => serde_json::to_value(
                    graph
                        .export_research(request, &self.cancellation)
                        .map_err(ResearchInterchangeError::Portable)?,
                ),
                Request::Fork(request) => serde_json::to_value(
                    graph
                        .fork_research(request, &self.cancellation)
                        .map_err(ResearchInterchangeError::Portable)?,
                ),
            };
            value.map_err(|_| {
                ResearchInterchangeError::Native(graphforge_api::GfError::Execution(
                    "research metadata serialization failed".into(),
                ))
            })
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        env.to_js_value(&output.map_err(|error| match error {
            ResearchInterchangeError::Native(error) => crate::to_napi_deferred_err(env, &error),
            ResearchInterchangeError::Portable(error) => {
                crate::portable::to_portable_deferred_err(env, error)
            }
        })?)
    }
}
