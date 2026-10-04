//! Thin native saved query metadata and cancellable Arrow execution bindings.
use crate::{Buffer, GraphForge, Result, Task, napi};
use graphforge_api::{CancellationToken, IrLiteral, SavedQuery, SavedQuerySource};
use std::collections::HashMap;

fn contract<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| {
        crate::to_napi_err(&graphforge_api::GfError::Validation(
            "invalid saved query JSON contract".into(),
        ))
    })
}
fn output(value: impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|_| {
        crate::to_napi_err(&graphforge_api::GfError::Execution(
            "saved query metadata serialization failed".into(),
        ))
    })
}
fn source(value: Option<serde_json::Value>) -> Result<SavedQuerySource> {
    value.map(contract).unwrap_or(Ok(SavedQuerySource::Current))
}
#[napi]
impl GraphForge {
    #[napi(ts_return_type = "import('./lib/saved-queries').SavedQuery")]
    pub fn create_saved_query(
        &self,
        #[napi(ts_arg_type = "import('./lib/saved-queries').SavedQuery")]
        definition: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let definition: SavedQuery = contract(definition)?;
        output(
            self.open_write_guard()?
                .create_saved_query(definition)
                .map_err(|e| crate::to_napi_err(&e))?,
        )
    }
    #[napi(ts_return_type = "import('./lib/saved-queries').SavedQuery")]
    pub fn update_saved_query(
        &self,
        #[napi(ts_arg_type = "import('./lib/saved-queries').SavedQuery")]
        definition: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let definition: SavedQuery = contract(definition)?;
        output(
            self.open_write_guard()?
                .update_saved_query(definition)
                .map_err(|e| crate::to_napi_err(&e))?,
        )
    }
    #[napi]
    pub fn delete_saved_query(&self, query_uuid: String) -> Result<()> {
        let id = crate::canonical_operation_id(&query_uuid)?.0;
        self.open_write_guard()?
            .delete_saved_query(id)
            .map_err(|e| crate::to_napi_err(&e))
    }
    #[napi(ts_return_type = "import('./lib/saved-queries').SavedQuery")]
    pub fn saved_query(
        &self,
        query_uuid: String,
        #[napi(ts_arg_type = "import('./lib/saved-queries').SavedQuerySource | null | undefined")]
        source: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let id = crate::canonical_operation_id(&query_uuid)?.0;
        output(
            self.open_guard()?
                .saved_query_at(id, &self::source(source)?)
                .map_err(|e| crate::to_napi_err(&e))?,
        )
    }
    #[napi(ts_return_type = "Array<import('./lib/saved-queries').SavedQuery>")]
    pub fn saved_queries(
        &self,
        #[napi(ts_arg_type = "import('./lib/saved-queries').SavedQuerySource | null | undefined")]
        source: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        output(
            self.open_guard()?
                .saved_queries_at(&self::source(source)?)
                .map_err(|e| crate::to_napi_err(&e))?,
        )
    }
    #[napi]
    pub fn execute_saved_query(
        &self,
        query_uuid: String,
        #[napi(
            ts_arg_type = "import('./lib/saved-queries').SavedQueryParameters | null | undefined"
        )]
        params: Option<HashMap<String, serde_json::Value>>,
        #[napi(ts_arg_type = "import('./lib/saved-queries').SavedQuerySource | null | undefined")]
        source: Option<serde_json::Value>,
        env: crate::Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SavedQueryTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(SavedQueryTask {
            engine: std::sync::Arc::clone(&self.inner),
            query_uuid: crate::canonical_operation_id(&query_uuid)?.0,
            params: crate::params_from_map(params)?,
            source: self::source(source)?,
            cancellation: crate::slices::cancellation(env, signal)?,
        }))
    }
}
pub struct SavedQueryTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    query_uuid: uuid::Uuid,
    params: HashMap<String, IrLiteral>,
    source: SavedQuerySource,
    cancellation: CancellationToken,
}
impl Task for SavedQueryTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let result = graph.execute_saved_query(
                self.query_uuid,
                &self.params,
                &self.source,
                Some(&self.cancellation),
            )?;
            crate::result_to_ipc(&result)
        })())
    }
    fn resolve(&mut self, env: crate::Env, output: Self::Output) -> Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}
