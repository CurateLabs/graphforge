//! Construction bindings and native task ownership.

use crate::Buffer;
use crate::CompositeTransactionInput;
use crate::Env;
use crate::GraphForge;
use crate::Object;
use crate::Result;
use crate::Unknown;
use crate::bulk_edge_publication_error;
use crate::bulk_node_publication_error;
use crate::canonical_operation_id;
use crate::composite;
use crate::ipc_to_record_batch;
use crate::napi;
use crate::node_handle_from_unknown;
use crate::props_from_js_object;
use crate::record_batch_to_ipc;
use crate::to_napi_err;

/// UUID-backed node handle returned by [`GraphForge::add_node`].
#[napi]
pub struct NodeHandle {
    pub(super) inner: graphforge_api::NodeHandle,
}

#[napi]
impl NodeHandle {
    /// Stable public UUID identity.
    #[napi(getter)]
    #[must_use]
    pub fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Primary label metadata (not an identity surrogate).
    #[napi(getter)]
    #[must_use]
    pub fn label(&self) -> String {
        self.inner.label.clone()
    }

    /// Human-readable UUID-only handle representation.
    #[napi(js_name = "toString")]
    #[must_use]
    pub fn as_string(&self) -> String {
        self.inner.to_string()
    }
}

/// UUID-backed edge handle returned by [`GraphForge::add_edge`].
#[napi]
pub struct EdgeHandle {
    inner: graphforge_api::EdgeHandle,
}

#[napi]
impl EdgeHandle {
    /// Stable public UUID identity.
    #[napi(getter)]
    #[must_use]
    pub fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Relationship-type metadata (not an identity surrogate).
    #[napi(getter)]
    #[must_use]
    pub fn rel_type(&self) -> String {
        self.inner.rel_type.clone()
    }

    /// Human-readable UUID-only handle representation.
    #[napi(js_name = "toString")]
    #[must_use]
    pub fn as_string(&self) -> String {
        self.inner.to_string()
    }
}

#[napi]
impl GraphForge {
    /// Add a node through the Rust facade and return its graph-owned UUID handle.
    #[napi]
    pub fn add_node(
        &self,
        env: Env,
        label: String,
        #[napi(ts_arg_type = "Record<string, unknown> | null | undefined")] props: Option<Object>,
    ) -> Result<NodeHandle> {
        self.ensure_open()?;
        let props = props_from_js_object(env, props)?;
        let graph = self.open_guard()?;
        graph
            .add_node(&label, &props)
            .map(|inner| NodeHandle { inner })
            .map_err(|error| to_napi_err(&error))
    }

    /// Add a directed edge and return its graph UUID handle.
    #[napi]
    pub fn add_edge(
        &self,
        env: Env,
        #[napi(ts_arg_type = "NodeHandle")] src: Unknown,
        rel_type: String,
        #[napi(ts_arg_type = "NodeHandle")] dst: Unknown,
        #[napi(ts_arg_type = "Record<string, unknown> | null | undefined")] props: Option<Object>,
    ) -> Result<EdgeHandle> {
        let src = node_handle_from_unknown(env, src, "source")?;
        let dst = node_handle_from_unknown(env, dst, "destination")?;
        let props = props_from_js_object(env, props)?;
        let graph = self.open_guard()?;
        graph
            .add_edge(&src.inner, &rel_type, &dst.inner, &props)
            .map(|inner| EdgeHandle { inner })
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one atomic bulk node batch (Arrow IPC) through the Rust contract.
    #[napi]
    pub fn publish_bulk_nodes(&self, operation_uuid: String, data: Buffer) -> Result<Buffer> {
        let operation_uuid = canonical_operation_id(&operation_uuid)?;
        let batch = ipc_to_record_batch(&data)?;
        let graph = self.open_guard()?;
        let receipt = graph
            .publish_bulk_nodes(operation_uuid, &[batch])
            .map_err(bulk_node_publication_error)?;
        record_batch_to_ipc(&receipt)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one atomic bulk edge batch (Arrow IPC) through the Rust contract.
    #[napi]
    pub fn publish_bulk_edges(&self, operation_uuid: String, data: Buffer) -> Result<Buffer> {
        let operation_uuid = canonical_operation_id(&operation_uuid)?;
        let batch = ipc_to_record_batch(&data)?;
        let graph = self.open_guard()?;
        let receipt = graph
            .publish_bulk_edges(operation_uuid, &[batch])
            .map_err(bulk_edge_publication_error)?;
        record_batch_to_ipc(&receipt)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one composite graph + knowledge transaction through Rust.
    ///
    /// Returns the canonical singleton Arrow IPC receipt. Node performs only
    /// request conversion; validation, staging, publication, recovery, and
    /// idempotency remain Rust-owned.
    #[napi]
    pub fn publish_composite_transaction(
        &self,
        request: CompositeTransactionInput,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let graph = self.open_guard()?;
        composite::publish_composite_transaction(&graph, request)
    }
}
