//! Query planning, session authority, and execution evidence lifetime.

use crate::ExecutionResult;
use crate::ExecutionStats;
use crate::SideEffects;
use crate::adjacency::AdjacencyProvider;
use crate::adjacency::PersistentAdjacencyProvider;
use crate::create_exec::GraphCreateExec;
use crate::create_exec::create_tally_in_plan;
use crate::demand;
use crate::expand_exec::ExpandExec;
use crate::expand_exec::OntologyInferExec;
use crate::expand_exec::V4OrdinalIdentityResolver;
use crate::expand_exec::V4OrdinalIdentitySession;
use crate::expand_exec::VarLenExpandExec;
use crate::mutation;
use crate::path_hydration;
use crate::physical_schema_fallback;
use crate::read_resource;
use crate::row_exec::OptionalMatchExec;
use crate::row_exec::UnwindExec;
use crate::write_driver;
use crate::write_exec::GraphDeleteExec;
use crate::write_exec::GraphRemoveExec;
use crate::write_exec::GraphSetExec;
use crate::write_resource;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::Stream;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_ir::ExprArena;
use graphforge_ir::ExprId;
use graphforge_ir::GraphOp;
use graphforge_ir::GraphPlan;
use graphforge_ir::IrExpr;
use graphforge_ir::VarId;
use graphforge_ontology::OntologyHandle;
use graphforge_plan::GraphCreateNode;
use graphforge_plan::GraphDeleteNode;
use graphforge_plan::GraphRemoveNode;
use graphforge_plan::GraphSetNode;
use graphforge_plan::OptionalMatchNode;
use graphforge_plan::UnwindNode;
use graphforge_plan::VarLenExpandNode;
use graphforge_rel::GraphPlanLowerer;
use graphforge_storage::GraphCatalog;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;

/// Whether `plan` contains an operator that reads persisted topology/property
/// data (and therefore needs a real project directory to bind its Parquet
/// provider). Recurses into `Optional`/`Union` sub-plans.
fn plan_reads_persisted_data(plan: &GraphPlan) -> bool {
    use graphforge_ir::GraphOp;
    plan.ops.iter().any(|op| match op {
        GraphOp::NodeScan { .. }
        | GraphOp::EdgeScan { .. }
        | GraphOp::TypedEdgeScan { .. }
        | GraphOp::Expand { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => {
            plan_reads_persisted_data(child)
        }
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_reads_persisted_data),
        _ => false,
    })
}

/// True when `plan` contains a write terminal that must lower via
/// [`GraphPlanLowerer::new_for_writes`].
fn plan_requires_writes(plan: &GraphPlan) -> bool {
    use graphforge_ir::GraphOp;
    plan.ops.iter().any(|op| match op {
        GraphOp::Create { .. }
        | GraphOp::Merge { .. }
        | GraphOp::Delete { .. }
        | GraphOp::Set { .. }
        | GraphOp::Remove { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => plan_requires_writes(child),
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_requires_writes),
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// ExtensionPlanner + QueryPlanner
// ---------------------------------------------------------------------------

/// `SessionConfig` extension carrying the session's adjacency provider
/// (#761). Config extensions are keyed by `TypeId`, so a concrete newtype is
/// required to carry the trait object from [`ExecutionSession`] to
/// [`GraphForgeExtensionPlanner`].
pub struct AdjacencyProviderExt(pub Arc<dyn AdjacencyProvider>);

/// Mutable session ownership ends at planning: plans retain only a selected immutable provider.
struct OwnedSessionAdjacency(RwLock<Arc<PersistentAdjacencyProvider>>);

/// `SessionConfig` extension carrying the facade's exact generation-pinned
/// ordinal identity authority.
struct OrdinalIdentityResolverExt(pub Option<Arc<V4OrdinalIdentitySession>>);

fn plan_expand_extension(
    expand: &graphforge_plan::ExpandNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let input = physical_inputs
        .first()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("Expand requires one physical input".into()))?;
    let resource = read_resource::required(session_state)?;
    resource.validate(expand.read_contract.as_ref())?;
    let provider = session_state
        .config()
        .get_extension::<AdjacencyProviderExt>()
        .ok_or_else(|| DataFusionError::Plan("GF_READ_RESOURCE_MISSING: adjacency".into()))?
        .0
        .clone();
    let identity_extension = session_state
        .config()
        .get_extension::<OrdinalIdentityResolverExt>();
    let ordinal_identity_required = identity_extension.is_some();
    let ordinal_identities = identity_extension
        .as_ref()
        .and_then(|extension| extension.0.as_ref().map(Arc::clone));
    Ok(Arc::new(ExpandExec::new(
        expand,
        input,
        provider,
        ordinal_identities,
        ordinal_identity_required,
        &resource,
    )))
}

/// Plans GraphForge's custom logical [`Extension`](LogicalPlan::Extension)
/// nodes into physical [`ExecutionPlan`]s.
#[derive(Debug, Default)]
pub(super) struct GraphForgeExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for GraphForgeExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(create) = node.as_any().downcast_ref::<GraphCreateNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphCreate requires one physical input".into())
            })?;
            let resource = write_resource::required(session_state, create.write_contract.as_ref())?;
            resource.validate_composition(create.semantic_composition_fingerprint.as_deref())?;
            return Ok(Some(Arc::new(GraphCreateExec::new(
                create, input, &resource,
            )?)));
        }
        if let Some(delete) = node.as_any().downcast_ref::<GraphDeleteNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphDelete requires one physical input".into())
            })?;
            let resource = write_resource::required(session_state, delete.write_contract.as_ref())?;
            return Ok(Some(Arc::new(GraphDeleteExec::new(
                delete, input, &resource,
            )?)));
        }
        if let Some(set) = node.as_any().downcast_ref::<GraphSetNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphSet requires one physical input".into())
            })?;
            let resource = write_resource::required(session_state, set.write_contract.as_ref())?;
            return Ok(Some(Arc::new(GraphSetExec::new(set, input, &resource)?)));
        }
        if let Some(remove) = node.as_any().downcast_ref::<GraphRemoveNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphRemove requires one physical input".into())
            })?;
            let resource = write_resource::required(session_state, remove.write_contract.as_ref())?;
            return Ok(Some(Arc::new(GraphRemoveExec::new(
                remove, input, &resource,
            )?)));
        }
        if let Some(expand) = node.as_any().downcast_ref::<graphforge_plan::ExpandNode>() {
            return plan_expand_extension(expand, physical_inputs, session_state).map(Some);
        }
        if let Some(var_len) = node.as_any().downcast_ref::<VarLenExpandNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("VarLenExpand requires one physical input".into())
            })?;
            let resource = read_resource::required(session_state)?;
            resource.validate(var_len.read_contract.as_ref())?;
            let provider = session_state
                .config()
                .get_extension::<AdjacencyProviderExt>()
                .ok_or_else(|| DataFusionError::Plan("GF_READ_RESOURCE_MISSING: adjacency".into()))?
                .0
                .clone();
            return Ok(Some(Arc::new(VarLenExpandExec::new(
                var_len,
                input,
                provider,
                resource.dir.clone(),
                resource.mode,
            ))));
        }
        if let Some(opt) = node.as_any().downcast_ref::<OptionalMatchNode>() {
            // inputs() order is [outer, optional], so physical_inputs matches.
            let (Some(outer), Some(inner)) = (
                physical_inputs.first().cloned(),
                physical_inputs.get(1).cloned(),
            ) else {
                return Err(DataFusionError::Internal(
                    "OptionalMatch requires two physical inputs".into(),
                ));
            };
            return Ok(Some(Arc::new(OptionalMatchExec::new(opt, outer, inner))));
        }
        if let Some(unwind) = node.as_any().downcast_ref::<UnwindNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("Unwind requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(UnwindExec::new(unwind, input))));
        }
        if let Some(infer) = node
            .as_any()
            .downcast_ref::<graphforge_plan::OntologyInferNode>()
        {
            // Pass-through (#605): the wrapped var-len input computes the closure;
            // this carries the inference rule_id into the physical plan/explain().
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("OntologyInfer requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(OntologyInferExec::new(infer, input))));
        }
        Ok(None)
    }
}

/// Custom DataFusion query planner that knows how to physically plan
/// GraphForge's graph-native logical nodes.
#[derive(Debug, Default)]
pub struct GraphForgeQueryPlanner;

#[async_trait]
impl QueryPlanner for GraphForgeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut pinned_state = session_state.clone();
        if let Some(owner) = session_state
            .config()
            .get_extension::<OwnedSessionAdjacency>()
        {
            let provider: Arc<dyn AdjacencyProvider> = {
                let current = owner.0.read().map_err(|_| {
                    DataFusionError::Execution("session adjacency lock poisoned".into())
                })?;
                Arc::clone(&*current) as _
            };
            pinned_state
                .config_mut()
                .set_extension(Arc::new(AdjacencyProviderExt(provider)));
        }
        let session_state = &pinned_state;
        let (logical_plan, hydration) = read_resource::bind(logical_plan, session_state)?;
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            GraphForgeExtensionPlanner,
        )]);
        let physical = planner
            .create_physical_plan(&logical_plan, session_state)
            .await?;
        Ok(path_hydration::wrap(physical, hydration))
    }
}

// ---------------------------------------------------------------------------
// ExecutionSession
// ---------------------------------------------------------------------------

/// Resource knobs applied when constructing an [`ExecutionSession`] (#337).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionResourceConfig {
    /// DataFusion `target_partitions`.
    pub target_partitions: usize,
    /// DataFusion batch size.
    pub batch_size: usize,
    /// Memory pool budget in bytes.
    pub memory_budget_bytes: u64,
    /// Whether spill-to-disk is enabled.
    pub spill_enabled: bool,
    /// Absolute spill directory when spill is enabled.
    pub spill_directory: Option<PathBuf>,
    /// Optional spill byte cap.
    pub spill_max_bytes: Option<u64>,
    /// Concurrent Parquet / I/O open budget (#337 / #339).
    pub io_concurrency: usize,
}

impl Default for SessionResourceConfig {
    fn default() -> Self {
        Self {
            target_partitions: 2,
            batch_size: 8_192,
            memory_budget_bytes: 512 * 1024 * 1024,
            spill_enabled: false,
            spill_directory: None,
            spill_max_bytes: None,
            io_concurrency: 2,
        }
    }
}

struct QueryEvidenceStream {
    inner: Option<SendableRecordBatchStream>,
    physical: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    memory_reserved_before: usize,
    returned_batch_bytes: usize,
    finalized: bool,
}

impl QueryEvidenceStream {
    fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        path_hydration::cancel_plan(&self.physical);
        drop(self.inner.take());
        demand::record_plan_completion(
            &self.physical,
            self.memory_reserved_before,
            self.task_ctx.memory_pool().reserved(),
            self.returned_batch_bytes,
            self.task_ctx.session_config().batch_size(),
        );
    }
}

impl Stream for QueryEvidenceStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.returned_batch_bytes = this
                    .returned_batch_bytes
                    .saturating_add(batch.get_array_memory_size());
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.finalize();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finalize();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for QueryEvidenceStream {
    fn drop(&mut self) {
        self.finalize();
    }
}

/// A configured DataFusion [`SessionContext`] ready to execute [`GraphPlan`]s.
///
/// Construct via [`ExecutionSession::new`] (read/query) or
/// [`ExecutionSession::new_with_target`] (write execution, e.g. `CREATE`).
///
/// # Thread safety
///
/// `ExecutionSession` is `Send + Sync`.
pub struct ExecutionSession {
    pub(super) mutation_health: mutation::MutationHealth,
    pub(super) ctx: SessionContext,
    /// The graph catalog, retained so read lowering can decide typed-vs-
    /// exploratory edge tables. The same `Arc` is also registered on `ctx`.
    pub(super) catalog: Arc<GraphCatalog>,
    ontology: Option<OntologyHandle>,
    /// Project directory for write execution; empty for read-only sessions.
    dir: PathBuf,
    /// Ontology mode driving write routing.
    mode: OntologyMode,
    /// Exact composition fingerprint authenticating semantic write routes.
    semantic_composition_fingerprint: Option<String>,
    /// The session's adjacency provider (also registered as a SessionConfig
    /// extension for the planner). Held concretely so successful writes can
    /// invalidate its memoized state and loaded views — a same-session
    /// read → write → read must observe post-write adjacency.
    pub(super) adjacency_provider: Arc<PersistentAdjacencyProvider>,
    owned_adjacency: Option<Arc<OwnedSessionAdjacency>>,
    /// Differential-test strategy, absent from ordinary builds.
    #[cfg(feature = "differential-testing")]
    relational_fixed_hop_reference: bool,
}

#[derive(Default)]
struct OrdinalIdentityConfig {
    session: Option<Arc<V4OrdinalIdentitySession>>,
    required: bool,
}

impl ExecutionSession {
    /// Remove mutation permission while retaining this session's read authority.
    #[must_use]
    pub fn restrict_to_reads(mut self) -> Self {
        let mut state = self.ctx.state();
        if let Some(resource) = state
            .config()
            .get_extension::<read_resource::GraphReadContext>()
        {
            state
                .config_mut()
                .set_extension(Arc::new(write_resource::GraphWriteContext {
                    resource,
                    writable: false,
                }));
        }
        self.ctx = SessionContext::new_with_state(state);
        self
    }

    /// Obtain the explicit admitted write target for physical operator construction.
    ///
    /// # Errors
    /// Returns a typed error for missing or read-only mutation authority.
    pub fn write_resource(&self) -> Result<write_resource::BoundWriteResource, GfError> {
        write_resource::required(&self.ctx.state(), None).map_err(GfError::from_plan_error)
    }

    /// Create a read/query session.
    ///
    /// Write execution (`CREATE`) requires a project directory — use
    /// [`new_with_target`](Self::new_with_target) for that.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new(catalog: GraphCatalog, ontology: Option<OntologyHandle>) -> Result<Self, GfError> {
        Ok(Self::build(
            catalog,
            ontology,
            PathBuf::new(),
            OntologyMode::Exploratory,
            None,
            OrdinalIdentityConfig::default(),
            &SessionResourceConfig::default(),
        ))
    }

    /// Create a session that can execute writes against `dir`.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Result<Self, GfError> {
        Ok(Self::build(
            catalog,
            ontology,
            dir,
            mode,
            None,
            OrdinalIdentityConfig::default(),
            &SessionResourceConfig::default(),
        ))
    }

    /// Like [`new_with_target`](Self::new_with_target) but reusing a
    /// long-lived adjacency provider (#832): the facade owns one per
    /// `GraphForge` instance so loaded CSR views amortize across queries.
    /// The provider is [`revalidate`](PersistentAdjacencyProvider::revalidate)d
    /// at construction, so each session still observes external index/topology
    /// changes.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target_and_provider(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
    ) -> Result<Self, GfError> {
        Self::new_with_target_provider_and_resources(
            catalog,
            ontology,
            dir,
            mode,
            provider,
            &SessionResourceConfig::default(),
        )
    }

    /// Like [`new_with_target_and_provider`] with an explicit resource policy.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target_provider_and_resources(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
        resources: &SessionResourceConfig,
    ) -> Result<Self, GfError> {
        Self::new_with_target_provider_resources_and_identity(
            catalog, ontology, dir, mode, provider, None, resources,
        )
    }

    /// Like [`Self::new_with_target_provider_and_resources`] with an exact
    /// generation-pinned destination identity authority.
    pub fn new_with_target_provider_resources_and_identity(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
        ordinal_identities: Option<Arc<V4OrdinalIdentityResolver>>,
        resources: &SessionResourceConfig,
    ) -> Result<Self, GfError> {
        let identity = match ordinal_identities {
            Some(resolver) => {
                let pin = resolver.pin()?;
                OrdinalIdentityConfig {
                    // Requirement and payload come from one resolver snapshot.
                    // Keep them separate so the expansion boundary remains
                    // fail-closed if a future planner/config rewrite loses the
                    // pinned authority.
                    required: pin.required,
                    session: pin.session,
                }
            }
            None => OrdinalIdentityConfig::default(),
        };
        Ok(Self::build(
            catalog,
            ontology,
            dir,
            mode,
            Some(provider),
            identity,
            resources,
        ))
    }

    fn build(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        shared_provider: Option<Arc<PersistentAdjacencyProvider>>,
        identity: OrdinalIdentityConfig,
        resources: &SessionResourceConfig,
    ) -> Self {
        // The session-scoped adjacency provider (#761), threaded to the
        // extension planner via SessionConfig extension. Read-only sessions
        // (empty dir) need no special case: with no `indexes/adjacency/`
        // capability dir the provider degrades to scan-build by itself.
        // A facade-shared provider (#832) is revalidated here — once per
        // session — so its memoized state is as fresh as a per-query
        // provider's, while loaded views amortize across queries.
        let owns_provider = shared_provider.is_none();
        let adjacency_provider = shared_provider.map_or_else(
            || {
                let provider = PersistentAdjacencyProvider::new(dir.clone(), mode);
                Arc::new(match catalog.admitted_inventory() {
                    Some(inventory) => provider.with_inventory(inventory),
                    None => provider,
                })
            },
            |p| {
                p.revalidate();
                p
            },
        );
        let owned_adjacency = owns_provider.then(|| {
            Arc::new(OwnedSessionAdjacency(RwLock::new(Arc::clone(
                &adjacency_provider,
            ))))
        });
        let provider: Arc<dyn AdjacencyProvider> = Arc::clone(&adjacency_provider) as _;
        let catalog = Arc::new(catalog);
        let mutation_health = mutation::MutationHealth::default();
        let read_resource = Arc::new(read_resource::GraphReadContext {
            health: mutation_health.clone(),
            dir: dir.clone(),
            mode,
            catalog: catalog.clone(),
            ontology: ontology.clone(),
        });
        let mut config = datafusion::prelude::SessionConfig::new()
            .with_extension(read_resource.clone())
            .with_extension(Arc::new(AdjacencyProviderExt(provider)))
            .with_extension(Arc::new(graphforge_storage::IoConcurrencyExt::new(
                resources.io_concurrency,
            )))
            .with_target_partitions(resources.target_partitions)
            .with_batch_size(resources.batch_size);
        if let Some(owner) = &owned_adjacency {
            config.set_extension(Arc::clone(owner));
        }
        if !dir.as_os_str().is_empty() {
            config.set_extension(Arc::new(write_resource::GraphWriteContext {
                resource: read_resource,
                writable: true,
            }));
        }
        if identity.session.is_some() || identity.required {
            config = config.with_extension(Arc::new(OrdinalIdentityResolverExt(identity.session)));
        }
        // Authenticated overlay scans publish sound physical-row upper bounds.
        // Let DataFusion use those estimates so a small one-partition source is
        // not eagerly repartitioned merely because newest-wins makes its exact
        // logical cardinality unavailable without executing the scan.
        config
            .options_mut()
            .execution
            .use_row_number_estimates_to_optimize_partitioning = true;

        let memory_budget = usize::try_from(resources.memory_budget_bytes).unwrap_or(usize::MAX);
        let mut runtime_builder = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_limit(memory_budget, 1.0);
        if resources.spill_enabled
            && let Some(dir) = &resources.spill_directory
        {
            let _ = std::fs::create_dir_all(dir);
            runtime_builder = runtime_builder.with_temp_file_path(dir.clone());
            if let Some(max) = resources.spill_max_bytes {
                runtime_builder = runtime_builder.with_max_temp_directory_size(max);
            }
        }
        let runtime_env = Arc::new(
            runtime_builder
                .build()
                .expect("DataFusion RuntimeEnv construction"),
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_query_planner(Arc::new(GraphForgeQueryPlanner))
            .with_optimizer_rules(graphforge_rel::input_predicates::optimizer_rules())
            // Runs after DataFusion's default rules, when terminal fetches and
            // eager round-robin exchanges are visible (#1269).
            .with_physical_optimizer_rule(Arc::new(demand::FixedHopDemandRule))
            // Runs last, after the fast paths have replaced the sorts they own:
            // full sorts get input runs large enough to merge within the pool
            // they spill from (#1591).
            .with_physical_optimizer_rule(Arc::new(crate::sort_runs::SortRunCoalesceRule::new(
                crate::sort_runs::sort_run_bytes(memory_budget, resources.target_partitions),
            )))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let semantic_composition_fingerprint = catalog
            .semantic_composition_fingerprint()
            .map(str::to_owned);
        ctx.register_catalog("graph", catalog.clone());
        Self {
            mutation_health,
            ctx,
            catalog,
            ontology,
            dir,
            mode,
            semantic_composition_fingerprint,
            adjacency_provider,
            owned_adjacency,
            #[cfg(feature = "differential-testing")]
            relational_fixed_hop_reference: false,
        }
    }

    pub(super) fn refresh_adjacency_after_mutation(&self) -> Result<(), GfError> {
        let Some(owner) = &self.owned_adjacency else {
            self.adjacency_provider.invalidate();
            return Ok(());
        };
        let inventory = self.catalog.admitted_inventory().ok_or_else(|| {
            GfError::Storage("refreshed session lacks admitted graph inventory".into())
        })?;
        let private = tempfile::Builder::new()
            .prefix("graphforge-session-adjacency-")
            .tempdir()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let provider = Arc::new(
            PersistentAdjacencyProvider::new(self.dir.clone(), self.mode)
                .with_inventory(inventory)
                .with_rebuild_root(private),
        );
        *owner
            .0
            .write()
            .map_err(|_| GfError::Storage("session adjacency lock poisoned".into()))? = provider;
        Ok(())
    }

    /// Use the relational fixed-hop implementation as an independent test
    /// oracle. Available only with the non-default `differential-testing` feature.
    #[cfg(feature = "differential-testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_relational_fixed_hop_reference(mut self) -> Self {
        self.relational_fixed_hop_reference = true;
        self
    }

    /// Execute a `CREATE` [`GraphPlan`] and return the write summary.
    ///
    /// Lowers the plan (resolving CREATE specs against the session's ontology
    /// and write target), physically plans it through the custom
    /// [`ExtensionPlanner`], and collects the resulting summary batch.
    ///
    /// # Errors
    /// Returns [`GfError`] if the session has no write target, or if lowering /
    /// planning / execution fails.
    pub async fn execute_create(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_create_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `CREATE` [`GraphPlan`] with `$name` parameters available to a
    /// terminal `RETURN` / projection suffix.
    ///
    /// The write specs themselves are already bound into the graph plan; params
    /// matter for expressions that remain in the logical plan, such as
    /// `CREATE (n {name: 'Apa'}) RETURN n[$idx]`.
    pub async fn execute_create_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        if !plan
            .ops
            .iter()
            .any(|op| matches!(op, graphforge_ir::GraphOp::Merge { .. }))
        {
            return self.execute_write_statement_with_params(plan, params).await;
        }
        let resource = self.write_resource()?;
        resource
            .validate_composition(plan.composition_fingerprint.as_deref())
            .map_err(GfError::from_plan_error)?;

        // Pass the catalog: although CREATE has no scans, the lowerer resolves
        // each edge's relation-type name from it (the ontology map alone is
        // empty in exploratory mode, where relation names live in the runtime
        // catalog). Without it, edges are written with a `_UNKNOWN` relation
        // name and a later `MATCH ()-[:REL]->()` filter never matches.
        let lowerer = GraphPlanLowerer::new_for_writes(
            &graphforge_storage::lowering_snapshot(Some(&self.catalog), Some(&resource.dir))?,
            self.ontology.as_ref(),
            resource.mode,
        )?;
        let logical = bind_query_params(lowerer.lower_plan(plan)?, params)?;

        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(GfError::from_plan_error)?;

        let batches = path_hydration::collect_guarded(Arc::clone(&physical), self.ctx.task_ctx())
            .await
            .map_err(GfError::from_execution_error)?;

        let schema = batches
            .first()
            .map_or_else(GraphCreateNode::summary_schema, RecordBatch::schema);
        let rows_produced = batches.iter().map(|b| b.num_rows() as u64).sum();
        // In write-result RETURN (emit-rows) mode the `batches` are the created
        // rows, not the summary — so the side-effect counts come from the exec's
        // tally (found by walking the executed plan). Otherwise read the summary.
        let side_effects = Some(create_tally_in_plan(&physical).map_or_else(
            || SideEffects::from_summary(&batches),
            |t| SideEffects {
                nodes_created: t.nodes_created,
                relationships_created: t.edges_created,
                properties_set: t.properties_set,
                labels_added: t.labels_added,
                ..SideEffects::default()
            },
        ));
        self.catalog
            .refresh_property_inventory(&self.dir)
            .map_err(GfError::from_execution_error)?;
        self.refresh_adjacency_after_mutation()?;
        Ok(ExecutionResult {
            schema,
            batches,
            stats: ExecutionStats {
                rows_produced,
                execution_time_ms: 0,
            },
            side_effects,
            mutation_receipt: None,
        })
    }

    /// Execute a `DELETE` / `DETACH DELETE` [`GraphPlan`] and return the
    /// one-row write summary (#740).
    ///
    /// Like [`execute_create`](Self::execute_create), this requires a write
    /// target (build the session via `new_with_target`); the lowerer opens the
    /// write path via `new_for_writes` and the physical `GraphDeleteExec` drives
    /// the storage rewrite primitives, enforcing the no-`DETACH` relationship
    /// rule.
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails (including the no-`DETACH` error).
    pub async fn execute_delete(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_delete_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `DELETE` / `DETACH DELETE` [`GraphPlan`] with `$name` query
    /// parameters available to the read prefix and delete target expressions.
    ///
    /// # Errors
    /// As [`execute_delete`](Self::execute_delete).
    pub async fn execute_delete_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a `SET` [`GraphPlan`] and return the one-row write summary
    /// (`properties_set`) (#791).
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails.
    pub async fn execute_set(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_set_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `SET` [`GraphPlan`] with `$name` query parameters available to
    /// the read prefix and assigned expressions.
    ///
    /// # Errors
    /// As [`execute_set`](Self::execute_set).
    pub async fn execute_set_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a `REMOVE` [`GraphPlan`] and return the one-row write summary
    /// (`properties_removed`) (#791).
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails.
    pub async fn execute_remove(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_remove_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `REMOVE` [`GraphPlan`] with `$name` query parameters available
    /// to the read prefix.
    ///
    /// # Errors
    /// As [`execute_remove`](Self::execute_remove).
    pub async fn execute_remove_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a write statement in clause order through the unified driver
    /// (#817): one read prefix runs once, then each write clause
    /// applies against the shared frontier (extended with CREATE-minted
    /// variables), and every file effect commits as a single staged batch.
    /// Returns the one-row six-counter summary.
    ///
    /// # Errors
    /// Returns [`GfError`] for lowering, execution, or commit failures.
    pub async fn execute_write_statement(
        &self,
        plan: &GraphPlan,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, &HashMap::new())
            .await
    }

    /// Execute any supported write statement through the clause-ordered driver
    /// with query parameters available to prefix and value expressions.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_write_statement_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        let resource = self.write_resource()?;
        let mut lifecycle = mutation::LocalMutationLifecycle::new(self, resource.clone())?;
        let mut transaction = mutation::MutationTransaction::local();
        let result = match self
            .prepare_write_statement_with_params(plan, params, &mut transaction)
            .await
        {
            Ok(result) => result,
            Err(error) => return transaction.abort(&mut lifecycle, error),
        };
        transaction.commit(&resource, false, &mut lifecycle)?;
        Ok(result)
    }

    /// Evaluate and stage a statement without installing files or publishing.
    /// The caller's transaction owns the catalog used to build this session.
    ///
    /// # Errors
    /// Returns an error for missing write authority, evaluation or staging failure.
    #[allow(clippy::too_many_lines)]
    pub async fn prepare_write_statement_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
        transaction: &mut mutation::MutationTransaction,
    ) -> Result<ExecutionResult, GfError> {
        let resource = self.write_resource()?;
        transaction.admit(&resource)?;
        transaction.ensure_unprepared()?;
        resource
            .validate_composition(plan.composition_fingerprint.as_deref())
            .map_err(GfError::from_plan_error)?;
        let split = write_driver::split_write_plan(&plan.ops)?;
        let lowerer = GraphPlanLowerer::new_for_writes(
            &graphforge_storage::lowering_snapshot(Some(&self.catalog), Some(&resource.dir))?,
            self.ontology.as_ref(),
            resource.mode,
        )?;

        // Run the read prefix once, keeping the variable registrations the
        // write phases resolve against.
        let mut var_map = graphforge_rel::VarMap::new();
        let logical =
            lowerer.lower_prefix(&plan.ops[..split.prefix_len], &plan.exprs, &mut var_map)?;
        let logical = bind_query_params(logical, params)?;
        let df_schema = logical.schema().as_ref().clone();
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(GfError::from_plan_error)?;
        let batches = path_hydration::collect_guarded(physical, self.ctx.task_ctx())
            .await
            .map_err(GfError::from_execution_error)?;
        let mut frontier = write_driver::Frontier { df_schema, batches };

        // Phase loop: buffer every effect, then one staged commit.
        let env = write_driver::PhaseEnv {
            inventory: self.catalog.admitted_inventory(),
            lowerer: &lowerer,
            exprs: &plan.exprs,
            dir: &resource.dir,
            mode: resource.mode,
            params,
            type_map: lowerer.entity_name_map(),
            hydration: path_hydration::HydrationResource::new(
                read_resource::required(&self.ctx.state()).map_err(GfError::from_plan_error)?,
                Arc::clone(&self.ctx.runtime_env().memory_pool),
            ),
        };
        let mut wctx = write_driver::StatementWriteContext::new(&resource.dir, resource.mode)?
            .with_semantic_composition_fingerprint(self.semantic_composition_fingerprint.clone());
        let create_retention =
            write_driver::create_retention_by_write(&plan.ops, &plan.exprs, &split);
        let mut cursor = split.prefix_len;
        for &write_index in &split.write_ops {
            if cursor < write_index {
                run_write_relational_segment(
                    &self.ctx,
                    &lowerer,
                    plan,
                    cursor..write_index,
                    &mut frontier,
                    &mut var_map,
                    params,
                    &wctx.writer.pending_nodes_batch()?,
                )
                .await?;
            }
            write_driver::run_write_phases(
                &env,
                &plan.ops,
                &[write_index],
                &mut frontier,
                &mut var_map,
                &mut wctx,
                create_retention
                    .as_ref()
                    .and_then(|retention| retention.get(&write_index)),
            )?;
            cursor = write_index + 1;
        }
        Self::validate_deleted_entity_projection(
            plan,
            split.read_suffix_start,
            !wctx.deleted.is_empty(),
        )?;
        let terminal_logical = lower_write_terminal_suffix(
            &lowerer,
            plan,
            split.read_suffix_start,
            &frontier,
            &var_map,
            params,
            &wctx.writer.pending_nodes_batch()?,
        )?;
        let property_inventory = env.inventory.clone();
        drop(env);
        drop(lowerer);
        let terminal_result = match terminal_logical {
            Some(logical) => {
                Some(write_driver::run_terminal_suffix(&self.ctx, &logical, &frontier).await?)
            }
            None => None,
        };
        let c = wctx.mutation.counters;
        let mutation_receipt = Some(wctx.mutation_receipt());
        transaction.prepare_statement(wctx, &resource, property_inventory.as_deref())?;
        let side_effects = Some(SideEffects {
            nodes_created: c.nodes_created,
            nodes_deleted: c.nodes_deleted,
            relationships_created: c.edges_created,
            relationships_deleted: c.edges_deleted,
            properties_set: c.properties_set,
            properties_removed: c.properties_removed,
            labels_added: c.labels_added,
            labels_removed: c.labels_removed,
        });
        if let Some((schema, batches, rows_produced)) = terminal_result {
            return Ok(ExecutionResult {
                schema,
                batches,
                stats: ExecutionStats {
                    rows_produced,
                    execution_time_ms: 0,
                },
                side_effects,
                mutation_receipt,
            });
        }

        let batch = write_driver::statement_summary_batch(&transaction.state.counters)?;
        Ok(ExecutionResult {
            schema: batch.schema(),
            batches: vec![batch],
            stats: ExecutionStats {
                rows_produced: 1,
                execution_time_ms: 0,
            },
            side_effects,
            mutation_receipt,
        })
    }

    fn validate_deleted_entity_projection(
        plan: &GraphPlan,
        suffix_start: Option<usize>,
        any_entity_deleted: bool,
    ) -> Result<(), GfError> {
        let Some(suffix_start) = suffix_start.filter(|_| any_entity_deleted) else {
            return Ok(());
        };
        let deleted: HashSet<VarId> = plan.ops[..suffix_start]
            .iter()
            .filter_map(|op| match op {
                GraphOp::Delete { vars, .. } => Some(vars.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        if deleted.is_empty() {
            return Ok(());
        }
        let references_deleted = plan.ops[suffix_start..].iter().any(|op| match op {
            GraphOp::Project { items, .. } | GraphOp::With { items, .. } => items
                .iter()
                .any(|item| Self::expr_accesses_deleted_vars(&plan.exprs, item.expr, &deleted)),
            GraphOp::Filter { predicate } => {
                Self::expr_accesses_deleted_vars(&plan.exprs, *predicate, &deleted)
            }
            GraphOp::Sort { keys } => keys
                .iter()
                .any(|key| Self::expr_accesses_deleted_vars(&plan.exprs, key.expr, &deleted)),
            GraphOp::Aggregate { group_by, aggs, .. } => {
                group_by
                    .iter()
                    .any(|expr| Self::expr_accesses_deleted_vars(&plan.exprs, *expr, &deleted))
                    || aggs.iter().any(|agg| {
                        agg.arg.is_some_and(|expr| {
                            Self::expr_accesses_deleted_vars(&plan.exprs, expr, &deleted)
                        })
                    })
            }
            _ => false,
        });
        if references_deleted {
            return Err(GfError::Execution(
                "DeletedEntityAccess: cannot access an entity after it has been deleted".into(),
            ));
        }
        Ok(())
    }

    fn expr_accesses_deleted_vars(arena: &ExprArena, id: ExprId, vars: &HashSet<VarId>) -> bool {
        match arena.get(id) {
            IrExpr::PropertyAccess { base, .. } => Self::expr_references_vars(arena, *base, vars),
            IrExpr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("type") => false,
            IrExpr::FunctionCall { name, args }
                if matches!(
                    name.as_str(),
                    "labels" | "properties" | "_node_struct" | "_rel_struct"
                ) =>
            {
                args.iter()
                    .any(|expr| Self::expr_references_vars(arena, *expr, vars))
            }
            IrExpr::FunctionCall { args, .. } | IrExpr::ListLiteral(args) => args
                .iter()
                .any(|expr| Self::expr_accesses_deleted_vars(arena, *expr, vars)),
            IrExpr::BinaryOp { left, right, .. } => {
                Self::expr_accesses_deleted_vars(arena, *left, vars)
                    || Self::expr_accesses_deleted_vars(arena, *right, vars)
            }
            IrExpr::UnaryOp { expr, .. } => Self::expr_accesses_deleted_vars(arena, *expr, vars),
            IrExpr::MapLiteral(entries) => entries
                .iter()
                .any(|(_, expr)| Self::expr_accesses_deleted_vars(arena, *expr, vars)),
            _ => false,
        }
    }

    fn expr_references_vars(arena: &ExprArena, id: ExprId, vars: &HashSet<VarId>) -> bool {
        match arena.get(id) {
            IrExpr::VarRef(var) => vars.contains(var),
            IrExpr::PropertyAccess { base, .. } => Self::expr_references_vars(arena, *base, vars),
            IrExpr::BinaryOp { left, right, .. } => {
                Self::expr_references_vars(arena, *left, vars)
                    || Self::expr_references_vars(arena, *right, vars)
            }
            IrExpr::UnaryOp { expr, .. } => Self::expr_references_vars(arena, *expr, vars),
            IrExpr::FunctionCall { args, .. } | IrExpr::ListLiteral(args) => args
                .iter()
                .any(|expr| Self::expr_references_vars(arena, *expr, vars)),
            IrExpr::MapLiteral(entries) => entries
                .iter()
                .any(|(_, expr)| Self::expr_references_vars(arena, *expr, vars)),
            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => {
                operand.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
                    || arms.iter().any(|arm| {
                        Self::expr_references_vars(arena, arm.when, vars)
                            || Self::expr_references_vars(arena, arm.then, vars)
                    })
                    || else_expr.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
            }
            IrExpr::Quantifier {
                list, predicate, ..
            } => {
                Self::expr_references_vars(arena, *list, vars)
                    || Self::expr_references_vars(arena, *predicate, vars)
            }
            IrExpr::ListComprehension {
                list,
                filter,
                projection,
                ..
            } => {
                Self::expr_references_vars(arena, *list, vars)
                    || filter.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
                    || projection.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
            }
            IrExpr::Literal(_) | IrExpr::Parameter(_) => false,
        }
    }

    /// Execute a read [`GraphPlan`] and return the result.
    ///
    /// Equivalent to [`execute_plan_with_params`](Self::execute_plan_with_params)
    /// with no parameters.
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, physical planning, or execution fails.
    pub async fn execute_plan(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_plan_with_params(plan, &HashMap::new()).await
    }

    /// Execute a read [`GraphPlan`], substituting `$name` placeholders with the
    /// supplied parameter values, and return the result.
    ///
    /// Lowers the plan to a DataFusion `LogicalPlan` (scans bound to the
    /// project's Parquet-backed catalog tables), replaces any query-parameter
    /// placeholders with the values in `params` (by name; a placeholder with no
    /// provided value errors), physically plans it through the custom
    /// [`GraphForgeQueryPlanner`], and collects the resulting batches.
    /// `CREATE` plans should use [`execute_create`](Self::execute_create).
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, parameter binding, physical planning, or
    /// execution fails.
    pub async fn execute_plan_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        let resolved_plan = self.resolve_row_count_expressions(plan, params).await?;
        let (physical, fallback_schema) = self.plan_physical(&resolved_plan, params).await?;

        let task_ctx = self.ctx.task_ctx();
        let memory_reserved_before = task_ctx.memory_pool().reserved();
        let collected =
            path_hydration::collect_guarded(Arc::clone(&physical), Arc::clone(&task_ctx)).await;
        path_hydration::cancel_plan(&physical);
        let memory_reserved_after = task_ctx.memory_pool().reserved();
        let returned_batch_bytes = collected.as_ref().map_or(0, |batches| {
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::get_array_memory_size)
                .sum()
        });
        demand::record_plan_completion(
            &physical,
            memory_reserved_before,
            memory_reserved_after,
            returned_batch_bytes,
            task_ctx.session_config().batch_size(),
        );
        let mut batches = collected.map_err(GfError::from_execution_error)?;

        // DataFusion's collect may return zero batches for an empty stream.
        // Public callers (and DF54-era optimistic publish tests) index
        // `batches[0]` for schema/row counts; mirror the write-path terminal
        // suffix and always surface one empty batch with the plan schema.
        let schema = batches
            .first()
            .map_or_else(|| fallback_schema, RecordBatch::schema);
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(&schema)));
        }
        let rows_produced = batches.iter().map(|b| b.num_rows() as u64).sum();
        Ok(ExecutionResult {
            schema,
            batches,
            stats: ExecutionStats {
                rows_produced,
                execution_time_ms: 0,
            },
            side_effects: None,
            mutation_receipt: None,
        })
    }

    async fn resolve_row_count_expressions(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<GraphPlan, GfError> {
        let mut resolved = plan.clone();
        for op in &mut resolved.ops {
            match op {
                GraphOp::Optional { child }
                | GraphOp::Exists { child, .. }
                | GraphOp::PatternComprehension { child, .. }
                | GraphOp::ListElementPatternComprehension { child, .. } => {
                    **child = Box::pin(self.resolve_row_count_expressions(child, params)).await?;
                }
                GraphOp::Union { inputs, .. } => {
                    for input in inputs {
                        *input =
                            Box::pin(self.resolve_row_count_expressions(input, params)).await?;
                    }
                }
                _ => {}
            }
        }
        let exprs = resolved.exprs.clone();
        for op in &mut resolved.ops {
            let (keyword, expr) = match op {
                GraphOp::SkipExpr { expr } => ("SKIP", *expr),
                GraphOp::LimitExpr { expr } => ("LIMIT", *expr),
                _ => continue,
            };
            let vars = graphforge_rel::VarMap::new();
            let lowerer = graphforge_rel::ExprLowerer::new(&exprs, self.ontology.as_ref(), &vars);
            let expr = lowerer.lower(expr).map_err(GfError::from_plan_error)?;
            let logical = LogicalPlanBuilder::empty(true)
                .project(vec![expr.alias("__gf_row_count")])
                .and_then(LogicalPlanBuilder::build)
                .map_err(GfError::from_plan_error)?;
            let logical = bind_query_params(logical, params)?;
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&logical)
                .await
                .map_err(GfError::from_plan_error)?;
            let batches = path_hydration::collect_guarded(physical, self.ctx.task_ctx())
                .await
                .map_err(GfError::from_execution_error)?;
            let batch = batches.first().ok_or_else(|| {
                GfError::Execution(format!("{keyword} expression returned no row"))
            })?;
            let value = ScalarValue::try_from_array(batch.column(0), 0)
                .map_err(GfError::from_execution_error)?;
            let count = match value {
                ScalarValue::Int64(Some(value)) => u64::try_from(value).ok(),
                ScalarValue::UInt64(Some(value)) => Some(value),
                ScalarValue::Int32(Some(value)) => u64::try_from(value).ok(),
                ScalarValue::UInt32(Some(value)) => Some(u64::from(value)),
                _ => None,
            }
            .ok_or_else(|| {
                GfError::Execution(format!(
                    "{keyword} expression must evaluate to a non-negative integer"
                ))
            })?;
            *op = match keyword {
                "SKIP" => GraphOp::Skip { count },
                _ => GraphOp::Limit { count },
            };
        }
        Ok(resolved)
    }

    /// Execute a read [`GraphPlan`] and return a lazy stream of result batches,
    /// substituting `$name` placeholders with `params`.
    ///
    /// The streaming counterpart of
    /// [`execute_plan_with_params`](Self::execute_plan_with_params): it builds
    /// the same physical plan but drives it incrementally via DataFusion's
    /// [`execute_stream`] rather than collecting eagerly. Output shaping
    /// (UUID-only columns, schema metadata) is applied by the caller per batch.
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, parameter binding, or physical planning
    /// fails. Per-batch execution errors surface on the returned stream.
    pub async fn execute_plan_stream(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<SendableRecordBatchStream, GfError> {
        let (physical, _) = self.plan_physical(plan, params).await?;
        let task_ctx = self.ctx.task_ctx();
        let memory_reserved_before = task_ctx.memory_pool().reserved();
        let stream =
            datafusion::physical_plan::execute_stream(Arc::clone(&physical), Arc::clone(&task_ctx))
                .map_err(GfError::from_execution_error)?;
        let schema = stream.schema();
        Ok(self
            .mutation_health
            .guard_stream(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                QueryEvidenceStream {
                    inner: Some(stream),
                    physical,
                    task_ctx,
                    memory_reserved_before,
                    returned_batch_bytes: 0,
                    finalized: false,
                },
            ))))
    }

    /// Render the physical plan for a [`GraphPlan`] (indented, one line per
    /// node) without executing it.
    ///
    /// Write plans (`CREATE` / `MERGE` / `DELETE` / `SET` / `REMOVE`) lower via
    /// [`GraphPlanLowerer::new_for_writes`] and stop after
    /// `create_physical_plan` — never `collect` — so EXPLAIN shows the write
    /// path without publishing mutations. Read plans reuse [`Self::plan_physical`].
    ///
    /// This is the physical-plan inspection surface: node lines carry
    /// execution detail the logical stages cannot show, such as
    /// `adjacency=hit | miss | building` on traversal nodes (#761).
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering or physical planning fails.
    pub async fn explain_physical(&self, plan: &GraphPlan) -> Result<String, GfError> {
        if plan_requires_writes(plan) {
            if self.dir.as_os_str().is_empty() {
                return Err(GfError::Execution(
                    "explain of a write plan requires a write target; \
                     build the session with new_with_target"
                        .into(),
                ));
            }
            let lowerer = GraphPlanLowerer::new_for_writes(
                &graphforge_storage::lowering_snapshot(Some(&self.catalog), Some(&self.dir))?,
                self.ontology.as_ref(),
                self.mode,
            )?;
            let logical = lowerer.lower_plan(plan)?;
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&logical)
                .await
                .map_err(GfError::from_plan_error)?;
            return Ok(datafusion::physical_plan::displayable(physical.as_ref())
                .indent(false)
                .to_string());
        }
        let (physical, _) = self.plan_physical(plan, &HashMap::new()).await?;
        Ok(datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string())
    }

    /// Lower `plan`, bind `$name` parameters, and build the physical plan —
    /// shared by the collecting and streaming read paths. Returns the physical
    /// plan and the logical plan's schema (a fallback for an empty result).
    async fn plan_physical(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<(Arc<dyn ExecutionPlan>, SchemaRef), GfError> {
        self.mutation_health.check()?;
        // Read lowering always needs the catalog (typed-vs-exploratory edge
        // routing). Scans additionally bind their real Parquet-backed providers
        // from the project directory. A read-only session built via `new` has an
        // empty `dir`: binding a scan there would resolve a CWD-relative path
        // like `topology/nodes.parquet`, silently reading the wrong file. Such a
        // session cannot read persisted data, so reject scan plans up front
        // (mirroring `execute_create`'s write-target guard) and lower the rest
        // schema-only so pure computed/`RETURN` plans still run.
        let lowerer = if self.dir.as_os_str().is_empty() {
            if plan_reads_persisted_data(plan) {
                return Err(GfError::Execution(
                    "execute_plan requires a project directory to read persisted nodes/edges; \
                     build the session with new_with_target"
                        .into(),
                ));
            }
            GraphPlanLowerer::new(
                Some(&graphforge_storage::lowering_snapshot(
                    Some(&self.catalog),
                    None,
                )?),
                self.ontology.as_ref(),
            )?
        } else {
            GraphPlanLowerer::new_for_reads(
                &graphforge_storage::lowering_snapshot(Some(&self.catalog), Some(&self.dir))?,
                self.ontology.as_ref(),
                self.mode,
            )?
        };
        #[cfg(feature = "differential-testing")]
        let lowerer = if self.relational_fixed_hop_reference {
            lowerer.with_relational_fixed_hop_reference()
        } else {
            lowerer
        };
        let logical = lowerer.lower_plan(plan)?;

        // Bind `$name` query parameters to their values, replacing the
        // DataFusion placeholders with literals. Skipped when there are no
        // params (the common read path) — `with_param_values` would otherwise
        // walk the plan needlessly; with params, a placeholder lacking a value
        // surfaces as a clear `GfError::Plan`.
        let logical = bind_query_params(logical, params)?;

        let fallback_schema = physical_schema_fallback(&logical);
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(GfError::from_plan_error)?;
        Ok((physical, fallback_schema))
    }

    /// Return a reference to the underlying DataFusion [`SessionContext`].
    #[must_use]
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

fn lower_write_terminal_suffix(
    lowerer: &GraphPlanLowerer,
    plan: &GraphPlan,
    start: Option<usize>,
    frontier: &write_driver::Frontier,
    var_map: &graphforge_rel::VarMap,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
    pending_nodes: &RecordBatch,
) -> Result<Option<Box<LogicalPlan>>, GfError> {
    let Some(start) = start else {
        return Ok(None);
    };
    let mut suffix_vars = var_map.clone();
    let logical = lowerer.lower_write_segment(
        &plan.ops[start..],
        &plan.exprs,
        &mut suffix_vars,
        Arc::new(frontier.df_schema.clone()),
        pending_nodes,
    )?;
    Ok(Some(Box::new(bind_query_params(logical, params)?)))
}

#[allow(clippy::too_many_arguments)]
async fn run_write_relational_segment(
    session: &SessionContext,
    lowerer: &GraphPlanLowerer,
    plan: &GraphPlan,
    range: std::ops::Range<usize>,
    frontier: &mut write_driver::Frontier,
    var_map: &mut graphforge_rel::VarMap,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
    pending_nodes: &RecordBatch,
) -> Result<(), GfError> {
    let logical = lowerer.lower_write_segment(
        &plan.ops[range],
        &plan.exprs,
        var_map,
        Arc::new(frontier.df_schema.clone()),
        pending_nodes,
    )?;
    let logical = bind_query_params(logical, params)?;
    let df_schema = logical.schema().as_ref().clone();
    let (_, batches, _) = write_driver::run_terminal_suffix(session, &logical, frontier).await?;
    frontier.df_schema = df_schema;
    frontier.batches = batches;
    Ok(())
}

fn bind_query_params(
    logical: LogicalPlan,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
) -> Result<LogicalPlan, GfError> {
    // Always apply param substitution — including an empty map — so unbound
    // `$name` placeholders fail at plan time instead of depending on whether
    // the optimizer happens to evaluate the expression (empty-graph scans can
    // otherwise skip the placeholder and silently return zero rows).
    let values: HashMap<String, datafusion::scalar::ScalarValue> = params
        .iter()
        .map(|(name, lit)| (name.clone(), graphforge_rel::ir_literal_to_scalar(lit)))
        .collect();
    logical
        .with_param_values(values)
        .map_err(GfError::from_plan_error)
}

#[cfg(test)]
mod tests;
