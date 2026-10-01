//! #1688 read-path candidates for the #1619 comparison.
//!
//! - **A, current:** `ExpandExec`, with `FixedHopDemandRule` substituting the
//!   fast operators when it recognizes the physical plan shape.
//! - **B, stock:** the relational fixed-hop lowering (hash joins, stock
//!   aggregate and top-K sort); the physical rewrites are off.
//! - **C, structural:** the lowerer chooses the fast operators from the Graph IR
//!   ([`graphforge_plan::fast_path::FastPathNode`]); the physical rewrites are
//!   off. A precondition failure at planning keeps the generic plan under a
//!   visible [`FastPathFallbackExec`] that names the reason.
//!
//! The candidate is read once per session from [`READ_PATH_CANDIDATE_ENV`].
//! Only builds with the non-default `read-path-experiment` feature have it.

use std::fmt;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionConfig;
use graphforge_core::GfError;
use graphforge_ir::Direction;
use graphforge_plan::ExpandNode;
use graphforge_plan::fast_path::{FastPathKind, FastPathNode};

use crate::edge_count::EdgeCountExec;
use crate::ordered_one_hop::OrderedOneHopExec;
use crate::ordered_two_hop::OrderedTwoHopPathCountExec;
use crate::session::{AdjacencyProviderExt, OrdinalIdentityResolverExt};
use crate::{ExpandExec, V4OrdinalIdentitySession};

/// Environment variable selecting the read-path candidate: `current`, `stock`,
/// or `structural`. Unset means `current`; any other value fails the session.
pub const READ_PATH_CANDIDATE_ENV: &str = "GF_READ_PATH_CANDIDATE";

/// Environment variable that injects a plan-shape change for the #1688
/// fallback test: `between-expands` places a one-partition `RepartitionExec`
/// between two stacked `ExpandExec`s before `FixedHopDemandRule` runs.
pub const READ_PATH_INJECT_ENV: &str = "GF_READ_PATH_INJECT";

/// Read [`READ_PATH_INJECT_ENV`]: whether to inject the transport operator.
///
/// # Errors
/// Returns [`GfError::Execution`] for a value that names no injection.
pub(crate) fn inject_transport_from_env() -> Result<bool, GfError> {
    match std::env::var(READ_PATH_INJECT_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "between-expands" => Ok(true),
        Ok(other) => Err(GfError::Execution(format!(
            "{READ_PATH_INJECT_ENV}={other:?} names no injection; use between-expands"
        ))),
        Err(error) => Err(GfError::Execution(format!(
            "{READ_PATH_INJECT_ENV}: {error}"
        ))),
    }
}

/// Places a one-partition `RepartitionExec` between stacked `ExpandExec`s: a
/// row-preserving transport operator the physical fast-path rewrites do not
/// pass through, used as the known positive for silent fallback.
#[derive(Debug)]
pub(crate) struct TransportBetweenExpands;

impl PhysicalOptimizerRule for TransportBetweenExpands {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_up(|node| {
            let child = node.children().first().map(|child| Arc::clone(child));
            match child {
                Some(child)
                    if node.downcast_ref::<ExpandExec>().is_some()
                        && child.downcast_ref::<ExpandExec>().is_some() =>
                {
                    let transport: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
                        child,
                        Partitioning::RoundRobinBatch(1),
                    )?);
                    Ok(Transformed::yes(node.with_new_children(vec![transport])?))
                }
                _ => Ok(Transformed::no(node)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "read_path_transport_between_expands"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A #1619 read-path candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPathCandidate {
    /// A: physical fast-path rewrites over `ExpandExec`.
    Current,
    /// B: relational fixed hops and stock operators.
    Stock,
    /// C: fast paths chosen from the Graph IR.
    Structural,
}

impl ReadPathCandidate {
    /// Read the candidate from [`READ_PATH_CANDIDATE_ENV`].
    ///
    /// # Errors
    /// Returns [`GfError::Execution`] for a value that names no candidate.
    pub fn from_env() -> Result<Self, GfError> {
        match std::env::var(READ_PATH_CANDIDATE_ENV) {
            Err(std::env::VarError::NotPresent) => Ok(Self::Current),
            Ok(value) => match value.as_str() {
                "current" => Ok(Self::Current),
                "stock" => Ok(Self::Stock),
                "structural" => Ok(Self::Structural),
                other => Err(GfError::Execution(format!(
                    "{READ_PATH_CANDIDATE_ENV}={other:?} names no read-path candidate; \
                     use current, stock, or structural"
                ))),
            },
            Err(error) => Err(GfError::Execution(format!(
                "{READ_PATH_CANDIDATE_ENV}: {error}"
            ))),
        }
    }

    /// Whether `FixedHopDemandRule` substitutes fast operators by plan shape.
    pub(crate) fn rewrites_physical_fast_paths(self) -> bool {
        self == Self::Current
    }
}

/// Read the candidate and injection settings for a new session: register the
/// session's markers in `config` and return the demand rule it plans with.
///
/// # Errors
/// Returns [`GfError::Execution`] for an unrecognized environment value.
pub(crate) fn configure_session(
    config: &mut SessionConfig,
) -> Result<(ReadPathCandidate, crate::demand::FixedHopDemandRule), GfError> {
    let candidate = ReadPathCandidate::from_env()?;
    let inject_transport = inject_transport_from_env()?;
    if candidate == ReadPathCandidate::Structural {
        config.set_extension(Arc::new(ExpandPoolAccounting));
    }
    Ok((
        candidate,
        crate::demand::FixedHopDemandRule::for_candidate(candidate, inject_transport),
    ))
}

/// Session marker: `ExpandExec` charges its held batches to the memory pool.
#[derive(Debug)]
pub(crate) struct ExpandPoolAccounting;

/// The reservation an `ExpandExec` stream charges, when the session asks for one.
pub(crate) fn expand_reservation(context: &TaskContext) -> Option<MemoryReservation> {
    context
        .session_config()
        .get_extension::<ExpandPoolAccounting>()
        .map(|_| MemoryConsumer::new("ExpandExec").register(context.memory_pool()))
}

/// Plan `node` when it is a [`FastPathNode`]: the fast operator, or the generic
/// input under a [`FastPathFallbackExec`] naming the precondition that failed.
/// `None` for every other node.
pub(crate) fn plan_extension(
    node: &dyn UserDefinedLogicalNode,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    session_state: &SessionState,
) -> Option<Result<Arc<dyn ExecutionPlan>, DataFusionError>> {
    let fast = node.as_any().downcast_ref::<FastPathNode>()?;
    let (Some(logical), Some(input)) = (logical_inputs.first(), physical_inputs.first()) else {
        return Some(Err(DataFusionError::Internal(
            "FastPath requires one input".into(),
        )));
    };
    Some(Ok(match choose(fast.kind, logical, input, session_state) {
        Ok(fast) => fast,
        Err(reason) => Arc::new(FastPathFallbackExec::new(Arc::clone(input), reason)),
    }))
}

fn choose(
    kind: FastPathKind,
    logical_input: &LogicalPlan,
    input: &Arc<dyn ExecutionPlan>,
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>, String> {
    let expands = provider_expands(logical_input);
    let provider = session_state
        .config()
        .get_extension::<AdjacencyProviderExt>()
        .ok_or("the session has no adjacency provider")?
        .0
        .clone();
    let schema = input.schema();
    let props = Arc::new(
        input
            .properties()
            .as_ref()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(1)),
    );
    match kind {
        FastPathKind::EdgeCount => {
            let expand = single_out_expand(&expands)?;
            Ok(Arc::new(EdgeCountExec::from_parts(
                schema,
                props,
                expand.rel_type_name.clone(),
                expand.direction,
                provider,
            )))
        }
        FastPathKind::OrderedOneHop { fetch } => {
            let expand = single_out_expand(&expands)?;
            Ok(Arc::new(OrderedOneHopExec::from_parts(
                schema,
                props,
                fetch,
                expand.rel_type_name.clone(),
                expand.direction,
                provider,
                ordered_identities(session_state)?,
            )))
        }
        FastPathKind::OrderedTwoHop {
            fetch,
            require_edge_disjoint,
        } => {
            let [first, second] = expands.as_slice() else {
                return Err(format!("{} provider expands, expected 2", expands.len()));
            };
            if first.rel_type_name != second.rel_type_name
                || [first.direction, second.direction] != [Direction::Out, Direction::Out]
            {
                return Err("the two hops differ in relation type or direction".into());
            }
            Ok(Arc::new(OrderedTwoHopPathCountExec::from_parts(
                schema,
                props,
                fetch,
                second.rel_type_name.clone(),
                provider,
                ordered_identities(session_state)?,
                require_edge_disjoint,
            )))
        }
    }
}

/// The adjacency-backed expands the generic lowering produced.
fn provider_expands(plan: &LogicalPlan) -> Vec<ExpandNode> {
    let mut expands = Vec::new();
    plan.apply(|plan| {
        if let LogicalPlan::Extension(extension) = plan
            && let Some(expand) = extension.node.as_any().downcast_ref::<ExpandNode>()
        {
            expands.push(expand.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting expands cannot fail");
    expands
}

fn single_out_expand(expands: &[ExpandNode]) -> Result<&ExpandNode, String> {
    match expands {
        [expand] if expand.direction == Direction::Out => Ok(expand),
        [_] => Err("the hop is not outgoing".into()),
        _ => Err(format!("{} provider expands, expected 1", expands.len())),
    }
}

/// The session's ordinal identities, when node-ordinal order is UUID order.
fn ordered_identities(
    session_state: &SessionState,
) -> Result<Arc<V4OrdinalIdentitySession>, String> {
    let identities = session_state
        .config()
        .get_extension::<OrdinalIdentityResolverExt>()
        .and_then(|extension| extension.0.clone())
        .ok_or("the session has no ordinal identity authority")?;
    if !identities.uuid_order_matches_ordinals() {
        return Err("node-ordinal order is not UUID order".into());
    }
    Ok(identities)
}

/// The generic plan for a statement whose fast path a precondition refused.
pub struct FastPathFallbackExec {
    input: Arc<dyn ExecutionPlan>,
    reason: String,
}

impl FastPathFallbackExec {
    fn new(input: Arc<dyn ExecutionPlan>, reason: String) -> Self {
        Self { input, reason }
    }

    /// Why the fast operator was not planned.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Debug for FastPathFallbackExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FastPathFallbackExec {{ reason: {} }}", self.reason)
    }
}

impl DisplayAs for FastPathFallbackExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FastPathFallbackExec: reason={}", self.reason)
    }
}

impl ExecutionPlan for FastPathFallbackExec {
    fn name(&self) -> &str {
        "FastPathFallbackExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("FastPathFallbackExec needs one child".into())
        })?;
        Ok(Arc::new(Self::new(input, self.reason.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.input.execute(partition, context)
    }
}
