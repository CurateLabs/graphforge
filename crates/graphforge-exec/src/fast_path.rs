//! Lowerer-chosen fast paths (ADR 0050).
//!
//! The lowerer wraps a statement whose Graph IR one of the adjacency fast
//! operators answers in a [`graphforge_plan::fast_path::FastPathNode`]. This
//! module plans that node into `EdgeCountExec`, `OrderedOneHopExec` or
//! `OrderedTwoHopPathCountExec`. When a session precondition fails it keeps the
//! generic plan under a visible [`FastPathFallbackExec`] that names the reason.
//! The choice never reads the physical plan, so DataFusion's partitioning and
//! transport operators cannot remove a fast path (#1513).

use std::fmt;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use graphforge_ir::Direction;
use graphforge_plan::ExpandNode;
use graphforge_plan::fast_path::{FastPathKind, FastPathNode};

use crate::V4OrdinalIdentitySession;
use crate::edge_count::EdgeCountExec;
use crate::ordered_one_hop::OrderedOneHopExec;
use crate::ordered_two_hop::OrderedTwoHopPathCountExec;
use crate::session::{AdjacencyProviderExt, OrdinalIdentityResolverExt};

/// The memory-pool reservation an `ExpandExec` stream charges its held batches to.
pub(crate) fn expand_reservation(context: &TaskContext) -> MemoryReservation {
    MemoryConsumer::new("ExpandExec").register(context.memory_pool())
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
