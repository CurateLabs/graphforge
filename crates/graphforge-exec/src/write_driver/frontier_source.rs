//! The rows a write statement carries into its next relational segment.
//!
//! A write segment is lowered over a one-row `EmptyRelation` that stands for
//! the frontier: the rows the earlier segments produced. Before planning, the
//! driver replaces that slot with [`FrontierSourceNode`], a leaf that holds
//! the frontier batches and plans to an in-memory source.
//!
//! Substituting after physical planning is unsound. A one-row relation with
//! no columns is the identity of a cross join, so DataFusion 56's
//! `EliminateConstantJoin` removes it when, say, `WITH *` carries no named
//! variables (Create3 `[3] MATCH-CREATE-WITH-CREATE`, #2042). The frontier's
//! row multiplicity then never reaches the plan. With the real rows in place
//! the optimizers see their actual cardinality.

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;

use arrow::array::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;
use graphforge_core::GfError;

use super::Frontier;

static NEXT_FRONTIER_SOURCE: AtomicU64 = AtomicU64::new(0);

/// A leaf holding the frontier rows of a write statement.
///
/// Equality and hashing use a process-unique id: two sources are the same
/// node only when one is a clone of the other.
#[derive(Clone)]
pub(crate) struct FrontierSourceNode {
    id: u64,
    schema: DFSchemaRef,
    batches: Arc<[RecordBatch]>,
}

impl FrontierSourceNode {
    fn new(schema: DFSchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            id: NEXT_FRONTIER_SOURCE.fetch_add(1, AtomicOrdering::Relaxed),
            schema,
            batches: batches.into(),
        }
    }

    /// The in-memory source over the frontier rows.
    /// The batches keep their own schema, as the frontier produced them; an
    /// empty frontier is one empty batch of the segment's input schema.
    pub(crate) fn plan(&self) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (schema, batches) = if let Some(first) = self.batches.first() {
            (first.schema(), self.batches.to_vec())
        } else {
            let schema = Arc::new(self.schema.as_arrow().clone());
            (Arc::clone(&schema), vec![RecordBatch::new_empty(schema)])
        };
        Ok(MemorySourceConfig::try_new_from_batches(schema, batches)?)
    }

    fn rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

impl fmt::Debug for FrontierSourceNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrontierSourceNode")
            .field("id", &self.id)
            .field("rows", &self.rows())
            .finish_non_exhaustive()
    }
}

impl PartialEq for FrontierSourceNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for FrontierSourceNode {}

impl Hash for FrontierSourceNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialOrd for FrontierSourceNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl UserDefinedLogicalNodeCore for FrontierSourceNode {
    fn name(&self) -> &str {
        "FrontierSource"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FrontierSource: rows={}", self.rows())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(self.clone())
    }
}

/// Replace the frontier slot of a lowered write segment, the one-row
/// `EmptyRelation` with the frontier's schema, with the frontier rows.
pub(super) fn bind_frontier(
    logical: &LogicalPlan,
    frontier: &Frontier,
) -> Result<LogicalPlan, GfError> {
    logical
        .clone()
        .transform_up(|plan| {
            let LogicalPlan::EmptyRelation(empty) = &plan else {
                return Ok(Transformed::no(plan));
            };
            if !empty.produce_one_row || empty.schema.as_ref() != &frontier.df_schema {
                return Ok(Transformed::no(plan));
            }
            let source =
                FrontierSourceNode::new(Arc::clone(&empty.schema), frontier.batches.clone());
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(source),
            })))
        })
        .map(|transformed| transformed.data)
        .map_err(GfError::from_plan_error)
}
