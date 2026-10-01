//! Lowerer-chosen fast-path node (ADR 0050).
//!
//! The lowerer emits this node when the Graph IR of a whole statement has one
//! of the shapes the adjacency fast paths answer. The generic lowering is kept
//! as the only input, so physical planning either replaces the node with the
//! fast operator or plans the input and names the reason it could not.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result as DfResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::passthrough_schema;

/// Which fast operator a [`FastPathNode`] stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FastPathKind {
    /// `MATCH ()-[r]->() RETURN count(r)` over every node.
    EdgeCount,
    /// One hop `ORDER BY` destination `node_uuid` `LIMIT fetch`.
    OrderedOneHop {
        /// The terminal limit.
        fetch: usize,
    },
    /// Two hops of one relation type, `ORDER BY` destination `node_uuid` `LIMIT fetch`.
    OrderedTwoHop {
        /// The terminal limit.
        fetch: usize,
        /// Whether the pattern requires the two relationships to differ.
        require_edge_disjoint: bool,
    },
}

/// Logical wrapper over a generic plan whose shape a fast operator answers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FastPathNode {
    /// The generic lowering of the same statement.
    pub input: Arc<LogicalPlan>,
    /// The fast operator this statement qualifies for.
    pub kind: FastPathKind,
    schema: DFSchemaRef,
}

impl FastPathNode {
    /// Wrap `input`, the generic lowering of a statement of shape `kind`.
    #[must_use]
    pub fn new(input: Arc<LogicalPlan>, kind: FastPathKind) -> Self {
        let schema = passthrough_schema(&[&input]);
        Self {
            input,
            kind,
            schema,
        }
    }
}

impl_partial_ord!(FastPathNode);

impl UserDefinedLogicalNodeCore for FastPathNode {
    fn name(&self) -> &str {
        "FastPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FastPath: kind={:?}", self.kind)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        Ok(Self::new(input, self.kind))
    }
}
