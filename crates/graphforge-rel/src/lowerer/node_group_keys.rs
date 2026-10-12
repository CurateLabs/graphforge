//! Group keys over a node variable (#1985).
//!
//! A node variable spans its `node_uuid`, topology columns and every property
//! column, and the `node_uuid` determines the rest: two rows are the same node
//! exactly when their identity agrees. An aggregate or `DISTINCT` that lists
//! each column as a group key makes a variable with twenty-four property columns
//! a twenty-four-column key, and DataFusion cannot prune a group key even when
//! nothing reads it, so every operator above carries the whole variable.
//!
//! The grouping is keyed by the `node_uuid` and carries every other column of
//! the variable as a `first_value` aggregate. Every row of a group is the same
//! node, so the first value is that node's value, bit for bit and whatever the
//! column type; two nodes with equal properties stay two groups. The projection
//! pushdown then drops the carried columns the rest of the query never reads,
//! and a projection above restores the columns, qualifiers and order the
//! operators above expect.
//!
//! The dependency is not declared as a functional dependency of the plan:
//! DataFusion rebuilds the plan when it binds parameters and drops dependencies
//! a projection declares, so only a grouping that does not rely on them keeps
//! the narrow key.

use datafusion::common::{Column, TableReference};
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlan, LogicalPlanBuilder};

use super::MapUnsupportedExpr;
use crate::expr::LoweringError;

/// The column that identifies a node within its variable.
const IDENTITY_COLUMN: &str = "node_uuid";

/// Prefix of the aggregate that carries one dependent column.
const CARRIED_PREFIX: &str = "__gf_node_column_";

/// Where one column of the original grouping is produced.
enum Slot {
    /// The next group key.
    Key,
    /// The next carried column.
    Carried,
}

/// One column of the output that restores the original grouping: a column of
/// the aggregate and, for a carried column, the qualified name it returns as.
pub(super) struct Output<'a> {
    /// Position in the aggregate's schema.
    pub(super) index: usize,
    /// The name a carried column takes again; `None` for a key or an aggregate.
    pub(super) restored: Option<&'a Column>,
}

/// A grouping rewritten to key a node variable by its identity.
pub(super) struct NodeGroupKeys {
    keys: Vec<DfExpr>,
    carried: Vec<DfExpr>,
    restored: Vec<Column>,
    slots: Vec<Slot>,
}

impl NodeGroupKeys {
    /// Rewrites `group` when it lists the `node_uuid` of a variable together
    /// with other plain columns of that variable; `None` when there is nothing
    /// to carry.
    pub(super) fn new(group: &[DfExpr]) -> Option<Self> {
        let identities: Vec<&TableReference> = group
            .iter()
            .filter_map(|expr| match expr {
                DfExpr::Column(column) if column.name == IDENTITY_COLUMN => {
                    column.relation.as_ref()
                }
                _ => None,
            })
            .collect();
        if identities.is_empty() {
            return None;
        }
        let mut keys = Vec::new();
        let mut carried = Vec::new();
        let mut restored = Vec::new();
        let mut slots = Vec::with_capacity(group.len());
        for expr in group {
            match expr {
                DfExpr::Column(column)
                    if column.name != IDENTITY_COLUMN
                        && column
                            .relation
                            .as_ref()
                            .is_some_and(|relation| identities.contains(&relation)) =>
                {
                    carried.push(
                        first_value(DfExpr::Column(column.clone()), vec![])
                            .alias(format!("{CARRIED_PREFIX}{}", restored.len())),
                    );
                    restored.push(column.clone());
                    slots.push(Slot::Carried);
                }
                other => {
                    keys.push(other.clone());
                    slots.push(Slot::Key);
                }
            }
        }
        if carried.is_empty() {
            return None;
        }
        Some(Self {
            keys,
            carried,
            restored,
            slots,
        })
    }

    /// The group keys of the aggregate.
    pub(super) fn keys(&self) -> &[DfExpr] {
        &self.keys
    }

    /// The aggregates that carry the dependent columns; they follow the
    /// query's own aggregates in the aggregate's schema.
    pub(super) fn carried(&self) -> &[DfExpr] {
        &self.carried
    }

    /// The columns of an aggregate over [`Self::keys`], the query's
    /// `aggregate_count` aggregates and [`Self::carried`], in the order of the
    /// original grouping followed by the query's aggregates.
    pub(super) fn outputs(&self, aggregate_count: usize) -> Vec<Output<'_>> {
        let carried_start = self.keys.len() + aggregate_count;
        let (mut key, mut carried) = (0, 0);
        let mut outputs = Vec::with_capacity(self.slots.len() + aggregate_count);
        for slot in &self.slots {
            match slot {
                Slot::Key => {
                    outputs.push(Output {
                        index: key,
                        restored: None,
                    });
                    key += 1;
                }
                Slot::Carried => {
                    outputs.push(Output {
                        index: carried_start + carried,
                        restored: Some(&self.restored[carried]),
                    });
                    carried += 1;
                }
            }
        }
        outputs.extend((self.keys.len()..carried_start).map(|index| Output {
            index,
            restored: None,
        }));
        outputs
    }
}

/// The column that returns a carried column under its original qualified name.
pub(super) fn restored_column(column: DfExpr, restored: &Column) -> DfExpr {
    column.alias_qualified(restored.relation.clone(), restored.name.clone())
}

/// `DISTINCT` over every column of `input`, keyed by node identity where the
/// input carries a node variable.
pub(super) fn distinct(input: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    let columns: Vec<DfExpr> = input
        .schema()
        .iter()
        .map(|(qualifier, field)| DfExpr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect();
    let Some(grouping) = NodeGroupKeys::new(&columns) else {
        return LogicalPlanBuilder::from(input)
            .distinct()
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr();
    };
    let aggregate = LogicalPlanBuilder::from(input)
        .aggregate(grouping.keys().to_vec(), grouping.carried().to_vec())
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;
    let projection: Vec<DfExpr> = grouping
        .outputs(0)
        .into_iter()
        .map(|output| {
            let (qualifier, field) = aggregate.schema().qualified_field(output.index);
            let column = DfExpr::Column(Column::new(qualifier.cloned(), field.name()));
            match output.restored {
                Some(restored) => restored_column(column, restored),
                None => column,
            }
        })
        .collect();
    LogicalPlanBuilder::from(aggregate)
        .project(projection)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}
