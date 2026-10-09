//! Drop a property-route join whose columns nothing reads (#1931).
//!
//! `MATCH (a)-[:R]->(b) RETURN count(*)` joins `b` to its property route "for
//! its properties", then reads none of them. The join is a left join on the
//! route's identity key, which a route holds at most once, so it can neither
//! add nor remove a row. It could only cost: executing it authenticates and
//! decodes the key column of every fragment of the route, whatever the query
//! asks of the graph. Expanding from one anchor therefore cost the size of the
//! destination's whole property route.
//!
//! The rule removes exactly that join and nothing else. A route whose value any
//! operator above reads is still scanned, authenticated and joined.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Projection};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use graphforge_plan::{GraphReadSource, GraphReadTable};

/// Remove a left join onto a property route when no right column is read.
#[derive(Debug)]
pub struct UnusedRouteJoin;

/// The identity column a route is keyed by, when `table` is a property route.
fn route_key(table: &GraphReadTable) -> Option<&'static str> {
    match table {
        GraphReadTable::Properties(_) | GraphReadTable::PropertyKeys(_) => Some("node_uuid"),
        GraphReadTable::EdgeProperties(..) | GraphReadTable::EdgePropertyKeys(_) => {
            Some("edge_uuid")
        }
        _ => None,
    }
}

impl OptimizerRule for UnusedRouteJoin {
    fn name(&self) -> &'static str {
        "graphforge_unused_route_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Join(join) = projection.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Left {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::TableScan(scan) = join.right.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(key) = scan
            .source
            .downcast_ref::<GraphReadSource>()
            .and_then(|source| route_key(&source.table))
        else {
            return Ok(Transformed::no(plan));
        };
        // At most one right row per left row: the join is on the route's key.
        let on_key = join.on.iter().any(|(_, right)| {
            matches!(right, Expr::Column(column)
                if column.name == key && join.right.schema().has_column(column))
        });
        if !on_key {
            return Ok(Transformed::no(plan));
        }
        let reads_route = projection
            .expr
            .iter()
            .flat_map(Expr::column_refs)
            .any(|column| join.right.schema().has_column(column));
        if reads_route {
            return Ok(Transformed::no(plan));
        }
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(projection.expr.clone(), Arc::clone(&join.left))?,
        )))
    }
}

#[cfg(test)]
mod tests;
