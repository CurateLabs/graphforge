//! Hint a stored-property equality to the scan that can answer it (#1931).
//!
//! `MATCH (a:Account {id: 7})` compiles to a filter over a left join of the
//! node scan with its property route. The filter reaches the join only after
//! the planner has moved it across the expansion, too late for DataFusion's own
//! pass to push it any further, so the route scan read every row and the filter
//! discarded all but the match.
//!
//! This rule runs last and offers each `column = literal` conjunct to the
//! property scan that supplies the column, through the `Properties` source's
//! own pushdown declaration. A hint is not a rewrite: the filter stays where it
//! is and still decides which rows survive. The scan may use the hint to return
//! fewer rows, but only rows the filter would have discarded, because the hint
//! sits on the nullable side of a left join under a filter that rejects NULL:
//! an omitted right row becomes a NULL-extended left row, and that row fails
//! the same filter.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Expr, Filter, Join, JoinType, LogicalPlan, Operator, Projection, TableProviderFilterPushDown,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use graphforge_plan::GraphReadSource;

/// Offer equality conjuncts of a filter to the stored-property scans under it.
#[derive(Debug)]
pub struct StoredEqualityHints;

/// A `column = literal` conjunct, normalised to that operand order.
fn equality(expr: &Expr) -> Option<(Column, Expr)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), literal @ Expr::Literal(value, _))
        | (literal @ Expr::Literal(value, _), Expr::Column(column))
            if !value.is_null() =>
        {
            Some((column.clone(), literal.clone()))
        }
        _ => None,
    }
}

fn comparison(column: &Column, literal: &Expr) -> Expr {
    Expr::Column(column.clone()).eq(literal.clone())
}

/// Offer `wanted` to the property scans under `plan`, looking through column
/// projections and left joins only. Returns the rewritten plan and whether any
/// scan took a hint.
fn hint(plan: &LogicalPlan, wanted: &[(Column, Expr)]) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let mapped = wanted
                .iter()
                .filter_map(|(column, literal)| {
                    let index = projection.schema.index_of_column(column).ok()?;
                    let mut source = &projection.expr[index];
                    while let Expr::Alias(alias) = source {
                        source = alias.expr.as_ref();
                    }
                    match source {
                        Expr::Column(inner) => Some((inner.clone(), literal.clone())),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>();
            if mapped.is_empty() {
                return Ok(None);
            }
            hint(&projection.input, &mapped)?
                .map(|input| {
                    Projection::try_new(projection.expr.clone(), Arc::new(input))
                        .map(LogicalPlan::Projection)
                })
                .transpose()
        }
        LogicalPlan::Join(join) if join.join_type == JoinType::Left => {
            let right = wanted
                .iter()
                .filter(|(column, _)| join.right.schema().index_of_column(column).is_ok())
                .cloned()
                .collect::<Vec<_>>();
            if right.is_empty() {
                return Ok(None);
            }
            Ok(hint(&join.right, &right)?.map(|right| {
                LogicalPlan::Join(Join {
                    right: Arc::new(right),
                    ..join.clone()
                })
            }))
        }
        LogicalPlan::TableScan(scan) => {
            let Some(source) = scan.source.downcast_ref::<GraphReadSource>() else {
                return Ok(None);
            };
            let mut filters = scan.filters.clone();
            for (column, literal) in wanted {
                if column.relation.as_ref() != Some(&scan.table_name) {
                    continue;
                }
                let predicate = comparison(column, literal);
                if filters.contains(&predicate) {
                    continue;
                }
                let supported = datafusion::logical_expr::TableSource::supports_filters_pushdown(
                    source,
                    &[&predicate],
                )?;
                if supported
                    .iter()
                    .all(|support| *support == TableProviderFilterPushDown::Unsupported)
                {
                    continue;
                }
                filters.push(predicate);
            }
            if filters.len() == scan.filters.len() {
                return Ok(None);
            }
            let mut replacement = scan.clone();
            replacement.filters = filters;
            Ok(Some(LogicalPlan::TableScan(replacement)))
        }
        _ => Ok(None),
    }
}

impl OptimizerRule for StoredEqualityHints {
    fn name(&self) -> &'static str {
        "graphforge_stored_equality_hints"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let wanted = split_conjunction(&filter.predicate)
            .into_iter()
            .filter_map(equality)
            .collect::<Vec<_>>();
        if wanted.is_empty() {
            return Ok(Transformed::no(plan));
        }
        match hint(&filter.input, &wanted)? {
            Some(input) => Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                filter.predicate.clone(),
                Arc::new(input),
            )?))),
            None => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests;
