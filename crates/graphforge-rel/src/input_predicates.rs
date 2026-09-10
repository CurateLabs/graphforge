//! Conservative predicate movement across required fixed graph expansion.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, tree_node::Transformed};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{Expr, ExprSchemable, Filter, LogicalPlan, Operator};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use graphforge_plan::ExpandNode;

/// Move total, deterministic input predicates before required fixed expansion.
///
/// DataFusion's extension hook uses unqualified column names. This rule instead
/// requires qualified references to unchanged input fields and never moves an
/// expression that can introduce row-dependent errors or volatile evaluation.
#[derive(Debug)]
pub struct FixedExpandInputPredicates;

fn atom_type(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    match expr {
        Expr::Column(column) if column.relation.is_some() => expr.get_type(schema).ok(),
        Expr::Literal(value, _) if !value.is_null() => Some(value.data_type()),
        _ => None,
    }
}

fn primitive(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::FixedSizeBinary(_)
    )
}

fn total_predicate(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            let left = atom_type(&binary.left, schema);
            left.as_ref().is_some_and(primitive) && left == atom_type(&binary.right, schema)
        }
        Expr::IsNull(value) | Expr::IsNotNull(value) => atom_type(value, schema).is_some(),
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            total_predicate(&binary.left, schema) && total_predicate(&binary.right, schema)
        }
        _ => crate::expr::is_fixed_relationship_disjoint(expr, schema),
    }
}

impl OptimizerRule for FixedExpandInputPredicates {
    fn name(&self) -> &str {
        "graphforge_fixed_expand_input_predicates"
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
        let LogicalPlan::Extension(extension) = filter.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(expand) = extension.node.as_any().downcast_ref::<ExpandNode>() else {
            return Ok(Transformed::no(plan));
        };
        // Do not alter which rows reach a potentially failing or volatile
        // residual expression in the same filter.
        if !total_predicate(&filter.predicate, filter.input.schema()) {
            return Ok(Transformed::no(plan));
        }
        let (push, keep): (Vec<_>, Vec<_>) = split_conjunction(&filter.predicate)
            .into_iter()
            .cloned()
            .partition(|expr| {
                let columns = expr.column_refs();
                !crate::expr::is_fixed_relationship_disjoint(expr, filter.input.schema())
                    && !columns.is_empty()
                    && columns.iter().all(|column| {
                        column.relation.is_some() && expand.input.schema().has_column(column)
                    })
            });
        let Some(predicate) = conjunction(push) else {
            return Ok(Transformed::no(plan));
        };
        let mut replacement = expand.clone();
        replacement.input = Arc::new(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::clone(&expand.input),
        )?));
        let replacement = LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(replacement),
        });
        let replacement = match conjunction(keep) {
            Some(predicate) => {
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(replacement))?)
            }
            None => replacement,
        };
        Ok(Transformed::yes(replacement))
    }
}
