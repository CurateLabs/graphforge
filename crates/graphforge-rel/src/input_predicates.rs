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

/// Preserve DataFusion defaults and place graph input filtering before projection pruning.
#[must_use]
pub fn optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Optimizer::new().rules;
    let position = rules
        .iter()
        .position(|rule| rule.name() == "push_down_filter")
        .expect("pinned DataFusion optimizer includes filter pushdown");
    rules.insert(position + 1, Arc::new(FixedExpandInputPredicates));
    rules
}

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
        Expr::ScalarFunction(function) if crate::expr::is_comparison_predicate(function) => {
            function.args.len() == 3
                && matches!(
                    &function.args[2],
                    Expr::Literal(
                        datafusion::common::ScalarValue::Int8(Some(0..=3))
                            | datafusion::common::ScalarValue::Int64(Some(0..=3)),
                        _
                    )
                )
                && atom_type(&function.args[0], schema)
                    .as_ref()
                    .is_some_and(primitive)
                && atom_type(&function.args[0], schema) == atom_type(&function.args[1], schema)
        }
        _ => crate::expr::is_fixed_relationship_disjoint(expr, schema),
    }
}

impl OptimizerRule for FixedExpandInputPredicates {
    fn name(&self) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::{Column, TableReference};
    use datafusion::logical_expr::{EmptyRelation, Extension, col, lit};
    use datafusion::optimizer::OptimizerContext;
    use graphforge_ir::Direction;

    fn expanded() -> LogicalPlan {
        let schema = DFSchema::new_with_metadata(
            vec![(
                Some(TableReference::bare("var_0")),
                Arc::new(Field::new("score", DataType::Int64, true)),
            )],
            Default::default(),
        )
        .unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        });
        LogicalPlan::Extension(Extension {
            node: Arc::new(ExpandNode::new(
                Arc::new(input),
                "*",
                0,
                1,
                2,
                Direction::Out,
                None,
                vec![Arc::new(Field::new("edge_id", DataType::UInt64, false))],
                vec![],
                vec![Arc::new(Field::new("score", DataType::Int64, true))],
            )),
        })
    }

    fn column(var: &str) -> Expr {
        Expr::Column(Column::new(Some(TableReference::bare(var)), "score"))
    }

    fn rewrite(predicate: Expr) -> Transformed<LogicalPlan> {
        FixedExpandInputPredicates
            .rewrite(
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(expanded())).unwrap()),
                &OptimizerContext::new(),
            )
            .unwrap()
    }

    #[test]
    fn same_named_destination_stays_residual() {
        let result = rewrite(
            column("var_0")
                .eq(lit(7_i64))
                .and(column("var_1").gt(lit(2_i64))),
        );
        assert!(result.transformed);
        let LogicalPlan::Filter(residual) = result.data else {
            panic!("destination predicate must remain");
        };
        assert_eq!(residual.predicate, column("var_1").gt(lit(2_i64)));
        let LogicalPlan::Extension(extension) = residual.input.as_ref() else {
            panic!();
        };
        let expand = extension
            .node
            .as_any()
            .downcast_ref::<ExpandNode>()
            .unwrap();
        let LogicalPlan::Filter(input) = expand.input.as_ref() else {
            panic!("input predicate must precede expansion");
        };
        assert_eq!(input.predicate, column("var_0").eq(lit(7_i64)));
    }

    #[test]
    fn potentially_failing_residual_blocks_entire_filter() {
        let predicate = column("var_0")
            .eq(lit(7_i64))
            .and((column("var_1") / lit(0_i64)).gt(lit(0_i64)));
        assert!(!rewrite(predicate).transformed);
    }

    #[test]
    fn unqualified_or_mixed_disjunction_is_not_moved() {
        assert!(!total_predicate(
            &col("score").eq(lit(7_i64)),
            expanded().schema()
        ));
        assert!(
            !rewrite(
                column("var_0")
                    .eq(lit(7_i64))
                    .or(column("var_1").eq(lit(7_i64)))
            )
            .transformed
        );
        assert!(!rewrite(column("var_1").eq(lit(7_i64))).transformed);
    }

    #[test]
    fn volatile_residual_blocks_entire_filter() {
        let random = datafusion::functions::math::random().call(vec![]);
        assert!(!rewrite(column("var_0").eq(lit(7_i64)).and(random.gt(lit(0.5)))).transformed);
    }

    #[test]
    fn input_column_comparison_preserves_qualified_membership() {
        assert!(rewrite(column("var_0").eq(column("var_0"))).transformed);
        assert!(!rewrite(column("var_0").eq(column("var_1"))).transformed);
    }

    #[test]
    fn nullable_input_checks_can_precede_expansion() {
        assert!(rewrite(column("var_0").is_null()).transformed);
        assert!(rewrite(column("var_0").is_not_null()).transformed);
        assert!(
            rewrite(
                column("var_0")
                    .gt_eq(lit(-7_i64))
                    .and(column("var_0").lt_eq(lit(7_i64)))
            )
            .transformed
        );
    }
}
