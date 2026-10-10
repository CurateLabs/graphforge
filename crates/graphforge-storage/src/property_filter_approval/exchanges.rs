//! Remove unordered exchanges made redundant by an approved CollectLeft join.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::DataFusionError;
use datafusion::functions_nested::array_has::ArrayHas;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::AcrossPartitions;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{Partitioning, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

#[cfg(test)]
#[path = "exchanges_tests.rs"]
mod tests;

fn unordered_bounded(plan: &dyn ExecutionPlan) -> bool {
    plan.fetch().is_none()
        && plan.output_ordering().is_none()
        && !plan.boundedness().is_unbounded()
        && plan.output_partitioning().partition_count() > 0
        && !plan
            .equivalence_properties()
            .constants()
            .iter()
            .any(|constant| constant.across_partitions == AcrossPartitions::Heterogeneous)
}

// Short-circuit evaluation is batch-dependent. Only total expressions may
// cross an exchange: fallible arithmetic/casts and unknown UDFs retain it.
fn row_wise(expr: &dyn PhysicalExpr, schema: &Schema) -> bool {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return schema
            .fields()
            .get(column.index())
            .is_some_and(|field| field.name() == column.name());
    }
    if expr.downcast_ref::<Literal>().is_some() {
        return true;
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        let left_type = binary.left().data_type(schema).ok();
        let right_type = binary.right().data_type(schema).ok();
        let types_safe = left_type == right_type
            && match binary.op() {
                Operator::And | Operator::Or => left_type == Some(DataType::Boolean),
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => matches!(
                    left_type,
                    Some(
                        DataType::Boolean
                            | DataType::Int64
                            | DataType::UInt32
                            | DataType::Utf8
                            | DataType::FixedSizeBinary(16)
                    )
                ),
                _ => false,
            };
        return types_safe
            && binary
                .children()
                .iter()
                .all(|child| row_wise(child.as_ref(), schema));
    }
    let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() else {
        return false;
    };
    // The native type-membership predicate over UInt32 IDs is total. Match
    // its implementation and argument types, never an arbitrary UDF name.
    if function.fun().inner().downcast_ref::<ArrayHas>().is_none() {
        return false;
    }
    let [haystack, needle] = function.args() else {
        return false;
    };
    haystack.downcast_ref::<Column>().is_some()
        && needle.downcast_ref::<Literal>().is_some()
        && matches!(haystack.data_type(schema).ok(), Some(DataType::List(field)) if field.data_type() == &DataType::UInt32)
        && needle.data_type(schema).ok() == Some(DataType::UInt32)
        && row_wise(haystack.as_ref(), schema)
}

pub(super) fn elide(
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if !unordered_bounded(input.as_ref()) {
        return Ok(input);
    }
    if let Some(exchange) = input.downcast_ref::<RepartitionExec>() {
        let keys_safe = match exchange.partitioning() {
            Partitioning::RoundRobinBatch(count) => *count > 0,
            Partitioning::Hash(keys, count) => {
                *count > 0
                    && !keys.is_empty()
                    && keys.iter().all(|key| {
                        key.downcast_ref::<Column>().is_some_and(|column| {
                            exchange
                                .input()
                                .schema()
                                .fields()
                                .get(column.index())
                                .is_some_and(|field| field.name() == column.name())
                        })
                    })
            }
            Partitioning::UnknownPartitioning(_) => false,
        };
        // Repartition can erase heterogeneous input constants from its own
        // properties. Check the retained child before dropping the exchange.
        if keys_safe
            && !exchange.preserve_order()
            && unordered_bounded(exchange.input().as_ref())
            && input.schema().as_ref() == exchange.input().schema().as_ref()
        {
            return elide(Arc::clone(exchange.input()));
        }
        return Ok(input);
    }
    let child = if let Some(filter) = input.downcast_ref::<FilterExec>() {
        if !row_wise(
            filter.predicate().as_ref(),
            filter.input().schema().as_ref(),
        ) {
            return Ok(input);
        }
        Arc::clone(filter.input())
    } else if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        if !projection
            .expr()
            .iter()
            .all(|item| row_wise(item.expr.as_ref(), projection.input().schema().as_ref()))
        {
            return Ok(input);
        }
        Arc::clone(projection.input())
    } else {
        // Preserve every other operator, including joins, taps and expands,
        // together with all distribution requirements inside its subtree.
        return Ok(input);
    };
    let replacement = elide(Arc::clone(&child))?;
    if Arc::ptr_eq(&child, &replacement) {
        return Ok(input);
    }
    let rebuilt = Arc::clone(&input).with_new_children(vec![replacement])?;
    if rebuilt.schema().as_ref() != input.schema().as_ref() {
        return Ok(input);
    }
    Ok(rebuilt)
}
