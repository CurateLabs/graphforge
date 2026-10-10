//! Share the strict property-equality build of an anchored INNER join.
//!
//! Logical null rejection admits the INNER join. The final physical rule runs
//! after distribution enforcement, so it removes redundant input exchanges
//! and restores the original output hash distribution after the public swap.
use crate::property_scan::PropertyOverlayExec;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, JoinType};
use datafusion::physical_expr::equivalence::AcrossPartitions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

pub(super) fn shared_equality_build(
    join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let plan: &dyn ExecutionPlan = join;
    if *join.join_type() != JoinType::Inner
        || *join.partition_mode() != PartitionMode::Partitioned
        || !join.contains_projection()
        || join.fetch().is_some()
        || join.filter().is_some()
        || plan.output_ordering().is_some()
        || join.left().boundedness().is_unbounded()
        || join.right().boundedness().is_unbounded()
        || plan
            .equivalence_properties()
            .constants()
            .iter()
            .any(|constant| constant.across_partitions == AcrossPartitions::Heterogeneous)
    {
        return Ok(None);
    }
    let output_partitioning = plan.output_partitioning().clone();
    if !matches!(&output_partitioning, Partitioning::Hash(_, count) if *count > 0) {
        return Ok(None);
    }
    let Partitioning::Hash(output_keys, _) = &output_partitioning else {
        unreachable!("hash partitioning was checked above")
    };
    // HashJoinExec projects its partitioning through the join projection. If
    // that projection drops the UUID key, DataFusion represents it as a
    // UnKnownColumn expression (whose data type is Null). RepartitionExec
    // would then fail when it tries to evaluate that key. Keep the original
    // join in this case; its distribution contract cannot be restored from
    // the projected output.
    if output_keys.iter().any(|key| {
        matches!(
            key.data_type(plan.schema().as_ref()),
            Ok(DataType::Null) | Err(_)
        )
    }) {
        return Ok(None);
    }
    let Some(exchange) = join.right().downcast_ref::<RepartitionExec>() else {
        return Ok(None);
    };
    let exchange_plan: &dyn ExecutionPlan = exchange;
    if exchange.fetch().is_some()
        || exchange.preserve_order()
        || exchange_plan.output_ordering().is_some()
    {
        return Ok(None);
    }
    let Some(scan) = equality_build_scan(exchange.input()) else {
        return Ok(None);
    };
    let Some(uuid_index) = scan.equality_build_uuid_column() else {
        return Ok(None);
    };
    let scan_schema = scan.schema();
    let Some(field) = scan_schema.fields().get(uuid_index) else {
        return Ok(None);
    };
    let Some(property) = join.on().iter().find_map(|(left, right)| {
        let left = left.downcast_ref::<Column>()?;
        let right = right.downcast_ref::<Column>()?;
        (right.index() == uuid_index
            && right.name() == field.name()
            && right.data_type(join.right().schema().as_ref()).ok()
                == Some(DataType::FixedSizeBinary(16))
            && left.data_type(join.left().schema().as_ref()).ok()
                == Some(DataType::FixedSizeBinary(16))
            && join
                .left()
                .schema()
                .fields()
                .get(left.index())
                .is_some_and(|field| !field.is_nullable() && field.name() == left.name()))
        .then_some(right)
    }) else {
        return Ok(None);
    };
    // Only remove the ordinary hash exchange for this exact single UUID key.
    let Partitioning::Hash(keys, _) = exchange.partitioning() else {
        return Ok(None);
    };
    if keys.len() != 1 || !keys[0].dyn_eq(property) || join.on().len() != 1 {
        return Ok(None);
    }
    let swapped = join.swap_inputs(PartitionMode::CollectLeft)?;
    let Some(swapped_join) = swapped.downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    if swapped_join.schema().as_ref() != join.schema().as_ref() {
        return Ok(None);
    }
    let shared: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(
        super::exchanges::elide(Arc::clone(exchange.input()))?,
    ));
    let probe = super::exchanges::elide(Arc::clone(join.left()))?;
    let rebuilt = swapped_join
        .builder()
        .with_new_children(vec![shared, probe])?
        .reset_state()
        .recompute_properties()
        .build_exec()?;
    if rebuilt.schema().as_ref() != join.schema().as_ref() {
        return Ok(None);
    }
    Ok(Some(Arc::new(RepartitionExec::try_new(
        rebuilt,
        output_partitioning,
    )?)))
}

/// Inspect only the schema-preserving residual equality pipeline. Its filter
/// remains authoritative when its redundant round-robin exchange is removed.
fn equality_build_scan(input: &Arc<dyn ExecutionPlan>) -> Option<&PropertyOverlayExec> {
    if let Some(scan) = input.downcast_ref::<PropertyOverlayExec>() {
        return Some(scan);
    }
    let filter = input.downcast_ref::<FilterExec>()?;
    if filter.projection().is_some()
        || input.fetch().is_some()
        || input.output_ordering().is_some()
        || input.schema().as_ref() != filter.input().schema().as_ref()
        || input
            .equivalence_properties()
            .constants()
            .iter()
            .any(|constant| constant.across_partitions == AcrossPartitions::Heterogeneous)
    {
        return None;
    }
    let scan_input = if let Some(exchange) = filter.input().downcast_ref::<RepartitionExec>() {
        let plan: &dyn ExecutionPlan = exchange;
        if !matches!(exchange.partitioning(), Partitioning::RoundRobinBatch(count) if *count > 0)
            || exchange.preserve_order()
            || exchange.fetch().is_some()
            || plan.output_ordering().is_some()
            || plan.schema().as_ref() != exchange.input().schema().as_ref()
        {
            return None;
        }
        exchange.input()
    } else {
        filter.input()
    };
    let scan = scan_input.downcast_ref::<PropertyOverlayExec>()?;
    scan.equality_filter_matches(filter.predicate().as_ref())
        .then_some(scan)
}
