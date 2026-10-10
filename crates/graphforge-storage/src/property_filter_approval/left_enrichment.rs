//! Prune a partitioned LEFT enrichment from its completed preserved frontier.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, JoinType};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::equivalence::AcrossPartitions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::property_join_nomination::{UuidBuildKeyNomination, UuidBuildKeyTapExec};
use crate::property_scan::PropertyOverlayExec;

fn unordered_bounded(plan: &dyn ExecutionPlan) -> bool {
    plan.fetch().is_none()
        && plan.output_ordering().is_none()
        && !plan.boundedness().is_unbounded()
        && !plan
            .equivalence_properties()
            .constants()
            .iter()
            .any(|constant| constant.across_partitions == AcrossPartitions::Heterogeneous)
}

pub(super) fn nominate_partitioned_left(
    join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let original: &dyn ExecutionPlan = join;
    if *join.join_type() != JoinType::Left
        || *join.partition_mode() != PartitionMode::Partitioned
        || !join.contains_projection()
        || join.on().len() != 1
        || !unordered_bounded(original)
        || !unordered_bounded(join.left().as_ref())
        || join.left().downcast_ref::<UuidBuildKeyTapExec>().is_some()
    {
        return Ok(None);
    }
    let output = original.output_partitioning().clone();
    if !matches!(&output, Partitioning::Hash(_, count) if *count > 0) {
        return Ok(None);
    }
    if super::has_unknown_hash_key(&output, original) {
        return Ok(None);
    }
    let Some((scan, build_column)) = matching_scan(join) else {
        return Ok(None);
    };

    // Keep the preserved side's operators. NULL keys are omitted
    // from the nomination, but their unmatched LEFT rows remain in the join.
    let nomination = UuidBuildKeyNomination::new();
    let frontier: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(
        super::exchanges::elide(Arc::clone(join.left()))?,
    ));
    let tapped: Arc<dyn ExecutionPlan> = Arc::new(UuidBuildKeyTapExec::new(
        frontier,
        build_column,
        Arc::clone(&nomination),
    ));
    let nominated: Arc<dyn ExecutionPlan> = Arc::new(scan.with_uuid_nomination(nomination));
    let rebuilt = join
        .builder()
        .with_new_children(vec![tapped, nominated])?
        .with_partition_mode(PartitionMode::CollectLeft)
        .reset_state()
        .recompute_properties()
        .build_exec()?;
    if rebuilt.schema().as_ref() != original.schema().as_ref() {
        return Ok(None);
    }
    Ok(Some(Arc::new(RepartitionExec::try_new(rebuilt, output)?)))
}

/// Bind only the exact direct scan behind its ordinary UUID hash exchange.
fn matching_scan(join: &HashJoinExec) -> Option<(&PropertyOverlayExec, usize)> {
    let exchange = join.right().downcast_ref::<RepartitionExec>()?;
    if exchange.preserve_order() || !unordered_bounded(exchange) {
        return None;
    }
    let scan = exchange.input().downcast_ref::<PropertyOverlayExec>()?;
    let uuid_index = scan.fresh_nomination_uuid_column()?;
    if !unordered_bounded(scan) || exchange.schema().as_ref() != scan.schema().as_ref() {
        return None;
    }
    let (build, probe) = &join.on()[0];
    let (Some(build_column), Some(probe_column)) = (
        build.downcast_ref::<Column>(),
        probe.downcast_ref::<Column>(),
    ) else {
        return None;
    };
    let build_schema = join.left().schema();
    let scan_schema = scan.schema();
    if !build_schema
        .fields()
        .get(build_column.index())
        .is_some_and(|field| {
            field.name() == build_column.name()
                && field.data_type() == &DataType::FixedSizeBinary(16)
        })
        || !scan_schema
            .fields()
            .get(probe_column.index())
            .is_some_and(|field| {
                probe_column.index() == uuid_index
                    && field.name() == probe_column.name()
                    && field.data_type() == &DataType::FixedSizeBinary(16)
            })
    {
        return None;
    }
    let Partitioning::Hash(keys, count) = exchange.partitioning() else {
        return None;
    };
    if *count == 0 || keys.len() != 1 || !keys[0].dyn_eq(probe.as_ref()) {
        return None;
    }

    Some((scan, build_column.index()))
}
