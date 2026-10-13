//! Prune a partitioned LEFT enrichment from its completed preserved frontier.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, JoinType};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::equivalence::AcrossPartitions;
use datafusion::physical_expr::expressions::{Column, UnKnownColumn};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::property_join_nomination::{
    NominationLimits, UuidBuildKeyNomination, UuidBuildKeyTapExec,
};
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
    nominate_partitioned_left_selected(join, None)
}

/// A frontier naming more than `1 / NOMINATION_ROUTE_DIVISOR` of its route's
/// rows is not nominated: past about half the route, one sequential scan
/// reads less than the point reads the nominated set would drive, so the
/// nomination abandons itself and the scan runs unpruned.
const NOMINATION_ROUTE_DIVISOR: usize = 2;

/// Prune a partitioned LEFT enrichment's direct property scan to the UUIDs
/// its preserved side produces, without changing the join.
///
/// The join stays partitioned. Its preserved side is tapped on the key column
/// as it streams (one 16-byte key per distinct row, charged to the query
/// pool) and re-hashed into the same partitions, so no build row is
/// concatenated or held beyond what the partitioned join already holds; the
/// scan waits for the tap to see the whole frontier and reads only its keys.
/// Nomination only prunes the scan, the join's probe side, and a LEFT join
/// keeps every build row, so the result cannot change. `selected_key` is the
/// caller's proof that a key-only scan may be pruned from that build column;
/// without it only a scan that projects property payload qualifies.
///
/// The nomination bounds itself: it abandons, and the scan then reads the
/// whole route as the un-nominated join would, once the frontier names more
/// than half the route's planned rows or its key set would outgrow an eighth
/// of the query pool. The plan keeps the same shape either way.
pub(super) fn nominate_partitioned_left_selected(
    join: &HashJoinExec,
    selected_key: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let original: &dyn ExecutionPlan = join;
    if *join.join_type() != JoinType::Left
        || *join.partition_mode() != PartitionMode::Partitioned
        || !join.contains_projection()
        || join.on().len() != 1
        || !unordered_bounded(original)
        || !unordered_bounded(join.left().as_ref())
    {
        return Ok(None);
    }
    let Partitioning::Hash(_, partitions) = original.output_partitioning() else {
        return Ok(None);
    };
    let partitions = *partitions;
    if partitions == 0
        || join.left().output_partitioning().partition_count() != partitions
        || join.right().output_partitioning().partition_count() != partitions
    {
        return Ok(None);
    }
    let Some((scan, build_column)) = matching_scan(join, selected_key.is_some()) else {
        return Ok(None);
    };
    if selected_key.is_some_and(|selected| selected != build_column) {
        return Ok(None);
    }
    // Keep the preserved side's operators. NULL keys are omitted from the
    // nomination, but their unmatched LEFT rows remain in the join.
    let nomination = UuidBuildKeyNomination::with_limits(NominationLimits {
        max_ids: scan
            .planned_rows()
            .map(|rows| rows / NOMINATION_ROUTE_DIVISOR),
    });
    let frontier: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(
        super::exchanges::elide(Arc::clone(join.left()))?,
    ));
    let tapped: Arc<dyn ExecutionPlan> = Arc::new(UuidBuildKeyTapExec::new(
        frontier,
        build_column,
        Arc::clone(&nomination),
    ));
    let build_key = Arc::clone(&join.on()[0].0);
    let build: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        tapped,
        Partitioning::Hash(vec![build_key], partitions),
    )?);
    let probe = Arc::clone(join.right())
        .with_new_children(vec![Arc::new(scan.with_uuid_nomination(nomination))])?;
    let rebuilt = join
        .builder()
        .with_new_children(vec![build, probe])?
        .reset_state()
        .recompute_properties()
        .build_exec()?;
    if rebuilt.schema().as_ref() != original.schema().as_ref()
        || !same_distribution(rebuilt.output_partitioning(), original.output_partitioning())
    {
        return Ok(None);
    }
    Ok(Some(rebuilt))
}

/// Whether two outputs hash their rows into the same partitions. A join whose
/// projection drops its hash key reports that key as an `UnKnownColumn`, which
/// by design equals nothing, so two such keys match by name.
fn same_distribution(rebuilt: &Partitioning, original: &Partitioning) -> bool {
    let (Partitioning::Hash(rebuilt, count), Partitioning::Hash(original, expected)) =
        (rebuilt, original)
    else {
        return false;
    };
    count == expected
        && rebuilt.len() == original.len()
        && rebuilt.iter().zip(original).all(|(left, right)| {
            left.dyn_eq(right.as_ref())
                || left
                    .downcast_ref::<UnKnownColumn>()
                    .zip(right.downcast_ref::<UnKnownColumn>())
                    .is_some_and(|(left, right)| left.name() == right.name())
        })
}

/// Bind only the exact direct scan behind its ordinary UUID hash exchange.
fn matching_scan(
    join: &HashJoinExec,
    selected_endpoint: bool,
) -> Option<(&PropertyOverlayExec, usize)> {
    let exchange = join.right().downcast_ref::<RepartitionExec>()?;
    if exchange.preserve_order() || !unordered_bounded(exchange) {
        return None;
    }
    let scan = exchange.input().downcast_ref::<PropertyOverlayExec>()?;
    let uuid_index = if selected_endpoint {
        scan.fresh_selected_endpoint_uuid_column()?
    } else {
        scan.fresh_nomination_uuid_column()?
    };
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
