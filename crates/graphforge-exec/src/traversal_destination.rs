//! Which output columns of a physical plan carry the UUID of a node that a
//! traversal reached.
//!
//! A property scan joined to such a column by key is a node variable's
//! enrichment. The traversal's completed output is the frontier that
//! enrichment reads, so the storage layer may prune a key-only scan to it
//! without changing the join. A topology scan has no such frontier: a
//! whole-graph `count(*)` must still authenticate the whole property route it
//! joins, so a scan's own UUID column never qualifies. A traversal whose rows
//! no anchor or fetch bounds keeps its partitioned join.

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::expand_exec::{ExpandExec, VarLenExpandExec, is_selected_endpoint_uuid};

/// Whether `column_index` of `plan` is a UUID the property scan joined to it
/// may be pruned to: the destination of an expansion whose input is bounded,
/// or the exact endpoint an equality-selected seed reaches.
///
/// The bound matters because the pruned join collects its build side into one
/// Arrow batch; see [`graphforge_storage::frontier_has_bound`].
pub(crate) fn is_frontier_endpoint_uuid(plan: &dyn ExecutionPlan, column_index: usize) -> bool {
    is_selected_endpoint_uuid(plan, column_index)
        || (is_traversal_destination_uuid(plan, column_index)
            && graphforge_storage::frontier_has_bound(plan))
}

/// The position of `node_uuid` within the destination node's topology columns.
fn destination_uuid_offset() -> Option<usize> {
    graphforge_storage::TOPOLOGY_NODES_SCHEMA
        .index_of("node_uuid")
        .ok()
}

/// Follow `column_index` down through row-preserving operators to the
/// destination UUID of the expansion that produced it.
///
/// Projections that pass a column through, filters and exchanges, and sorts
/// only choose, order, and move rows, so the column keeps its meaning.
fn is_traversal_destination_uuid(plan: &dyn ExecutionPlan, column_index: usize) -> bool {
    let schema = plan.schema();
    if schema
        .fields()
        .get(column_index)
        .is_none_or(|field| field.name() != "node_uuid")
    {
        return false;
    }
    let Some(offset) = destination_uuid_offset() else {
        return false;
    };
    let width = graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();

    // An expansion appends the destination node's topology columns last.
    if plan.downcast_ref::<ExpandExec>().is_some() {
        return schema
            .fields()
            .len()
            .checked_sub(width)
            .is_some_and(|start| start + offset == column_index);
    }
    // A variable-length expansion appends them directly after its input,
    // followed by the relationship list.
    if plan.downcast_ref::<VarLenExpandExec>().is_some() {
        return plan
            .children()
            .first()
            .is_some_and(|input| input.schema().fields().len() + offset == column_index);
    }

    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let Some(expression) = projection.expr().get(column_index) else {
            return false;
        };
        let Some(column) = expression.expr.downcast_ref::<Column>() else {
            return false;
        };
        let input = projection.input();
        return input
            .schema()
            .fields()
            .get(column.index())
            .is_some_and(|field| field.name() == column.name())
            && is_traversal_destination_uuid(input.as_ref(), column.index());
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        // A filter drops rows. When it also narrows its columns, follow the
        // requested column to the input column it passes through.
        let input_index = match filter.projection() {
            None => column_index,
            Some(projection) => match projection.get(column_index) {
                Some(&input_index) => input_index,
                None => return false,
            },
        };
        return filter
            .input()
            .schema()
            .fields()
            .get(input_index)
            .zip(schema.fields().get(column_index))
            .is_some_and(|(input, output)| input.as_ref() == output.as_ref())
            && is_traversal_destination_uuid(filter.input().as_ref(), input_index);
    }
    // Operators that keep their input's schema and only move or order rows.
    let input = if let Some(exchange) = plan.downcast_ref::<RepartitionExec>() {
        exchange.input()
    } else if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        coalesce.input()
    } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
        sort.input()
    } else if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
        merge.input()
    } else {
        return false;
    };
    input.schema().as_ref() == schema.as_ref()
        && is_traversal_destination_uuid(input.as_ref(), column_index)
}

#[cfg(test)]
mod tests;
