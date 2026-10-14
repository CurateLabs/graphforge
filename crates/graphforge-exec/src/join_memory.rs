//! A hash join is kept only when a side is estimated to fit the query memory
//! pool, and otherwise becomes a sort-merge join (#1985).
//!
//! DataFusion's hash join holds its whole build side in memory and cannot
//! spill: past the pool it fails with `Resources exhausted ... HashJoinInput
//! ... can spill: false`. Its sort-merge join sorts both inputs with spillable
//! sorts and buffers only one key group, so it completes in any pool that
//! holds its sorts, at the price of sorting. The hash join is the faster plan,
//! so it is kept only when the statistics its inputs publish say the build side
//! fits.
//!
//! The rule runs directly after DataFusion's `JoinSelection`, which has by
//! then put the smaller side on the left whenever both sizes are known. Each
//! input is judged against [`BUILD_POOL_SHARE_DENOMINATOR`]th of the pool
//! (see [`Placement`]): the build side when it fits, else the probe side
//! becomes the build side when that fits, else the join is planned as a
//! sort-merge join. An input that publishes neither a size nor a row count
//! does not fit: with no estimate nothing bounds what the join builds at run
//! time, and only a join that can spill is safe. A sort-merge join over a pool
//! too small for its sorts still fails with the typed resource error naming
//! the consumers. The sorts and repartitions the sort-merge join requires are
//! added by DataFusion's later `EnforceDistribution` and `EnforceSorting`.
//!
//! One join with an unknown build side stays a hash join for the storage
//! rules: a LEFT join that enriches its preserved rows from a property scan
//! those rules prune to the keys that side produces
//! ([`graphforge_storage::PropertyFilterApprovalRule::prunes_probe_scan_to_build_keys`]). The pruning
//! exists only on a hash join, and it is what keeps such a query from reading
//! the whole property route; a swapped or sort-merge join reads the route in
//! full. The [`UnsizedBuildJoinRule`], which runs after the storage rules, then
//! plans every LEFT join whose build side is still unsized as a sort-merge
//! join, keeping the frontier tap and the pruned scan the storage rules built.
//! The frontier is what the plan cannot size, so only a join that can spill is
//! safe for it.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;

pub(crate) use unsized_build::UnsizedBuildJoinRule;

/// A join build side may be estimated at most `1 / BUILD_POOL_SHARE_DENOMINATOR`
/// of the pool before the join is planned as a sort-merge join. The rest of
/// the pool serves the probe side, the exchanges, and every other operator of
/// the query.
const BUILD_POOL_SHARE_DENOMINATOR: usize = 2;
/// Bytes assumed for each value of a variable-width column when a build side
/// publishes only a row count.
const VARIABLE_WIDTH_VALUE_BYTES: usize = 64;

/// Plans a join whose build side cannot fit the pool as a sort-merge join.
#[derive(Debug)]
pub(crate) struct HashJoinMemoryRule {
    memory_budget_bytes: usize,
}

impl HashJoinMemoryRule {
    pub(crate) fn new(memory_budget_bytes: usize) -> Self {
        Self {
            memory_budget_bytes,
        }
    }

    fn build_limit(&self) -> usize {
        self.memory_budget_bytes / BUILD_POOL_SHARE_DENOMINATOR
    }

    /// How `join` is planned, from the sizes its two inputs are estimated at.
    fn place(&self, join: &HashJoinExec) -> Placement {
        let limit = self.build_limit();
        let fits = |side: &Arc<dyn ExecutionPlan>| {
            estimated_bytes(side).is_some_and(|bytes| bytes <= limit)
        };
        if fits(join.left())
            || (estimated_bytes(join.left()).is_none()
                && graphforge_storage::PropertyFilterApprovalRule::prunes_probe_scan_to_build_keys(
                    join,
                    crate::traversal_destination::is_frontier_endpoint_uuid,
                ))
        {
            Placement::Keep
        } else if fits(join.right()) {
            Placement::Swap
        } else {
            Placement::SortMerge
        }
    }
}

/// How a hash join is planned once its build side is judged against the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// The build side is estimated to fit: the hash join stands.
    Keep,
    /// Only the probe side is estimated to fit: it builds and the other side
    /// streams.
    Swap,
    /// Neither side is estimated to fit, or the estimates are unknown: a join
    /// that can spill.
    SortMerge,
}

/// Bytes a plan's output is estimated to occupy, from the statistics it
/// publishes. `None` when it publishes neither a size nor a row count.
fn estimated_bytes(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let statistics = plan.partition_statistics(None).ok()?;
    if let Some(bytes) = statistics.total_byte_size.get_value() {
        return Some(*bytes);
    }
    let rows = *statistics.num_rows.get_value()?;
    Some(rows.saturating_mul(row_width(&plan.schema())))
}

/// Bytes a row of `schema` is assumed to occupy: the width of each fixed-width
/// column, and [`VARIABLE_WIDTH_VALUE_BYTES`] for every other.
pub(crate) fn row_width(schema: &SchemaRef) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| {
            field
                .data_type()
                .primitive_width()
                .unwrap_or(VARIABLE_WIDTH_VALUE_BYTES)
        })
        .sum()
}

/// A sort-merge join of `left` and `right` on `join`'s keys, with the columns
/// `join` emits. `left` and `right` are `join`'s inputs, or those inputs with
/// the exchanges and sorts the sort-merge join requires.
///
/// The sort-merge join emits every column of both sides; the hash join's
/// embedded projection, if any, is applied above it.
fn sort_merge_over(
    join: &HashJoinExec,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let sort_options = vec![SortOptions::default(); join.on().len()];
    let sort_merge = SortMergeJoinExec::try_new(
        left,
        right,
        join.on().to_vec(),
        join.filter().cloned(),
        *join.join_type(),
        sort_options,
        join.null_equality(),
    )?;
    let Some(kept) = join.projection.as_deref() else {
        return Ok(Arc::new(sort_merge));
    };
    let schema = sort_merge.schema();
    let exprs = kept
        .iter()
        .map(|&index| {
            let name = schema.field(index).name().clone();
            (
                Arc::new(Column::new(&name, index)) as Arc<dyn PhysicalExpr>,
                name,
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(
        exprs,
        Arc::new(sort_merge),
    )?))
}

/// DataFusion's default physical rules with [`HashJoinMemoryRule`] directly
/// after `JoinSelection`.
///
/// # Errors
/// Fails if DataFusion no longer has a rule named `join_selection`, so a
/// renamed rule cannot silently drop the guard.
pub(crate) fn physical_optimizer_rules(
    memory_budget_bytes: usize,
) -> Result<Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>> {
    let mut rules = PhysicalOptimizer::new().rules;
    let Some(position) = rules
        .iter()
        .position(|rule| rule.name() == "join_selection")
    else {
        return Err(datafusion::error::DataFusionError::Internal(
            "DataFusion's physical optimizer has no join_selection rule".into(),
        ));
    };
    rules.insert(
        position + 1,
        Arc::new(HashJoinMemoryRule::new(memory_budget_bytes)),
    );
    Ok(rules)
}

impl PhysicalOptimizerRule for HashJoinMemoryRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            match self.place(join) {
                Placement::Keep => return Ok(Transformed::no(node)),
                Placement::Swap => {
                    return join
                        .swap_inputs(*join.partition_mode())
                        .map(Transformed::yes);
                }
                Placement::SortMerge => {}
            }
            sort_merge_over(join, Arc::clone(join.left()), Arc::clone(join.right()))
                .map(Transformed::yes)
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "graphforge_hash_join_memory"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

mod unsized_build;

#[cfg(test)]
mod tests;
