//! Hash joins that cannot fit the query memory pool become sort-merge joins
//! (#1985).
//!
//! DataFusion's hash join holds its whole build side in memory and cannot
//! spill: past the pool it fails with `Resources exhausted ... HashJoinInput
//! ... can spill: false`. Its sort-merge join sorts both inputs with spillable
//! sorts and buffers only one key group, so it completes in any pool at the
//! price of sorting. The hash join is the faster plan whenever the build side
//! fits, so it is kept unless the statistics the build side publishes say it
//! cannot.
//!
//! The rule runs directly after DataFusion's `JoinSelection`, which has by
//! then chosen the build side from the same statistics (the smaller side is
//! already on the left whenever both sizes are known). A build side that is
//! still estimated beyond [`BUILD_POOL_SHARE_DENOMINATOR`]th of the pool
//! is a join no choice of side can fit. The sorts and repartitions the
//! sort-merge join requires are added by DataFusion's later `EnforceDistribution`
//! and `EnforceSorting`.
//!
//! A build side with unknown statistics is left as a hash join: nothing says
//! it will not fit, and a join that still exhausts the pool fails with the
//! typed resource error naming its consumers, as before.

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
        let limit = self.build_limit();
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if estimated_bytes(join.left()).is_none_or(|bytes| bytes <= limit) {
                return Ok(Transformed::no(node));
            }
            let sort_options = vec![SortOptions::default(); join.on().len()];
            let sort_merge = SortMergeJoinExec::try_new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                join.on().to_vec(),
                join.filter().cloned(),
                *join.join_type(),
                sort_options,
                join.null_equality(),
            )?;
            // The sort-merge join emits every column of both sides; the hash
            // join's embedded projection, if any, is applied above it.
            let replacement: Arc<dyn ExecutionPlan> = match join.projection.as_deref() {
                None => Arc::new(sort_merge),
                Some(kept) => {
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
                    Arc::new(ProjectionExec::try_new(exprs, Arc::new(sort_merge))?)
                }
            };
            Ok(Transformed::yes(replacement))
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
