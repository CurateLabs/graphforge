//! A LEFT join whose build side has no size estimate is planned as a sort-merge
//! join after the storage rules have run (#1985).
//!
//! [`super::HashJoinMemoryRule`] leaves one hash join with an unsized build
//! side standing: a LEFT join that enriches its preserved rows from a property
//! scan the storage rules prune to the keys those rows produce. The pruning is
//! a property of the hash join, so the rule runs before it and cannot place the
//! join.
//!
//! The build side of such a join is a frontier the plan cannot size: a small
//! one (a few thousand ids) and one that outgrows the pool look the same until
//! the join runs. DataFusion's hash join holds its build side in a reservation
//! that cannot spill, so the join is planned as a sort-merge join, whose sorts
//! spill. This rule runs after the storage rules and replaces the hash join
//! with a sort-merge join over the inputs those rules built. The inputs keep
//! the frontier tap and the pruned scan, so the scan still reads only the
//! frontier's keys when the frontier is small, and the join spills when it is
//! not.
//!
//! The sort-merge join needs both inputs hash-partitioned on its keys and
//! sorted by them. The exchanges and sorts are added here, as DataFusion's
//! `EnforceDistribution` and `EnforceSorting` ran before the storage rules and
//! do not see a join this rule creates. Each sort reads through a
//! [`SortRunCoalesceExec`], which the earlier `SortRunCoalesceRule` would have
//! added to it.

use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{Distribution, LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::prelude::JoinType;

use super::{estimated_bytes, sort_merge_over};
use crate::sort_runs::SortRunCoalesceExec;

/// Plans every LEFT hash join with an unsized build side as a sort-merge join.
#[derive(Debug)]
pub(crate) struct UnsizedBuildJoinRule {
    /// Bytes each sort run holds, as for [`crate::sort_runs::SortRunCoalesceRule`].
    run_bytes: usize,
}

impl UnsizedBuildJoinRule {
    pub(crate) fn new(run_bytes: usize) -> Self {
        Self { run_bytes }
    }

    fn sort_merge(&self, join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = match join.partition_mode() {
            PartitionMode::Partitioned => join.left().output_partitioning().partition_count(),
            PartitionMode::CollectLeft => join.right().output_partitioning().partition_count(),
            PartitionMode::Auto => {
                return Err(DataFusionError::Internal(
                    "an Auto-mode hash join survived the physical optimizer".into(),
                ));
            }
        };
        let (left_keys, right_keys): (Vec<_>, Vec<_>) = join
            .on()
            .iter()
            .map(|(left, right)| (Arc::clone(left), Arc::clone(right)))
            .unzip();
        // A property nomination installed by the storage rules waits for the
        // entire build frontier. Always drain this input before a sort-merge
        // join polls its probe, even when the frontier is already ordered:
        // reusing that order could leave its key tap waiting on the probe.
        let left = self.sorted(
            hashed(Arc::clone(join.left()), &left_keys, partitions)?,
            &left_keys,
            true,
        )?;
        let right = self.sorted(
            hashed(Arc::clone(join.right()), &right_keys, partitions)?,
            &right_keys,
            false,
        )?;
        sort_merge_over(join, left, right)
    }

    /// `input` sorted by `keys` within each partition. `drain` forces
    /// materialization even when its published ordering already satisfies the
    /// keys, so build-key nominations complete before the probe is polled.
    fn sorted(
        &self,
        input: Arc<dyn ExecutionPlan>,
        keys: &[Arc<dyn PhysicalExpr>],
        drain: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exprs = keys
            .iter()
            .map(|key| PhysicalSortExpr::new(Arc::clone(key), SortOptions::default()))
            .collect::<Vec<_>>();
        if !drain
            && input
                .properties()
                .equivalence_properties()
                .ordering_satisfy(exprs.iter().cloned())?
        {
            return Ok(input);
        }
        let Some(ordering) = LexOrdering::new(exprs) else {
            return Ok(input);
        };
        let per_partition = input.output_partitioning().partition_count() > 1;
        let coalesced = Arc::new(SortRunCoalesceExec::with_force_sort(
            input,
            self.run_bytes,
            drain,
        ));
        Ok(Arc::new(
            SortExec::new(ordering, coalesced).with_preserve_partitioning(per_partition),
        ))
    }
}

/// `input` hash-partitioned on `keys` into `partitions` partitions. An input
/// already partitioned that way is returned as is. A round-robin exchange the
/// planner placed directly below is replaced rather than stacked under the hash
/// exchange.
fn hashed(
    input: Arc<dyn ExecutionPlan>,
    keys: &[Arc<dyn PhysicalExpr>],
    partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let properties = input.properties();
    let required = Distribution::HashPartitioned(keys.to_vec());
    let partitioning = properties.output_partitioning();
    if partitioning.partition_count() == partitions
        && partitioning
            .satisfaction(&required, properties.equivalence_properties(), false)
            .is_satisfied()
    {
        return Ok(input);
    }
    let input = match input.downcast_ref::<RepartitionExec>() {
        Some(exchange) if matches!(exchange.partitioning(), Partitioning::RoundRobinBatch(_)) => {
            Arc::clone(exchange.input())
        }
        _ => input,
    };
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(keys.to_vec(), partitions),
    )?))
}

impl PhysicalOptimizerRule for UnsizedBuildJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if *join.join_type() != JoinType::Left || estimated_bytes(join.left()).is_some() {
                return Ok(Transformed::no(node));
            }
            self.sort_merge(join).map(Transformed::yes)
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "graphforge_unsized_build_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
