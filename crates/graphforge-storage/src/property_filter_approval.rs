//! Final-plan authority for exact property-scan UUID pruning hints.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;

/// Approves only UUID filters still owned by a surviving hash join.
///
/// Property scans may receive dynamic filters from operators that need the
/// scan to produce rows before those filters can complete (for example a
/// bounded sort or aggregate). The scan records candidate filters during
/// pushdown, but waits on none until this final rule confirms the producer is a
/// surviving `HashJoinExec` in the completed physical plan.
#[derive(Debug, Default)]
pub struct PropertyFilterApprovalRule;

impl PhysicalOptimizerRule for PropertyFilterApprovalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut approved_ids = BTreeSet::new();
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>()
                && let Some(filter) = join.dynamic_filter_expr()
                && let Some(expression_id) = filter.expression_id()
            {
                approved_ids.insert(expression_id);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        plan.transform_up(|node| {
            let Some(scan) = node.downcast_ref::<crate::property_scan::PropertyOverlayExec>()
            else {
                return Ok(Transformed::no(node));
            };
            let approved = scan.approve_uuid_filters(&approved_ids);
            Ok(Transformed::yes(
                Arc::new(approved) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "graphforge_property_filter_approval"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
