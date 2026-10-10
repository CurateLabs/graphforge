//! Final-plan authority for exact property-scan UUID nominations.

mod equality_anchor;
mod exchanges;
mod left_enrichment;

use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::equivalence::AcrossPartitions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::repartition::RepartitionExec;

use crate::property_join_nomination::{UuidBuildKeyNomination, UuidBuildKeyTapExec};
use crate::property_scan::PropertyOverlayExec;

/// Approves only finite UUID keys collected from a surviving join's build side.
///
/// Dynamic-filter expressions can also be emitted by operators whose scan must
/// run before the expression completes. This final rule binds an approved
/// property scan to the matching live hash join and taps that join's already
/// required build input. The join predicate remains authoritative.
#[derive(Debug, Default)]
pub struct PropertyFilterApprovalRule;

/// Opt-in authority for a selected expansion endpoint whose UUID is hidden by
/// a join projection. The callback is supplied by the execution crate, which
/// owns the expansion node and can prove the exact output-column lineage.
type SelectedEndpointPredicate = dyn Fn(&dyn ExecutionPlan, usize) -> bool + Send + Sync;

/// Optimizer rule that nominates only a proven selected expansion endpoint.
#[derive(Clone)]
pub struct SelectedEndpointPropertyFilterApprovalRule {
    selected_endpoint: Arc<SelectedEndpointPredicate>,
}

/// Check whether a physical node is the strict INNER UUID equality join used
/// to choose an expansion seed.
pub fn is_filtered_uuid_seed(plan: &dyn ExecutionPlan, source_node_id_index: usize) -> bool {
    equality_anchor::is_filtered_uuid_seed(plan, source_node_id_index)
}

impl std::fmt::Debug for SelectedEndpointPropertyFilterApprovalRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectedEndpointPropertyFilterApprovalRule")
            .finish()
    }
}

impl SelectedEndpointPropertyFilterApprovalRule {
    /// Create a selected-endpoint rule with exec-owned column-lineage proof.
    #[must_use]
    pub fn new(
        selected_endpoint: impl Fn(&dyn ExecutionPlan, usize) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            selected_endpoint: Arc::new(selected_endpoint),
        }
    }
}

impl PhysicalOptimizerRule for SelectedEndpointPropertyFilterApprovalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if *join.join_type() == JoinType::Right
                && *join.partition_mode() == PartitionMode::CollectLeft
            {
                let Some((_, frontier_key)) = join.on().first() else {
                    return Ok(Transformed::no(node));
                };
                let Some(frontier_key) = frontier_key.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(node));
                };
                let selected =
                    (self.selected_endpoint)(join.right().as_ref(), frontier_key.index());
                if !selected {
                    return Ok(Transformed::no(node));
                }
                let Some(rebuilt) = nominate_collect_left_right_selected(join)? else {
                    return Ok(Transformed::no(node));
                };
                return Ok(Transformed::yes(rebuilt));
            }
            if *join.join_type() != JoinType::Left || join.on().len() != 1 {
                return Ok(Transformed::no(node));
            }
            let Some((left_key, _)) = join.on().first() else {
                return Ok(Transformed::no(node));
            };
            let Some(left_key) = left_key.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };
            let selected = (self.selected_endpoint)(join.left().as_ref(), left_key.index());
            if !selected {
                return Ok(Transformed::no(node));
            }
            let rebuilt = match *join.partition_mode() {
                PartitionMode::Partitioned => left_enrichment::nominate_partitioned_left_selected(
                    join,
                    Some(left_key.index()),
                )?,
                PartitionMode::CollectLeft => {
                    nominate_existing_collect_left_selected(join, Some(left_key.index()))?
                }
                PartitionMode::Auto => None,
            };
            let Some(rebuilt) = rebuilt else {
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(rebuilt))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "graphforge_selected_endpoint_property_filter_approval"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl PhysicalOptimizerRule for PropertyFilterApprovalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if let Some(rebuilt) = equality_anchor::shared_equality_build(join)? {
                return Ok(Transformed::yes(rebuilt));
            }
            if *join.join_type() == JoinType::Right {
                let Some(rebuilt) = nominate_collect_left_right(join)? else {
                    return Ok(Transformed::no(node));
                };
                return Ok(Transformed::yes(rebuilt));
            }
            if *join.join_type() == JoinType::Left {
                if let Some(rebuilt) = left_enrichment::nominate_partitioned_left(join)? {
                    return Ok(Transformed::yes(rebuilt));
                }
                let Some(rebuilt) = nominate_existing_collect_left(join)? else {
                    return Ok(Transformed::no(node));
                };
                return Ok(Transformed::yes(rebuilt));
            }
            if !matches!(*join.join_type(), JoinType::Inner | JoinType::RightSemi)
                || !matches!(
                    *join.partition_mode(),
                    PartitionMode::Partitioned | PartitionMode::CollectLeft
                )
                || join.left().boundedness().is_unbounded()
                || join.right().boundedness().is_unbounded()
                || join.left().downcast_ref::<UuidBuildKeyTapExec>().is_some()
            {
                return Ok(Transformed::no(node));
            }

            let Some(dynamic_filter) = join.dynamic_filter_expr() else {
                return Ok(Transformed::no(node));
            };
            let Some(expression_id) = dynamic_filter.expression_id() else {
                return Ok(Transformed::no(node));
            };

            let candidate = find_matching_candidate(join.right(), expression_id, join)?;
            let Some(candidate) = candidate else {
                return Ok(Transformed::no(node));
            };
            let Some((build_column, probe_expression)) =
                join.on().iter().find_map(|(build, probe)| {
                    probe
                        .dyn_eq(candidate.original_probe_key.as_ref())
                        .then_some((build, probe))
                })
            else {
                return Ok(Transformed::no(node));
            };
            let Some(build_column) = build_column.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };
            let Some(probe_column) = probe_expression.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };

            let build_schema = join.left().schema();
            let probe_schema = join.right().schema();
            let Some(build_field) = build_schema.fields().get(build_column.index()) else {
                return Ok(Transformed::no(node));
            };
            let Some(probe_field) = probe_schema.fields().get(probe_column.index()) else {
                return Ok(Transformed::no(node));
            };
            if build_field.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16)
                || probe_field.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16)
                || probe_field.name() != probe_column.name()
            {
                return Ok(Transformed::no(node));
            }

            let nomination = UuidBuildKeyNomination::new();
            let replacement_right = attach_nomination(
                Arc::clone(join.right()),
                expression_id,
                &candidate.original_probe_key,
                &nomination,
            )?;
            let coalesced_left: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(Arc::clone(join.left())));
            let tapped_left: Arc<dyn ExecutionPlan> = Arc::new(UuidBuildKeyTapExec::new(
                coalesced_left,
                build_column.index(),
                nomination,
            ));
            let rebuilt = join
                .builder()
                .with_new_children(vec![tapped_left, replacement_right])?
                .with_partition_mode(PartitionMode::CollectLeft)
                .recompute_properties()
                .build_exec()?;
            Ok(Transformed::yes(rebuilt))
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

/// A projected-away hash key is valid distribution metadata but cannot be
/// evaluated by a repartition inserted after a join rewrite.
fn has_unknown_hash_key(partitioning: &Partitioning, plan: &dyn ExecutionPlan) -> bool {
    let Partitioning::Hash(keys, _) = partitioning else {
        return false;
    };
    keys.iter().any(|key| {
        matches!(
            key.data_type(plan.schema().as_ref()),
            Ok(arrow::datatypes::DataType::Null) | Err(_)
        )
    })
}

/// Reorient the exact physical shape used for a partitioned destination
/// enrichment: a direct property scan is the build side of CollectLeft RIGHT,
/// and the preserved frontier produces the parent's RoundRobin partitions.
/// Swapping makes the frontier the single LEFT-preserved build side, so a
/// completed frontier UUID nomination can safely prune only that direct scan.
fn nominate_collect_left_right(
    join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    nominate_collect_left_right_inner(join, false)
}

fn nominate_collect_left_right_selected(
    join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    nominate_collect_left_right_inner(join, true)
}

fn nominate_collect_left_right_inner(
    join: &HashJoinExec,
    selected_endpoint: bool,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    if !collect_left_right_is_eligible(join) {
        return Ok(None);
    }

    let join_plan: &dyn ExecutionPlan = join;
    let output_partitioning = join_plan.output_partitioning().clone();
    let Some(scan) = join.left().downcast_ref::<PropertyOverlayExec>() else {
        return Ok(None);
    };
    let uuid_index = if selected_endpoint {
        scan.fresh_selected_endpoint_uuid_column()
    } else {
        scan.nomination_uuid_column()
    };
    let Some(uuid_index) = uuid_index else {
        return Ok(None);
    };
    let scan_schema = scan.schema();
    let Some(uuid_field) = scan_schema.fields().get(uuid_index) else {
        return Ok(None);
    };

    let frontier_column = join.on().iter().find_map(|(scan_key, frontier_key)| {
        let scan_key = scan_key.downcast_ref::<Column>()?;
        if scan_key.index() != uuid_index
            || scan_key.name() != uuid_field.name()
            || scan_key.data_type(join.left().schema().as_ref()).ok()
                != Some(arrow::datatypes::DataType::FixedSizeBinary(16))
        {
            return None;
        }
        let frontier_key = frontier_key.downcast_ref::<Column>()?;
        (frontier_key.data_type(join.right().schema().as_ref()).ok()
            == Some(arrow::datatypes::DataType::FixedSizeBinary(16)))
        .then_some(frontier_key.index())
    });
    let Some(frontier_column) = frontier_column else {
        return Ok(None);
    };

    // The public swap remaps join keys, JoinFilter, and embedded projection.
    // Only the exact embedded-projection shape is eligible here; rebuilding a
    // wrapper returned for a non-projection join would require broader lineage.
    let swapped = join.swap_inputs(PartitionMode::CollectLeft)?;
    let Some(swapped_join) = swapped.downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    if *swapped_join.join_type() != JoinType::Left
        || *swapped_join.partition_mode() != PartitionMode::CollectLeft
        || swapped_join.schema().as_ref() != join.schema().as_ref()
    {
        return Ok(None);
    }
    let Some(build_column) = swapped_join.on().iter().find_map(|(build, probe)| {
        let build = build.downcast_ref::<Column>()?;
        let probe = probe.downcast_ref::<Column>()?;
        (build.index() == frontier_column
            && build.data_type(swapped_join.left().schema().as_ref()).ok()
                == Some(arrow::datatypes::DataType::FixedSizeBinary(16))
            && probe.index() == uuid_index
            && probe.name() == uuid_field.name()
            && probe.data_type(swapped_join.right().schema().as_ref()).ok()
                == Some(arrow::datatypes::DataType::FixedSizeBinary(16)))
        .then_some(build.index())
    }) else {
        return Ok(None);
    };

    let nomination = UuidBuildKeyNomination::new();
    let coalesced_frontier: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(Arc::clone(swapped_join.left())));
    let tapped_frontier: Arc<dyn ExecutionPlan> = Arc::new(UuidBuildKeyTapExec::new(
        coalesced_frontier,
        build_column,
        Arc::clone(&nomination),
    ));
    let Some(swapped_scan) = swapped_join.right().downcast_ref::<PropertyOverlayExec>() else {
        return Ok(None);
    };
    let nominated_scan: Arc<dyn ExecutionPlan> =
        Arc::new(swapped_scan.with_uuid_nomination(nomination));
    let rewritten = swapped_join
        .builder()
        .with_new_children(vec![tapped_frontier, nominated_scan])?
        .reset_state()
        .recompute_properties()
        .build_exec()?;
    if rewritten.schema().as_ref() != join.schema().as_ref() {
        return Ok(None);
    }
    let restored: Arc<dyn ExecutionPlan> =
        Arc::new(RepartitionExec::try_new(rewritten, output_partitioning)?);
    Ok(Some(restored))
}

fn collect_left_right_is_eligible(join: &HashJoinExec) -> bool {
    let join_plan: &dyn ExecutionPlan = join;
    *join.partition_mode() == PartitionMode::CollectLeft
        && join.contains_projection()
        && join.fetch().is_none()
        && join_plan.output_ordering().is_none()
        && !join.left().boundedness().is_unbounded()
        && !join.right().boundedness().is_unbounded()
        && !join_plan
            .equivalence_properties()
            .constants()
            .iter()
            .any(|constant| constant.across_partitions == AcrossPartitions::Heterogeneous)
        && matches!(join_plan.output_partitioning(), Partitioning::RoundRobinBatch(partitions) if *partitions > 0)
}

struct MatchingCandidate {
    original_probe_key: Arc<dyn PhysicalExpr>,
}

fn find_matching_candidate(
    right: &Arc<dyn ExecutionPlan>,
    expression_id: u64,
    join: &HashJoinExec,
) -> Result<Option<MatchingCandidate>, DataFusionError> {
    let mut match_candidate = None;
    right.apply(|node| {
        if let Some(scan) = node.downcast_ref::<PropertyOverlayExec>() {
            for candidate in scan.uuid_filter_candidates() {
                if candidate.expression_id == expression_id
                    && join
                        .on()
                        .iter()
                        .any(|(_, probe)| probe.dyn_eq(candidate.original_probe_key.as_ref()))
                {
                    match_candidate = Some(MatchingCandidate {
                        original_probe_key: candidate.original_probe_key,
                    });
                    break;
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
    Ok(match_candidate)
}

/// An existing single-partition LEFT enrichment join can safely nominate
/// direct property reads from its build UUIDs without changing join semantics
/// or output properties. Keep the association on this exact direct scan edge.
fn nominate_existing_collect_left(
    join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    nominate_existing_collect_left_inner(join, None)
}

fn nominate_existing_collect_left_selected(
    join: &HashJoinExec,
    selected_key: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    nominate_existing_collect_left_inner(join, selected_key)
}

fn nominate_existing_collect_left_inner(
    join: &HashJoinExec,
    selected_key: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    if *join.partition_mode() != PartitionMode::CollectLeft
        || join.left().output_partitioning().partition_count() != 1
        || join.left().boundedness().is_unbounded()
        || join.right().boundedness().is_unbounded()
        || join.left().downcast_ref::<UuidBuildKeyTapExec>().is_some()
    {
        return Ok(None);
    }
    let Some(scan) = join.right().downcast_ref::<PropertyOverlayExec>() else {
        return Ok(None);
    };
    let uuid_index = if selected_key.is_some() {
        scan.fresh_selected_endpoint_uuid_column()
    } else {
        scan.nomination_uuid_column()
    };
    let Some(uuid_index) = uuid_index else {
        return Ok(None);
    };
    let scan_schema = scan.schema();
    let Some(uuid_field) = scan_schema.fields().get(uuid_index) else {
        return Ok(None);
    };

    let matching_build_column = join.on().iter().find_map(|(build, probe)| {
        let probe = probe.downcast_ref::<Column>()?;
        if probe.index() != uuid_index
            || probe.name() != uuid_field.name()
            || probe.data_type(join.right().schema().as_ref()).ok()
                != Some(arrow::datatypes::DataType::FixedSizeBinary(16))
        {
            return None;
        }
        let build = build.downcast_ref::<Column>()?;
        (build.data_type(join.left().schema().as_ref()).ok()
            == Some(arrow::datatypes::DataType::FixedSizeBinary(16)))
        .then_some(build.index())
    });
    let Some(build_column) = matching_build_column else {
        return Ok(None);
    };
    if selected_key.is_some_and(|selected| selected != build_column) {
        return Ok(None);
    }

    let nomination = UuidBuildKeyNomination::new();
    let tapped_left: Arc<dyn ExecutionPlan> = Arc::new(UuidBuildKeyTapExec::new(
        Arc::clone(join.left()),
        build_column,
        Arc::clone(&nomination),
    ));
    let nominated_right: Arc<dyn ExecutionPlan> = Arc::new(scan.with_uuid_nomination(nomination));
    let rebuilt = join
        .builder()
        .with_new_children(vec![tapped_left, nominated_right])?
        .recompute_properties()
        .build_exec()?;
    Ok(Some(rebuilt))
}

fn attach_nomination(
    right: Arc<dyn ExecutionPlan>,
    expression_id: u64,
    original_probe_key: &Arc<dyn PhysicalExpr>,
    nomination: &Arc<UuidBuildKeyNomination>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    right
        .transform_up(|node| {
            let Some(scan) = node.downcast_ref::<PropertyOverlayExec>() else {
                return Ok(Transformed::no(node));
            };
            let matches = scan.uuid_filter_candidates().iter().any(|candidate| {
                candidate.expression_id == expression_id
                    && candidate
                        .original_probe_key
                        .dyn_eq(original_probe_key.as_ref())
            });
            if !matches {
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::yes(
                Arc::new(scan.with_uuid_nomination(Arc::clone(nomination)))
                    as Arc<dyn ExecutionPlan>,
            ))
        })
        .map(|transformed| transformed.data)
}
