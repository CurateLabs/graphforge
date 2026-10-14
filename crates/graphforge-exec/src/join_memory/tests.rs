//! #1985: where [`HashJoinMemoryRule`] places a hash join, from the sizes its
//! inputs publish. The inputs are plans that publish exactly the statistics a
//! test names, so each placement is decided by the estimates alone.

use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, NullEquality, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, Partitioning, PlanProperties,
    SendableRecordBatchStream, displayable,
};
use datafusion::prelude::JoinType;

const MIB: usize = 1024 * 1024;
/// Half of it bounds a build side: 8 MiB.
const BUDGET: usize = 16 * MIB;

/// A leaf that publishes the statistics it was built with and executes nothing.
#[derive(Debug)]
struct SizedExec {
    label: &'static str,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl SizedExec {
    fn new(
        label: &'static str,
        value: &str,
        rows: Precision<usize>,
        bytes: Precision<usize>,
    ) -> Arc<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(value, DataType::Int64, false),
        ]));
        let mut statistics = Statistics::new_unknown(&schema);
        statistics.num_rows = rows;
        statistics.total_byte_size = bytes;
        Arc::new(Self {
            label,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            statistics: Arc::new(statistics),
            schema,
        })
    }

    /// Publishes `bytes` and no row count.
    fn bytes(label: &'static str, value: &str, bytes: usize) -> Arc<Self> {
        Self::new(label, value, Precision::Absent, Precision::Inexact(bytes))
    }

    /// Publishes nothing, and is ordered by the join key.
    fn sorted(label: &'static str, value: &str) -> Arc<Self> {
        let base = Self::unknown(label, value);
        let mut equivalence = EquivalenceProperties::new(Arc::clone(&base.schema));
        equivalence.add_ordering([PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))]);
        Arc::new(Self {
            label,
            schema: Arc::clone(&base.schema),
            statistics: Arc::clone(&base.statistics),
            properties: Arc::new(PlanProperties::new(
                equivalence,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    /// Publishes nothing.
    fn unknown(label: &'static str, value: &str) -> Arc<Self> {
        Self::new(label, value, Precision::Absent, Precision::Absent)
    }
}

impl DisplayAs for SizedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SizedExec: {}", self.label)
    }
}

impl ExecutionPlan for SizedExec {
    fn name(&self) -> &str {
        "SizedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "a SizedExec only publishes statistics".into(),
        ))
    }

    fn partition_statistics(&self, _: Option<usize>) -> Result<Arc<Statistics>, DataFusionError> {
        Ok(Arc::clone(&self.statistics))
    }
}

fn hash_join(
    left: Arc<SizedExec>,
    right: Arc<SizedExec>,
    projection: Option<Vec<usize>>,
) -> Arc<dyn ExecutionPlan> {
    let on = vec![(
        Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
    )];
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            projection,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("a hash join over two leaves"),
    )
}

fn place(join: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    HashJoinMemoryRule::new(BUDGET)
        .optimize(Arc::clone(join), &ConfigOptions::default())
        .expect("the rule places the join")
}

fn text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(false).to_string()
}

/// The build side of the one hash join in `plan`.
fn build_side(plan: &Arc<dyn ExecutionPlan>) -> String {
    let mut build = None;
    plan.apply(|node| {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            build = Some(displayable(join.left().as_ref()).one_line().to_string());
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("walking the plan");
    build.unwrap_or_else(|| panic!("no hash join in:\n{}", text(plan)))
}

#[test]
fn a_build_side_that_fits_keeps_the_hash_join() {
    let join = hash_join(
        SizedExec::bytes("build", "a", 2 * MIB),
        SizedExec::bytes("probe", "b", 512 * MIB),
        None,
    );
    let placed = place(&join);
    assert_eq!(
        build_side(&placed).trim(),
        "SizedExec: build",
        "{}",
        text(&placed)
    );
    assert!(
        !text(&placed).contains("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
}

#[test]
fn an_over_budget_build_side_swaps_with_a_probe_side_that_fits() {
    let join = hash_join(
        SizedExec::bytes("large", "a", 512 * MIB),
        SizedExec::bytes("small", "b", 2 * MIB),
        None,
    );
    let placed = place(&join);
    assert_eq!(
        build_side(&placed).trim(),
        "SizedExec: small",
        "{}",
        text(&placed)
    );
    assert_eq!(placed.schema(), join.schema(), "the swap keeps the columns");
}

#[test]
fn a_build_side_without_statistics_swaps_with_a_probe_side_that_fits() {
    // DataFusion's own join selection swaps only when it knows both sizes.
    let join = hash_join(
        SizedExec::unknown("unknown", "a"),
        SizedExec::bytes("small", "b", 2 * MIB),
        None,
    );
    let placed = place(&join);
    assert_eq!(
        build_side(&placed).trim(),
        "SizedExec: small",
        "{}",
        text(&placed)
    );
}

#[test]
fn a_join_with_no_side_that_fits_becomes_a_sort_merge_join() {
    let join = hash_join(
        SizedExec::bytes("large", "a", 512 * MIB),
        SizedExec::bytes("larger", "b", 1024 * MIB),
        None,
    );
    let placed = place(&join);
    assert!(
        text(&placed).starts_with("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
    assert_eq!(placed.schema(), join.schema());
}

#[test]
fn a_join_whose_sides_publish_no_statistics_becomes_a_sort_merge_join() {
    // Nothing bounds what an unsized build side holds at run time, so only a
    // join that can spill is safe.
    let join = hash_join(
        SizedExec::unknown("left", "a"),
        SizedExec::unknown("right", "b"),
        None,
    );
    let placed = place(&join);
    assert!(
        text(&placed).starts_with("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
}

#[test]
fn a_sort_merge_join_keeps_the_columns_a_hash_join_projected() {
    let join = hash_join(
        SizedExec::unknown("left", "a"),
        SizedExec::unknown("right", "b"),
        Some(vec![1, 3]),
    );
    assert_eq!(join.schema().fields().len(), 2);
    let placed = place(&join);
    assert!(
        text(&placed).contains("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
    assert_eq!(placed.schema(), join.schema(), "{}", text(&placed));
}

#[test]
fn a_row_count_is_priced_at_the_width_of_a_row() {
    // Two Int64 columns: 16 bytes a row, so 8 MiB holds 524,288 rows.
    let at_limit = hash_join(
        SizedExec::new("build", "a", Precision::Inexact(500_000), Precision::Absent),
        SizedExec::unknown("probe", "b"),
        None,
    );
    assert_eq!(build_side(&place(&at_limit)).trim(), "SizedExec: build");

    let over_limit = hash_join(
        SizedExec::new("build", "a", Precision::Inexact(600_000), Precision::Absent),
        SizedExec::unknown("probe", "b"),
        None,
    );
    let placed = place(&over_limit);
    assert!(
        text(&placed).starts_with("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
}

// -- UnsizedBuildJoinRule ----------------------------------------------------

const RUN_BYTES: usize = MIB;

fn join_of(
    join_type: JoinType,
    mode: PartitionMode,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    projection: Option<Vec<usize>>,
) -> Arc<dyn ExecutionPlan> {
    let on = vec![(
        Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
    )];
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            projection,
            mode,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("a hash join over two inputs"),
    )
}

fn plan_late(join: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    UnsizedBuildJoinRule::new(RUN_BYTES)
        .optimize(Arc::clone(join), &ConfigOptions::default())
        .expect("the late rule plans the join")
}

fn key() -> Vec<Arc<dyn PhysicalExpr>> {
    vec![Arc::new(Column::new("k", 0))]
}

fn hash_exchange(input: Arc<dyn ExecutionPlan>, partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        RepartitionExec::try_new(input, Partitioning::Hash(key(), partitions))
            .expect("a hash exchange"),
    )
}

fn round_robin(input: Arc<dyn ExecutionPlan>, partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(partitions))
            .expect("a round-robin exchange"),
    )
}

fn count(plan: &Arc<dyn ExecutionPlan>, needle: &str) -> usize {
    text(plan).matches(needle).count()
}

#[test]
fn a_left_join_with_an_unsized_build_side_becomes_a_sorted_sort_merge_join() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::CollectLeft,
        SizedExec::unknown("frontier", "a"),
        SizedExec::bytes("scan", "b", 512 * MIB),
        None,
    );
    let placed = plan_late(&join);
    let plan = text(&placed);
    assert!(plan.starts_with("SortMergeJoinExec"), "{plan}");
    assert_eq!(count(&placed, "SortExec"), 2, "a sort per input:\n{plan}");
    assert_eq!(
        count(&placed, "SortRunCoalesceExec"),
        2,
        "each sort reads coalesced runs:\n{plan}"
    );
    assert_eq!(count(&placed, "RepartitionExec"), 0, "{plan}");
    assert_eq!(placed.schema(), join.schema());
}

#[test]
fn the_sort_merge_join_keeps_the_columns_the_hash_join_projected() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::CollectLeft,
        SizedExec::unknown("frontier", "a"),
        SizedExec::unknown("scan", "b"),
        Some(vec![1, 3]),
    );
    assert_eq!(join.schema().fields().len(), 2);
    let placed = plan_late(&join);
    assert!(
        text(&placed).contains("SortMergeJoinExec"),
        "{}",
        text(&placed)
    );
    assert_eq!(placed.schema(), join.schema(), "{}", text(&placed));
}

#[test]
fn a_left_join_with_a_sized_build_side_stays_a_hash_join() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::CollectLeft,
        SizedExec::bytes("frontier", "a", MIB),
        SizedExec::bytes("scan", "b", 512 * MIB),
        None,
    );
    let placed = plan_late(&join);
    assert_eq!(text(&placed), text(&join));
}

#[test]
fn a_join_that_is_not_a_left_join_is_left_to_the_memory_rule() {
    let join = join_of(
        JoinType::Inner,
        PartitionMode::CollectLeft,
        SizedExec::unknown("frontier", "a"),
        SizedExec::bytes("scan", "b", 512 * MIB),
        None,
    );
    let placed = plan_late(&join);
    assert_eq!(text(&placed), text(&join));
}

#[test]
fn a_partitioned_join_reuses_the_exchanges_that_already_feed_it() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::Partitioned,
        hash_exchange(SizedExec::unknown("frontier", "a"), 4),
        hash_exchange(SizedExec::bytes("scan", "b", 512 * MIB), 4),
        None,
    );
    let placed = plan_late(&join);
    let plan = text(&placed);
    assert!(plan.starts_with("SortMergeJoinExec"), "{plan}");
    assert_eq!(
        count(&placed, "RepartitionExec"),
        2,
        "no second exchange:\n{plan}"
    );
    assert_eq!(count(&placed, "Hash("), 2, "{plan}");
    assert_eq!(count(&placed, "SortExec"), 2, "{plan}");
    assert_eq!(
        placed.output_partitioning().partition_count(),
        4,
        "the output keeps the join's partitioning:\n{plan}"
    );
}

#[test]
fn a_collect_left_join_over_a_partitioned_probe_side_hashes_both_inputs() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::CollectLeft,
        SizedExec::unknown("frontier", "a"),
        round_robin(SizedExec::bytes("scan", "b", 512 * MIB), 4),
        None,
    );
    assert_eq!(join.output_partitioning().partition_count(), 4);
    let placed = plan_late(&join);
    let plan = text(&placed);
    assert!(plan.starts_with("SortMergeJoinExec"), "{plan}");
    assert_eq!(
        count(&placed, "Hash("),
        2,
        "both inputs are hashed:\n{plan}"
    );
    assert_eq!(
        count(&placed, "RoundRobinBatch"),
        0,
        "the round-robin exchange is replaced, not stacked:\n{plan}"
    );
    assert_eq!(count(&placed, "SortExec"), 2, "{plan}");
    assert_eq!(placed.output_partitioning().partition_count(), 4, "{plan}");
}

#[test]
fn a_probe_already_sorted_by_the_join_keys_is_not_sorted_again() {
    let join = join_of(
        JoinType::Left,
        PartitionMode::CollectLeft,
        SizedExec::unknown("frontier", "a"),
        SizedExec::sorted("scan", "b"),
        None,
    );
    let placed = plan_late(&join);
    assert_eq!(count(&placed, "SortExec"), 1, "{}", text(&placed));
}

#[path = "execution_tests.rs"]
mod execution_tests;
