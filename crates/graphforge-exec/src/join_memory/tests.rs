//! #1985: where [`HashJoinMemoryRule`] places a hash join, from the sizes its
//! inputs publish. The inputs are plans that publish exactly the statistics a
//! test names, so each placement is decided by the estimates alone.

use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, NullEquality, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
    displayable,
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
