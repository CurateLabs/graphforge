//! A nominated scan must wait for the entire preserved frontier, even when
//! that frontier already advertises the join's key ordering.

use super::*;
use std::time::Duration;

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, StringArray};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::equivalence::{AcrossPartitions, ConstExpr};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use graphforge_storage::PropertyFilterApprovalRule;
use graphforge_storage::catalog::PropertyTable;

/// Real, immutable sorted batches with absent size estimates, as an expansion
/// followed by a key sort may publish. The source executes its two batches;
/// only its planner statistics differ from the ordinary memory source's.
#[derive(Debug)]
struct SortedUuidFrontier {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SortedUuidFrontier {
    fn new(known_constant: bool) -> Arc<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("padding", DataType::Utf8, false),
        ]));
        let batches = [1_u8, if known_constant { 1 } else { 2 }]
            .map(|value| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(
                            FixedSizeBinaryArray::try_from_iter([[value; 16]].into_iter()).unwrap(),
                        ),
                        // A run must emit before EOF; otherwise coalescing the
                        // whole tiny frontier hides a sort that bypasses itself
                        // at runtime when its input already advertises ordering.
                        Arc::new(StringArray::from(vec!["x".repeat(RUN_BYTES + 1)])),
                    ],
                )
                .unwrap()
            })
            .to_vec();
        let input =
            MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
        let mut equivalence = EquivalenceProperties::new(schema);
        equivalence.add_ordering([PhysicalSortExpr::new_default(Arc::new(Column::new(
            "node_uuid",
            0,
        )))]);
        if known_constant {
            equivalence
                .add_constants([ConstExpr::new(
                    Arc::new(Column::new("node_uuid", 0)),
                    AcrossPartitions::Uniform(Some(
                        datafusion::scalar::ScalarValue::FixedSizeBinary(16, Some(vec![1; 16])),
                    )),
                )])
                .unwrap();
        }
        Arc::new(Self {
            input,
            properties: Arc::new(PlanProperties::new(
                equivalence,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }
}

impl DisplayAs for SortedUuidFrontier {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortedUuidFrontier")
    }
}

impl ExecutionPlan for SortedUuidFrontier {
    fn name(&self) -> &'static str {
        "SortedUuidFrontier"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "an immutable frontier is a leaf");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn partition_statistics(&self, _: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

async fn nominated_enrichment(
    directory: &std::path::Path,
    context: &SessionContext,
    known_constant: bool,
) -> Arc<dyn ExecutionPlan> {
    // An absent property route is a supported empty scan. Without an
    // inventory its row estimate is unknown, so the nomination cannot abandon
    // early: the scan really waits until the tap has observed EOF.
    let property_schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let table = PropertyTable::open(directory, "Person", property_schema);
    let scan = table.scan(&context.state(), None, &[], None).await.unwrap();
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let join: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            SortedUuidFrontier::new(known_constant),
            scan,
            vec![(Arc::clone(&key), key)],
            None,
            &JoinType::Left,
            Some(vec![0, 3]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    assert!(estimated_bytes(join.children()[0]).is_none());
    PropertyFilterApprovalRule
        .optimize(join, &ConfigOptions::default())
        .unwrap()
}

#[tokio::test]
async fn an_ordered_frontier_finishes_its_nomination_before_the_sort_merge_probe_reads() {
    assert_frontier_drains(false).await;
}

#[tokio::test]
async fn a_known_constant_frontier_finishes_its_nomination_before_the_probe_reads() {
    assert_frontier_drains(true).await;
}

async fn assert_frontier_drains(known_constant: bool) {
    let dir = tempfile::TempDir::new().unwrap();
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1),
    );
    // Reconstruct the previous rule's ordered-left reuse over the same real
    // nomination: the right side is sorted, but the ordered tap streams its
    // first batch without finishing the nomination. The probe then waits for
    // EOF while the sort-merge join waits for its first probe batch.
    let before = nominated_enrichment(dir.path(), &context, known_constant).await;
    let previous = {
        let fixed = plan_late(&before);
        let full_join = Arc::clone(fixed.children()[0]);
        let sorted_join = full_join.downcast_ref::<SortMergeJoinExec>().unwrap();
        let hash_join = before.downcast_ref::<HashJoinExec>().unwrap();
        sort_merge_over(
            hash_join,
            Arc::clone(hash_join.left()),
            Arc::clone(sorted_join.right()),
        )
        .unwrap()
    };
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            collect(previous, context.task_ctx()),
        )
        .await
        .is_err(),
        "reusing the ordered tap creates a dependency cycle"
    );

    // Nomination execution state belongs to one execution, so the fixed plan
    // is built over a fresh association, not the cancelled reference's tap.
    let nominated = nominated_enrichment(dir.path(), &context, known_constant).await;
    assert!(text(&nominated).contains("UuidBuildKeyTapExec"));
    let placed = plan_late(&nominated);
    assert!(text(&placed).contains("SortMergeJoinExec"));
    // A later tree rebuild must retain the ordering mask that forces the
    // left sort to materialize. Rebuild that exact coalescer and its parents,
    // then execute the reconstructed plan rather than only checking metadata.
    let full_join = Arc::clone(placed.children()[0]);
    let left_sort = Arc::clone(full_join.children()[0]);
    let coalesced = Arc::clone(left_sort.children()[0]);
    let ordering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "node_uuid",
        0,
    )))];
    assert!(
        coalesced.children()[0]
            .equivalence_properties()
            .ordering_satisfy(ordering.iter().cloned())
            .unwrap()
    );
    assert!(
        !coalesced
            .equivalence_properties()
            .ordering_satisfy(ordering.iter().cloned())
            .unwrap()
    );
    assert!(coalesced.output_ordering().is_none());
    let rebuilt_coalescer = Arc::clone(&coalesced)
        .with_new_children(vec![Arc::clone(coalesced.children()[0])])
        .unwrap();
    assert!(rebuilt_coalescer.output_ordering().is_none());
    assert!(
        !rebuilt_coalescer
            .equivalence_properties()
            .ordering_satisfy(ordering.iter().cloned())
            .unwrap()
    );
    let rebuilt_left = left_sort
        .with_new_children(vec![rebuilt_coalescer])
        .unwrap();
    let rebuilt_join = Arc::clone(&full_join)
        .with_new_children(vec![rebuilt_left, Arc::clone(full_join.children()[1])])
        .unwrap();
    let placed = placed.with_new_children(vec![rebuilt_join]).unwrap();
    let batches = tokio::time::timeout(Duration::from_secs(5), collect(placed, context.task_ctx()))
        .await
        .expect("the left input must drain before the nominated probe can read")
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row));
            assert!(
                payloads.is_null(row),
                "the absent route has no payload rows"
            );
            rows.push(ids.value(row).to_vec());
        }
    }
    assert_eq!(
        rows,
        [vec![1; 16], vec![if known_constant { 1 } else { 2 }; 16]]
    );
}
