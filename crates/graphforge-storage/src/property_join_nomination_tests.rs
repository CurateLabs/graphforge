use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeBinaryBuilder, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::NullEquality;
use datafusion::common::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use futures::StreamExt;

use crate::property_filter_approval::PropertyFilterApprovalRule;
use crate::property_join_nomination::{UuidBuildKeyNomination, UuidBuildKeyTapExec};
use crate::property_overlay::{EqualityValue, PropertyEquality};
use crate::property_scan::{PropertyOverlayExec, PropertyScanOptions};

fn input_with_values(values: &[Option<[u8; 16]>]) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        true,
    )]));
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value).unwrap(),
            None => builder.append_null(),
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())]).unwrap();
    let partitions = if values.is_empty() {
        vec![Vec::new()]
    } else {
        vec![vec![batch]]
    };
    TestMemoryExec::try_new_exec(&partitions, schema, None).unwrap()
}

#[tokio::test]
async fn tap_forwards_batches_and_publishes_deduplicated_non_null_uuid_keys() {
    let first = 9_u128.to_be_bytes();
    let second = 2_u128.to_be_bytes();
    let input = input_with_values(&[Some(first), None, Some(second), Some(first)]);
    let nomination = UuidBuildKeyNomination::new();
    let tap = UuidBuildKeyTapExec::new(input, 0, Arc::clone(&nomination));
    let mut stream = tap.execute(0, Arc::new(TaskContext::default())).unwrap();

    let forwarded = stream.next().await.unwrap().unwrap();
    assert_eq!(forwarded.num_rows(), 4);
    assert!(stream.next().await.is_none());
    assert_eq!(nomination.ids(), Some(&BTreeSet::from([second, first])));
}

#[tokio::test]
async fn tap_drop_wakes_waiters_without_publishing_partial_keys() {
    let first = 9_u128.to_be_bytes();
    let input = input_with_values(&[Some(first)]);
    let nomination = UuidBuildKeyNomination::new();
    let tap = UuidBuildKeyTapExec::new(input, 0, Arc::clone(&nomination));
    let mut stream = tap.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    drop(stream);

    let (consumer, _receiver) = tokio::sync::mpsc::channel(1);
    let result = tokio::time::timeout(Duration::from_secs(1), nomination.wait(&consumer))
        .await
        .expect("dropping the tap must wake nomination waiters");
    assert!(result.is_err());
    assert!(nomination.ids().is_none());
}

#[tokio::test]
async fn tap_batch_error_wakes_waiters_without_publishing_partial_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        true,
    )]));
    let invalid = RecordBatch::try_from_iter(vec![(
        "node_uuid",
        Arc::new(StringArray::from(vec!["not-a-uuid-array"])) as _,
    )])
    .unwrap();
    let input: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![invalid]], schema, None).unwrap();
    let nomination = UuidBuildKeyNomination::new();
    let tap = UuidBuildKeyTapExec::new(input, 0, Arc::clone(&nomination));
    let mut stream = tap.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_err());
    drop(stream);

    let (consumer, _receiver) = tokio::sync::mpsc::channel(1);
    let result = tokio::time::timeout(Duration::from_secs(1), nomination.wait(&consumer))
        .await
        .expect("tap errors must wake nomination waiters");
    assert!(result.is_err());
    assert!(nomination.ids().is_none());
}

#[tokio::test]
async fn empty_build_publishes_a_complete_empty_uuid_set() {
    let input = input_with_values(&[]);
    let nomination = UuidBuildKeyNomination::new();
    let tap = UuidBuildKeyTapExec::new(input, 0, Arc::clone(&nomination));
    let mut stream = tap.execute(0, Arc::new(TaskContext::default())).unwrap();

    assert!(stream.next().await.is_none());
    assert_eq!(nomination.ids(), Some(&BTreeSet::new()));
}

#[tokio::test]
async fn closed_scan_consumer_cancels_a_pending_nomination_wait() {
    let nomination = UuidBuildKeyNomination::new();
    let (consumer, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);

    let result = tokio::time::timeout(Duration::from_secs(1), nomination.wait(&consumer))
        .await
        .expect("closed scan consumers must cancel nomination waits")
        .unwrap();
    assert!(!result);
}

fn empty_uuid_build() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    TestMemoryExec::try_new_exec(&[Vec::new()], schema, None).unwrap()
}

fn property_scan(limit: Option<usize>, equality: Option<PropertyEquality>) -> PropertyOverlayExec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("ident", DataType::Int64, true),
    ]));
    PropertyOverlayExec::try_new(
        std::path::PathBuf::from("unused"),
        None,
        "Person".into(),
        false,
        schema,
        PropertyScanOptions {
            projection: None,
            limit,
            batch_size: 16,
            footer_statistics: false,
            equality,
        },
    )
    .unwrap()
}

fn join_with_scan(join_type: JoinType, mode: PartitionMode) -> Arc<dyn ExecutionPlan> {
    let left_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let right_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let right: Arc<dyn ExecutionPlan> = Arc::new(property_scan(None, None));
    Arc::new(
        HashJoinExec::try_new(
            empty_uuid_build(),
            right,
            vec![(left_key, right_key)],
            None,
            &join_type,
            None,
            mode,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

fn left_join_with_wrong_uuid_index() -> Arc<dyn ExecutionPlan> {
    let left_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let wrong_probe_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("other_uuid", 1));
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("other_uuid", DataType::FixedSizeBinary(16), false),
    ]));
    let right: Arc<dyn ExecutionPlan> = Arc::new(
        PropertyOverlayExec::try_new(
            std::path::PathBuf::from("unused"),
            None,
            "Person".into(),
            false,
            schema,
            PropertyScanOptions {
                projection: None,
                limit: None,
                batch_size: 16,
                footer_statistics: false,
                equality: None,
            },
        )
        .unwrap(),
    );
    Arc::new(
        HashJoinExec::try_new(
            empty_uuid_build(),
            right,
            vec![(left_key, wrong_probe_key)],
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

fn right_enrichment_join(
    scan: PropertyOverlayExec,
    partitions: usize,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let scan: Arc<dyn ExecutionPlan> = Arc::new(scan);
    let frontier: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            empty_uuid_build(),
            datafusion::physical_expr::Partitioning::RoundRobinBatch(partitions),
        )
        .unwrap(),
    );
    let left_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let right_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let join = HashJoinExec::try_new(
        scan,
        frontier,
        vec![(left_key, right_key)],
        None,
        &JoinType::Right,
        Some(vec![1]),
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
    .builder()
    .with_fetch(fetch)
    .build_exec()
    .unwrap();
    join
}

#[test]
fn direct_collect_left_enrichment_taps_only_eligible_scans() {
    let rule = PropertyFilterApprovalRule;
    let config = ConfigOptions::default();
    let plan = rule
        .optimize(
            join_with_scan(JoinType::Left, PartitionMode::CollectLeft),
            &config,
        )
        .unwrap();
    let join = plan.downcast_ref::<HashJoinExec>().unwrap();
    assert!(join.left().downcast_ref::<UuidBuildKeyTapExec>().is_some());
    assert_eq!(*join.partition_mode(), PartitionMode::CollectLeft);

    for join_type in [JoinType::Full, JoinType::Right, JoinType::RightAnti] {
        let plan = join_with_scan(join_type, PartitionMode::CollectLeft);
        let unchanged = rule.optimize(Arc::clone(&plan), &config).unwrap();
        assert!(unchanged.downcast_ref::<HashJoinExec>().is_some());
        let join = unchanged.downcast_ref::<HashJoinExec>().unwrap();
        assert!(join.left().downcast_ref::<UuidBuildKeyTapExec>().is_none());
    }

    let partitioned_left = rule
        .optimize(
            join_with_scan(JoinType::Left, PartitionMode::Partitioned),
            &config,
        )
        .unwrap();
    let join = partitioned_left.downcast_ref::<HashJoinExec>().unwrap();
    assert!(join.left().downcast_ref::<UuidBuildKeyTapExec>().is_none());

    let wrong_index = rule
        .optimize(left_join_with_wrong_uuid_index(), &config)
        .unwrap();
    let join = wrong_index.downcast_ref::<HashJoinExec>().unwrap();
    assert!(join.left().downcast_ref::<UuidBuildKeyTapExec>().is_none());
}

#[test]
fn partitioned_collect_left_right_enrichment_swaps_and_restores_parent_contract() {
    let rule = PropertyFilterApprovalRule;
    let config = ConfigOptions::default();
    for partitions in [2, 4] {
        let original = right_enrichment_join(property_scan(None, None), partitions, None);
        let original_join = original.downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(*original_join.join_type(), JoinType::Right);
        assert_eq!(*original_join.partition_mode(), PartitionMode::CollectLeft);
        assert!(original_join.contains_projection());
        assert_eq!(
            original.output_partitioning(),
            &datafusion::physical_expr::Partitioning::RoundRobinBatch(partitions)
        );

        let optimized = rule.optimize(Arc::clone(&original), &config).unwrap();
        let restored = optimized.downcast_ref::<RepartitionExec>().unwrap();
        assert_eq!(
            restored.partitioning(),
            &datafusion::physical_expr::Partitioning::RoundRobinBatch(partitions)
        );
        assert_eq!(restored.schema().as_ref(), original.schema().as_ref());
        let join = restored.input().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(*join.join_type(), JoinType::Left);
        assert_eq!(*join.partition_mode(), PartitionMode::CollectLeft);
        assert_eq!(join.schema().as_ref(), original.schema().as_ref());
        assert!(join.contains_projection());
        assert!(join.left().downcast_ref::<UuidBuildKeyTapExec>().is_some());
        let coalesced = join
            .left()
            .downcast_ref::<UuidBuildKeyTapExec>()
            .unwrap()
            .children()[0]
            .downcast_ref::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>(
            )
            .unwrap();
        assert_eq!(
            coalesced.input().output_partitioning(),
            &datafusion::physical_expr::Partitioning::RoundRobinBatch(partitions)
        );
        assert!(join.right().downcast_ref::<PropertyOverlayExec>().is_some());
    }
}

#[test]
fn right_enrichment_rejects_fetch_limits_equality_and_missing_projection() {
    let rule = PropertyFilterApprovalRule;
    let config = ConfigOptions::default();
    let limited_join = right_enrichment_join(property_scan(None, None), 2, Some(1));
    let unchanged = rule.optimize(Arc::clone(&limited_join), &config).unwrap();
    assert!(unchanged.downcast_ref::<HashJoinExec>().is_some());

    for scan in [
        property_scan(Some(1), None),
        property_scan(
            None,
            Some(PropertyEquality {
                column: "ident".into(),
                value: EqualityValue::Int(1),
            }),
        ),
    ] {
        let plan = right_enrichment_join(scan, 2, None);
        let unchanged = rule.optimize(Arc::clone(&plan), &config).unwrap();
        assert!(unchanged.downcast_ref::<HashJoinExec>().is_some());
    }

    let scan: Arc<dyn ExecutionPlan> = Arc::new(property_scan(None, None));
    let frontier: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            empty_uuid_build(),
            datafusion::physical_expr::Partitioning::RoundRobinBatch(2),
        )
        .unwrap(),
    );
    let left_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let right_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let no_projection: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            scan,
            frontier,
            vec![(left_key, right_key)],
            None,
            &JoinType::Right,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let unchanged = rule.optimize(Arc::clone(&no_projection), &config).unwrap();
    assert!(unchanged.downcast_ref::<HashJoinExec>().is_some());
}

#[test]
fn left_enrichment_scan_nominations_exclude_limits_and_property_equalities() {
    assert_eq!(property_scan(None, None).nomination_uuid_column(), Some(0));
    assert_eq!(property_scan(Some(1), None).nomination_uuid_column(), None);
    assert_eq!(
        property_scan(
            None,
            Some(PropertyEquality {
                column: "ident".into(),
                value: EqualityValue::Int(5),
            }),
        )
        .nomination_uuid_column(),
        None
    );
}
