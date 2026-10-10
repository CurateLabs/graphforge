use super::*;
use crate::SelectedEndpointPropertyFilterApprovalRule;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

fn enrichment(scan: PropertyOverlayExec, partitions: usize) -> Arc<dyn ExecutionPlan> {
    enrichment_with_projection(scan, partitions, vec![0, 1, 2])
}

fn enrichment_with_projection(
    scan: PropertyOverlayExec,
    partitions: usize,
    projection: Vec<usize>,
) -> Arc<dyn ExecutionPlan> {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    // A nullable frontier is legal: its unmatched rows must survive LEFT.
    let frontier = input_with_values(&[Some([1; 16]), Some([1; 16]), None]);
    let left: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(frontier, Partitioning::Hash(vec![key.clone()], partitions))
            .unwrap(),
    );
    let right: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            Arc::new(scan),
            Partitioning::Hash(vec![key.clone()], partitions),
        )
        .unwrap(),
    );
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            vec![(key.clone(), key)],
            None,
            &JoinType::Left,
            Some(projection),
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

fn key_only_property_scan() -> PropertyOverlayExec {
    PropertyOverlayExec::try_new(
        std::path::PathBuf::from("unused"),
        None,
        "_untyped".into(),
        false,
        Arc::new(Schema::new(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )])),
        PropertyScanOptions {
            projection: None,
            limit: None,
            batch_size: 16,
            footer_statistics: false,
            equality: None,
        },
    )
    .unwrap()
}

#[test]
fn partitioned_left_enrichment_keeps_projected_away_hash_keys_as_metadata() {
    for partitions in [1, 2, 4] {
        // Return only the property, dropping both UUID columns. The original
        // join may describe its distribution with an unknown key, but that
        // metadata must never become an executable repartition expression.
        let original = enrichment_with_projection(property_scan(None, None), partitions, vec![2]);
        let Partitioning::Hash(keys, _) = original.output_partitioning() else {
            panic!("expected hash partitioning");
        };
        assert!(
            keys[0]
                .downcast_ref::<datafusion::physical_expr::expressions::UnKnownColumn>()
                .is_some()
        );
        let error = keys[0]
            .evaluate(&arrow::record_batch::RecordBatch::new_empty(
                original.schema(),
            ))
            .unwrap_err();
        assert!(error.to_string().contains("UnKnownColumn::evaluate()"));

        let optimized = PropertyFilterApprovalRule
            .optimize(Arc::clone(&original), &ConfigOptions::default())
            .unwrap();
        assert!(Arc::ptr_eq(&original, &optimized));
    }
}

#[test]
fn selected_endpoint_lifts_only_its_hidden_uuid_for_repartitioning() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        let original = enrichment_with_projection(property_scan(None, None), partitions, vec![2]);
        let optimized = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 0)
            .optimize(original.clone(), &config)
            .unwrap();
        assert!(!Arc::ptr_eq(&original, &optimized));
        assert_eq!(optimized.schema().as_ref(), original.schema().as_ref());
        let projection = optimized
            .downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>()
            .unwrap();
        let repartition = projection
            .input()
            .downcast_ref::<RepartitionExec>()
            .unwrap();
        let Partitioning::Hash(keys, count) = repartition.partitioning() else {
            panic!("selected frontier must be repartitioned by the lifted UUID");
        };
        assert_eq!(*count, partitions);
        assert_eq!(keys.len(), 1);
        assert!(
            keys[0]
                .data_type(repartition.input().schema().as_ref())
                .is_ok_and(|data_type| data_type == DataType::FixedSizeBinary(16))
        );
        SanityCheckPlan::new().optimize(optimized, &config).unwrap();
    }

    // An unrelated equality or a different selected ordinal cannot authorize
    // the hidden-key rewrite.
    let original = enrichment_with_projection(key_only_property_scan(), 2, vec![1]);
    let unchanged = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 1)
        .optimize(original.clone(), &config)
        .unwrap();
    assert!(Arc::ptr_eq(&original, &unchanged));
}

#[test]
fn selected_key_only_scan_supports_existing_collect_left_and_right_shapes() {
    let config = ConfigOptions::default();
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));

    let frontier = input_with_values(&[Some([1; 16]), Some([2; 16])]);
    let left_collect: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            frontier.clone(),
            Arc::new(key_only_property_scan()),
            vec![(key.clone(), key.clone())],
            None,
            &JoinType::Left,
            Some(vec![1]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let left_result = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 0)
        .optimize(left_collect.clone(), &config)
        .unwrap();
    let left_join = left_result.downcast_ref::<HashJoinExec>().unwrap();
    assert_eq!(*left_join.partition_mode(), PartitionMode::CollectLeft);
    assert!(
        left_join
            .left()
            .downcast_ref::<UuidBuildKeyTapExec>()
            .is_some()
    );
    assert!(
        left_join
            .right()
            .downcast_ref::<PropertyOverlayExec>()
            .is_some()
    );
    assert_eq!(left_join.schema().as_ref(), left_collect.schema().as_ref());

    let right_frontier: Arc<dyn ExecutionPlan> =
        Arc::new(RepartitionExec::try_new(frontier, Partitioning::RoundRobinBatch(2)).unwrap());
    let right_plan: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            Arc::new(key_only_property_scan()),
            right_frontier,
            vec![(key.clone(), key)],
            None,
            &JoinType::Right,
            Some(vec![1]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let right_result = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 0)
        .optimize(right_plan.clone(), &config)
        .unwrap();
    let restored = right_result.downcast_ref::<RepartitionExec>().unwrap();
    assert_eq!(restored.schema().as_ref(), right_plan.schema().as_ref());
    let swapped = restored.input().downcast_ref::<HashJoinExec>().unwrap();
    assert_eq!(*swapped.join_type(), JoinType::Left);
    assert!(
        swapped
            .left()
            .downcast_ref::<UuidBuildKeyTapExec>()
            .is_some()
    );
}

#[test]
fn partitioned_left_enrichment_nominates_the_complete_nullable_frontier() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        let original = enrichment(property_scan(None, None), partitions);
        SanityCheckPlan::new()
            .optimize(original.clone(), &config)
            .unwrap();
        let optimized = PropertyFilterApprovalRule
            .optimize(original.clone(), &config)
            .unwrap();
        let restored = optimized.downcast_ref::<RepartitionExec>().unwrap();
        assert_eq!(restored.partitioning(), original.output_partitioning());
        assert_eq!(restored.schema().as_ref(), original.schema().as_ref());
        let join = restored.input().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(*join.join_type(), JoinType::Left);
        assert_eq!(*join.partition_mode(), PartitionMode::CollectLeft);
        let tap = join.left().downcast_ref::<UuidBuildKeyTapExec>().unwrap();
        let children = tap.children();
        let coalesced = children[0]
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let original_join = original.downcast_ref::<HashJoinExec>().unwrap();
        let original_exchange = original_join
            .left()
            .downcast_ref::<RepartitionExec>()
            .unwrap();
        assert!(Arc::ptr_eq(coalesced.input(), original_exchange.input()));
        assert!(join.right().downcast_ref::<PropertyOverlayExec>().is_some());
        SanityCheckPlan::new().optimize(optimized, &config).unwrap();
    }
}

#[test]
fn partitioned_left_enrichment_excludes_scan_limits_and_equalities() {
    let config = ConfigOptions::default();
    for scan in [
        property_scan(Some(1), None),
        property_scan(None, None).with_uuid_nomination(UuidBuildKeyNomination::new()),
        property_scan(
            None,
            Some(PropertyEquality {
                column: "ident".into(),
                value: EqualityValue::Int(1),
            }),
        ),
    ] {
        let original = enrichment(scan, 4);
        let unchanged = PropertyFilterApprovalRule
            .optimize(original.clone(), &config)
            .unwrap();
        assert!(Arc::ptr_eq(&original, &unchanged));
    }
}

#[test]
fn partitioned_left_enrichment_is_idempotent_and_rejects_other_exchanges() {
    let config = ConfigOptions::default();
    let original = enrichment(property_scan(None, None), 4);
    let optimized = PropertyFilterApprovalRule
        .optimize(original.clone(), &config)
        .unwrap();
    let repeated = PropertyFilterApprovalRule
        .optimize(optimized.clone(), &config)
        .unwrap();
    assert!(Arc::ptr_eq(&optimized, &repeated));

    let original_join = original.downcast_ref::<HashJoinExec>().unwrap();
    let exchange = original_join
        .right()
        .downcast_ref::<RepartitionExec>()
        .unwrap();
    for partitioning in [
        Partitioning::RoundRobinBatch(4),
        Partitioning::Hash(vec![Arc::new(Column::new("ident", 1))], 4),
    ] {
        let wrong: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(exchange.input().clone(), partitioning).unwrap());
        let plan = original_join
            .builder()
            .with_new_children(vec![original_join.left().clone(), wrong])
            .unwrap()
            .reset_state()
            .recompute_properties()
            .build_exec()
            .unwrap();
        let unchanged = PropertyFilterApprovalRule
            .optimize(plan.clone(), &config)
            .unwrap();
        assert!(Arc::ptr_eq(&plan, &unchanged));
    }
}

#[test]
fn identity_only_enrichment_keeps_its_partitioned_join_without_a_uuid_set() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        let scan = PropertyOverlayExec::try_new(
            std::path::PathBuf::from("unused"),
            None,
            "_untyped".into(),
            false,
            Arc::new(Schema::new(vec![Field::new(
                "node_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )])),
            PropertyScanOptions {
                projection: None,
                limit: None,
                batch_size: 16,
                footer_statistics: false,
                equality: None,
            },
        )
        .unwrap();
        assert_eq!(scan.nomination_uuid_column(), None);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
        let frontier = input_with_values(&[Some([1; 16]), Some([1; 16]), None]);
        let left = Arc::new(
            RepartitionExec::try_new(frontier, Partitioning::Hash(vec![key.clone()], partitions))
                .unwrap(),
        );
        let right = Arc::new(
            RepartitionExec::try_new(
                Arc::new(scan),
                Partitioning::Hash(vec![key.clone()], partitions),
            )
            .unwrap(),
        );
        let original: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(key.clone(), key)],
                None,
                &JoinType::Left,
                Some(vec![0]),
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let optimized = PropertyFilterApprovalRule
            .optimize(original.clone(), &config)
            .unwrap();
        assert!(Arc::ptr_eq(&original, &optimized));
        SanityCheckPlan::new().optimize(optimized, &config).unwrap();
    }
}
