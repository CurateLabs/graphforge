use super::*;
use crate::SelectedEndpointPropertyFilterApprovalRule;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;

fn enrichment(scan: PropertyOverlayExec, partitions: usize) -> Arc<dyn ExecutionPlan> {
    enrichment_with_projection(scan, partitions, vec![0, 1, 2])
}

fn enrichment_with_projection(
    scan: PropertyOverlayExec,
    partitions: usize,
    projection: Vec<usize>,
) -> Arc<dyn ExecutionPlan> {
    enrichment_from_frontier(
        input_with_values(&[Some([1; 16]), Some([1; 16]), None]),
        scan,
        partitions,
        projection,
    )
}

fn anchored_enrichment(scan: PropertyOverlayExec, partitions: usize) -> Arc<dyn ExecutionPlan> {
    anchored_enrichment_with_projection(scan, partitions, vec![0, 1, 2, 3])
}

fn anchored_enrichment_with_projection(
    scan: PropertyOverlayExec,
    partitions: usize,
    projection: Vec<usize>,
) -> Arc<dyn ExecutionPlan> {
    enrichment_from_frontier(
        super::equality_seed_uuid_frontier(),
        scan,
        partitions,
        projection,
    )
}

fn enrichment_from_frontier(
    frontier: Arc<dyn ExecutionPlan>,
    scan: PropertyOverlayExec,
    partitions: usize,
    projection: Vec<usize>,
) -> Arc<dyn ExecutionPlan> {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    // A nullable frontier is legal: its unmatched rows must survive LEFT.
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

/// Whether `optimized` is `original` with its preserved side tapped: the join
/// keeps its partitioned shape, the preserved side is coalesced, tapped on its
/// key and re-hashed into the original partitions, and the scan is pruned by
/// the tap's nomination.
fn assert_tapped_partitioned_join(
    original: &Arc<dyn ExecutionPlan>,
    optimized: &Arc<dyn ExecutionPlan>,
) {
    assert_tapped_partitioned_join_over(original, optimized, true);
}

/// `elides_exchange` says whether the original preserved-side exchange was
/// dropped under the tap. An exchange above a `fetch` must stay: removing it
/// would change which rows the limit sees.
fn assert_tapped_partitioned_join_over(
    original: &Arc<dyn ExecutionPlan>,
    optimized: &Arc<dyn ExecutionPlan>,
    elides_exchange: bool,
) {
    assert!(!Arc::ptr_eq(original, optimized));
    assert_eq!(optimized.schema().as_ref(), original.schema().as_ref());
    let original_join = original.downcast_ref::<HashJoinExec>().unwrap();
    let join = optimized.downcast_ref::<HashJoinExec>().unwrap();
    assert_eq!(*join.join_type(), JoinType::Left);
    assert_eq!(*join.partition_mode(), PartitionMode::Partitioned);
    assert_eq!(
        optimized.output_partitioning().partition_count(),
        original.output_partitioning().partition_count()
    );

    // The preserved rows stay streaming: the original exchange's input is
    // coalesced into the tap and hashed again, never concatenated.
    let original_build = original_join
        .left()
        .downcast_ref::<RepartitionExec>()
        .unwrap();
    let build = join.left().downcast_ref::<RepartitionExec>().unwrap();
    assert_eq!(build.partitioning(), original_build.partitioning());
    let tap = build.input().downcast_ref::<UuidBuildKeyTapExec>().unwrap();
    let children = tap.children();
    let coalesced = children[0]
        .downcast_ref::<CoalescePartitionsExec>()
        .unwrap();
    let expected: &Arc<dyn ExecutionPlan> = if elides_exchange {
        original_build.input()
    } else {
        original_join.left()
    };
    assert!(Arc::ptr_eq(coalesced.input(), expected));

    let original_probe = original_join
        .right()
        .downcast_ref::<RepartitionExec>()
        .unwrap();
    let probe = join.right().downcast_ref::<RepartitionExec>().unwrap();
    assert_eq!(probe.partitioning(), original_probe.partitioning());
    let scan = probe.input().downcast_ref::<PropertyOverlayExec>().unwrap();
    assert_eq!(scan.fresh_nomination_uuid_column(), None);
    assert_eq!(scan.fresh_selected_endpoint_uuid_column(), None);

    SanityCheckPlan::new()
        .optimize(Arc::clone(optimized), &ConfigOptions::default())
        .unwrap();
}

#[test]
fn partitioned_left_enrichment_nominates_a_frontier_with_no_bound() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        // Nothing anchors or limits the frontier: it is whatever reaches the
        // join. The tap collects its keys instead of concatenating its rows,
        // so the width of those rows no longer decides whether to nominate.
        let original = enrichment(property_scan(None, None), partitions);
        SanityCheckPlan::new()
            .optimize(original.clone(), &config)
            .unwrap();
        let optimized = PropertyFilterApprovalRule
            .optimize(original.clone(), &config)
            .unwrap();
        assert_tapped_partitioned_join(&original, &optimized);

        // Optimizing the result again changes nothing: its scan is already
        // pruned.
        let repeated = PropertyFilterApprovalRule
            .optimize(Arc::clone(&optimized), &config)
            .unwrap();
        assert!(Arc::ptr_eq(&optimized, &repeated));
    }
}

#[test]
fn partitioned_left_enrichment_nominates_an_anchored_frontier() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        let original = anchored_enrichment(property_scan(None, None), partitions);
        SanityCheckPlan::new()
            .optimize(original.clone(), &config)
            .unwrap();
        let optimized = PropertyFilterApprovalRule
            .optimize(original.clone(), &config)
            .unwrap();
        assert_tapped_partitioned_join(&original, &optimized);
    }
}

#[test]
fn partitioned_left_enrichment_nominates_a_hidden_key_of_a_fetch_bounded_frontier() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        // `ORDER BY ... LIMIT` bounds the frontier. Only the property column
        // of the scan survives the projection, so neither side's UUID is
        // visible above the join.
        let frontier: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            input_with_values(&[Some([1; 16]), Some([2; 16]), None]),
            0,
            Some(10),
        ));
        let original =
            enrichment_from_frontier(frontier, property_scan(None, None), partitions, vec![2]);
        let optimized = PropertyFilterApprovalRule
            .optimize(Arc::clone(&original), &config)
            .unwrap();
        assert_tapped_partitioned_join_over(&original, &optimized, false);
    }
}

#[test]
fn partitioned_left_enrichment_nominates_a_frontier_whose_key_the_projection_hides() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        // Drop the preserved UUID from a valid equality-seeded frontier. The
        // original join describes its distribution with an unknown key, which
        // equals nothing, so the rebuilt join is compared by that key's name:
        // the nominated join must hash its output into the same partitions.
        let original = anchored_enrichment_with_projection(
            property_scan(None, None),
            partitions,
            vec![1, 2, 3],
        );
        let original_join = original.downcast_ref::<HashJoinExec>().unwrap();
        let Partitioning::Hash(keys, _) = original.output_partitioning() else {
            panic!("expected hash partitioning");
        };
        assert!(
            keys[0]
                .downcast_ref::<datafusion::physical_expr::expressions::UnKnownColumn>()
                .is_some()
        );
        assert!(crate::property_filter_approval::is_filtered_seed_uuid_key(
            original_join.left().as_ref(),
            0,
        ));

        // The ordinary rule needs no selected-endpoint proof: nomination only
        // prunes the scan, and the preserved frontier decides every row.
        let optimized = PropertyFilterApprovalRule
            .optimize(Arc::clone(&original), &config)
            .unwrap();
        assert_tapped_partitioned_join(&original, &optimized);
    }
}

#[test]
fn partitioned_left_enrichment_keeps_a_hidden_key_when_only_identity_is_read() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        // A key-only route join authenticates the route for `count(*)`; only
        // a selected-endpoint proof may prune it.
        let original = enrichment_with_projection(key_only_property_scan(), partitions, vec![1]);
        let unchanged = PropertyFilterApprovalRule
            .optimize(Arc::clone(&original), &config)
            .unwrap();
        assert!(Arc::ptr_eq(&original, &unchanged));
    }
}

#[test]
fn selected_endpoint_nominates_a_hidden_uuid_without_changing_the_join() {
    let config = ConfigOptions::default();
    for partitions in [1, 2, 4] {
        let original = enrichment_with_projection(property_scan(None, None), partitions, vec![2]);
        let optimized = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 0)
            .optimize(original.clone(), &config)
            .unwrap();
        assert_tapped_partitioned_join(&original, &optimized);

        // A key-only scan is nominated only for the selected endpoint.
        let key_only = enrichment_with_projection(key_only_property_scan(), partitions, vec![1]);
        let selected = SelectedEndpointPropertyFilterApprovalRule::new(|_, index| index == 0)
            .optimize(key_only.clone(), &config)
            .unwrap();
        assert_tapped_partitioned_join(&key_only, &selected);
    }

    // An unrelated equality or a different selected ordinal cannot authorize
    // the rewrite of a key-only route.
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
    assert_eq!(restored.partitioning(), &Partitioning::RoundRobinBatch(2));
    assert_eq!(restored.schema().as_ref(), right_plan.schema().as_ref());
    // The swapped join's per-partition batches merge into one stream before the
    // restored exchange fans them out again.
    let merged = restored.input().downcast_ref::<RepartitionExec>().unwrap();
    assert_eq!(merged.partitioning(), &Partitioning::RoundRobinBatch(1));
    let swapped = merged.input().downcast_ref::<HashJoinExec>().unwrap();
    assert_eq!(*swapped.join_type(), JoinType::Left);
    assert_eq!(*swapped.partition_mode(), PartitionMode::Partitioned);
    let build = swapped.left().downcast_ref::<RepartitionExec>().unwrap();
    assert!(
        build
            .input()
            .downcast_ref::<UuidBuildKeyTapExec>()
            .is_some()
    );
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
    let original = anchored_enrichment(property_scan(None, None), 4);
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
