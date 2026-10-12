use super::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
use datafusion::optimizer::OptimizerContext;
use graphforge_plan::GraphReadTable;

fn nodes() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "type_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        Field::new("weight", DataType::Int64, true),
    ]))
}

fn properties() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("ident", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// `Filter(predicate)` over a column projection over `nodes <join> props`.
fn plan(table: GraphReadTable, join: JoinType, projected: bool, predicate: Expr) -> LogicalPlan {
    let nodes = LogicalPlanBuilder::scan(
        "n",
        graphforge_plan::GraphReadSource::new(GraphReadTable::Nodes, &nodes(), None),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let props = LogicalPlanBuilder::scan(
        "p",
        graphforge_plan::GraphReadSource::new(table, &properties(), None),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    // The equi-join key sits in `on`, where DataFusion's own optimizer has put
    // it by the time this rule runs.
    let joined = LogicalPlanBuilder::from(nodes)
        .join(props, join, uuid_key("n"), None)
        .unwrap();
    let projected = if projected {
        joined
            .project(vec![
                col("n.node_uuid"),
                col("p.ident").alias_qualified(Some("n"), "ident"),
                col("p.name").alias_qualified(Some("n"), "name"),
            ])
            .unwrap()
    } else {
        joined
    };
    projected.filter(predicate).unwrap().build().unwrap()
}

/// `<alias>.node_uuid = p.node_uuid` as an equi-join key.
fn uuid_key(alias: &str) -> (Vec<Column>, Vec<Column>) {
    (
        vec![Column::from_qualified_name(format!("{alias}.node_uuid"))],
        vec![Column::from_qualified_name("p.node_uuid")],
    )
}

fn rewritten(plan: LogicalPlan) -> (LogicalPlan, bool) {
    let result = StoredEqualityHints
        .rewrite(plan, &OptimizerContext::new())
        .unwrap();
    (result.data, result.transformed)
}

/// The filters the property scan carries.
fn scan_filters(plan: &LogicalPlan) -> Vec<Expr> {
    let mut found = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan.table_name.table() == "p"
        {
            found = scan.filters.clone();
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

#[test]
fn an_aliased_equality_reaches_the_property_scan_and_stays_in_the_plan() {
    let predicate = col("n.ident").eq(lit(7_i64));
    let before = plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        true,
        predicate.clone(),
    );
    let (after, transformed) = rewritten(before);
    assert!(transformed);
    assert_eq!(scan_filters(&after), vec![col("p.ident").eq(lit(7_i64))]);
    let LogicalPlan::Filter(filter) = &after else {
        panic!("the filter stays: {after}");
    };
    assert_eq!(filter.predicate, predicate);
}

#[test]
fn a_literal_on_the_left_and_a_direct_join_are_normalised() {
    let (after, transformed) = rewritten(plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        false,
        lit("ada").eq(col("p.name")),
    ));
    assert!(transformed);
    assert_eq!(scan_filters(&after), vec![col("p.name").eq(lit("ada"))]);
}

#[test]
fn only_equalities_on_stored_columns_of_the_declared_type_are_offered() {
    for predicate in [
        // The route stores `ident` as Int64.
        col("n.ident").eq(lit("7")),
        col("n.ident").eq(lit(7_i32)),
        col("n.ident").gt(lit(7_i64)),
        col("n.ident").eq(col("n.name")),
        col("n.ident").eq(lit(datafusion::common::ScalarValue::Int64(None))),
        // Not a column of the property scan.
        col("n.node_uuid").eq(lit(7_i64)),
    ] {
        let (after, transformed) = rewritten(plan(
            GraphReadTable::Properties("Entity".into()),
            JoinType::Left,
            true,
            predicate.clone(),
        ));
        assert!(!transformed, "{predicate}");
        assert!(scan_filters(&after).is_empty(), "{predicate}");
    }
}

#[test]
fn only_the_nullable_side_of_a_left_join_takes_a_hint() {
    for (join, expected) in [
        (JoinType::Left, true),
        (JoinType::Inner, false),
        (JoinType::Right, false),
        (JoinType::Full, false),
    ] {
        let (_, transformed) = rewritten(plan(
            GraphReadTable::Properties("Entity".into()),
            join,
            true,
            col("n.ident").eq(lit(7_i64)),
        ));
        assert_eq!(transformed, expected, "{join:?}");
    }
}

#[test]
fn key_only_and_edge_routes_take_no_hint() {
    for table in [
        GraphReadTable::PropertyKeys("Entity".into()),
        GraphReadTable::EdgeProperties("LINK".into(), None),
        GraphReadTable::Nodes,
    ] {
        let (_, transformed) = rewritten(plan(
            table.clone(),
            JoinType::Left,
            true,
            col("n.ident").eq(lit(7_i64)),
        ));
        assert!(!transformed, "{table:?}");
    }
}

#[test]
fn a_hint_is_offered_once() {
    let (once, _) = rewritten(plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        true,
        col("n.ident").eq(lit(7_i64)),
    ));
    let (twice, transformed) = rewritten(once.clone());
    assert!(!transformed);
    assert_eq!(scan_filters(&twice), scan_filters(&once));
}

#[test]
fn a_computed_column_is_not_looked_through() {
    let nodes = LogicalPlanBuilder::scan(
        "n",
        graphforge_plan::GraphReadSource::new(GraphReadTable::Nodes, &nodes(), None),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let props = LogicalPlanBuilder::scan(
        "p",
        graphforge_plan::GraphReadSource::new(
            GraphReadTable::Properties("Entity".into()),
            &properties(),
            None,
        ),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let plan = LogicalPlanBuilder::from(nodes)
        .join_on(
            props,
            JoinType::Left,
            vec![col("n.node_uuid").eq(col("p.node_uuid"))],
        )
        .unwrap()
        .project(vec![
            col("n.node_uuid"),
            (col("p.ident") + lit(1_i64)).alias_qualified(Some("n"), "ident"),
        ])
        .unwrap()
        .filter(col("n.ident").eq(lit(8_i64)))
        .unwrap()
        .build()
        .unwrap();
    let (after, transformed) = rewritten(plan);
    assert!(!transformed);
    assert!(scan_filters(&after).is_empty());
}

fn join_types(plan: &LogicalPlan) -> Vec<JoinType> {
    let mut types = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(join) = node {
            types.push(join.join_type);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    types
}

#[test]
fn a_single_strict_property_equality_admits_inner_join_and_retains_filter() {
    for predicate in [col("n.ident").eq(lit(7_i64)), lit(7_i64).eq(col("n.ident"))] {
        let before = plan(
            GraphReadTable::Properties("Entity".into()),
            JoinType::Left,
            true,
            predicate.clone(),
        );
        let (after, transformed) = rewritten(before);
        assert!(transformed);
        assert_eq!(join_types(&after), vec![JoinType::Inner]);
        let LogicalPlan::Filter(filter) = after else {
            panic!("filter missing");
        };
        assert_eq!(filter.predicate, predicate);
    }
}

#[test]
fn conjunctions_and_computed_projections_keep_left_padding() {
    let predicate = col("n.ident")
        .eq(lit(7_i64))
        .and(col("n.name").eq(lit("ada")));
    let (after, _) = rewritten(plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        true,
        predicate,
    ));
    assert_eq!(join_types(&after), vec![JoinType::Left]);
    let before = plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        false,
        col("p.ident").eq(lit(7_i64)),
    );
    let LogicalPlan::Filter(filter) = before else {
        panic!("filter");
    };
    let projected = LogicalPlanBuilder::from(filter.input.as_ref().clone())
        .project(vec![
            col("p.ident"),
            (lit(1_i64) / col("p.ident")).alias("computed"),
        ])
        .unwrap()
        .filter(col("p.ident").eq(lit(7_i64)))
        .unwrap()
        .build()
        .unwrap();
    let (after, _) = rewritten(projected);
    assert_eq!(join_types(&after), vec![JoinType::Left]);
}

#[test]
fn an_existing_scan_hint_still_admits_strict_null_rejection() {
    let before = plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        false,
        col("p.ident").eq(lit(7_i64)),
    );
    let hinted = before
        .transform_up(|node| {
            let LogicalPlan::TableScan(mut scan) = node else {
                return Ok(Transformed::no(node));
            };
            if scan.table_name.table() != "p" {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            }
            scan.filters.push(col("p.ident").eq(lit(7_i64)));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })
        .unwrap()
        .data;
    let (after, changed) = rewritten(hinted);
    assert!(changed);
    assert_eq!(join_types(&after), vec![JoinType::Inner]);
    assert_eq!(scan_filters(&after).len(), 1);
}

#[test]
fn a_nested_right_join_keeps_expression_evaluation_before_null_rejection() {
    let predicate = col("p.ident").eq(lit(7_i64));
    let inner = plan(
        GraphReadTable::Properties("Entity".into()),
        JoinType::Left,
        false,
        predicate.clone(),
    );
    let LogicalPlan::Filter(filter) = inner else {
        panic!("inner filter");
    };
    let outer = LogicalPlanBuilder::scan(
        "a",
        GraphReadSource::new(GraphReadTable::Nodes, &nodes(), None),
        None,
    )
    .unwrap()
    .join_on(
        filter.input.as_ref().clone(),
        JoinType::Left,
        vec![
            col("a.node_uuid").eq(col("n.node_uuid")),
            (lit(1_i64) / col("p.ident")).gt(lit(0_i64)),
        ],
    )
    .unwrap()
    .filter(predicate.clone())
    .unwrap()
    .build()
    .unwrap();
    let (after, transformed) = rewritten(outer);
    assert!(transformed);
    assert_eq!(join_types(&after), vec![JoinType::Left, JoinType::Left]);
    assert_eq!(scan_filters(&after), vec![predicate.clone()]);
    let LogicalPlan::Filter(filter) = after else {
        panic!("outer filter");
    };
    assert_eq!(filter.predicate, predicate);
}

fn node_scan(alias: &str) -> LogicalPlan {
    LogicalPlanBuilder::scan(
        alias,
        GraphReadSource::new(GraphReadTable::Nodes, &nodes(), None),
        None,
    )
    .unwrap()
    .build()
    .unwrap()
}

fn property_scan(alias: &str) -> LogicalPlan {
    LogicalPlanBuilder::scan(
        alias,
        GraphReadSource::new(
            GraphReadTable::Properties("Entity".into()),
            &properties(),
            None,
        ),
        None,
    )
    .unwrap()
    .build()
    .unwrap()
}

/// `Filter(predicate)` over `left <left join> p` on `left_alias.node_uuid`.
fn joined(left: LogicalPlan, left_alias: &str, predicate: Expr) -> LogicalPlan {
    LogicalPlanBuilder::from(left)
        .join(
            property_scan("p"),
            JoinType::Left,
            uuid_key(left_alias),
            None,
        )
        .unwrap()
        .filter(predicate)
        .unwrap()
        .build()
        .unwrap()
}

/// The anchor of the node scan named `alias`, if it has one.
fn node_anchor(plan: &LogicalPlan, alias: &str) -> Option<GraphReadAnchor> {
    let mut found = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan.table_name.table() == alias
            && let Some(source) = scan.source.downcast_ref::<GraphReadSource>()
        {
            found = source.anchor.clone();
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

fn entity_anchor(equality: Expr) -> Option<GraphReadAnchor> {
    Some(GraphReadAnchor {
        route: "Entity".into(),
        equality,
    })
}

fn label_filter(plan: LogicalPlan) -> LogicalPlan {
    use datafusion::functions_nested::expr_fn::array_has;
    LogicalPlanBuilder::from(plan)
        .filter(array_has(col("n.type_ids"), lit(7_u32)))
        .unwrap()
        .build()
        .unwrap()
}

#[test]
fn the_equality_anchors_the_node_scan_that_feeds_the_join() {
    for projected in [true, false] {
        let predicate = if projected {
            col("n.ident").eq(lit(7_i64))
        } else {
            col("p.ident").eq(lit(7_i64))
        };
        let (after, transformed) = rewritten(plan(
            GraphReadTable::Properties("Entity".into()),
            JoinType::Left,
            projected,
            predicate,
        ));
        assert!(transformed);
        assert_eq!(
            node_anchor(&after, "n"),
            entity_anchor(col("p.ident").eq(lit(7_i64))),
            "projected={projected}"
        );
        assert_eq!(node_anchor(&after, "p"), None);
    }
}

#[test]
fn a_conjunction_anchors_by_the_equality_the_property_scan_uses() {
    let (after, _) = rewritten(joined(
        node_scan("n"),
        "n",
        col("p.ident")
            .eq(lit(7_i64))
            .and(col("p.name").eq(lit("ada"))),
    ));
    // The left join stays: only a single strict equality admits an inner join.
    assert_eq!(join_types(&after), vec![JoinType::Left]);
    assert_eq!(
        node_anchor(&after, "n"),
        entity_anchor(col("p.ident").eq(lit(7_i64)))
    );
    assert_eq!(
        scan_filters(&after).first(),
        Some(&col("p.ident").eq(lit(7_i64)))
    );
}

#[test]
fn the_anchor_looks_through_label_filters_and_column_projections() {
    let left = LogicalPlanBuilder::from(label_filter(node_scan("n")))
        .project(vec![
            col("n.node_uuid").alias_qualified(Some("m"), "node_uuid"),
            col("n.type_ids").alias_qualified(Some("m"), "type_ids"),
        ])
        .unwrap()
        .build()
        .unwrap();
    let (after, transformed) = rewritten(joined(left, "m", col("p.ident").eq(lit(7_i64))));
    assert!(transformed);
    assert_eq!(
        node_anchor(&after, "n"),
        entity_anchor(col("p.ident").eq(lit(7_i64)))
    );
}

#[test]
fn the_anchor_stops_at_work_that_could_fail_on_a_row_it_would_omit() {
    let predicate = col("p.ident").eq(lit(7_i64));
    // A filter whose evaluation can fail.
    let failing = LogicalPlanBuilder::from(node_scan("n"))
        .filter((lit(1_i64) / col("n.weight")).gt(lit(0_i64)))
        .unwrap()
        .build()
        .unwrap();
    // A projection that computes.
    let computed = LogicalPlanBuilder::from(node_scan("n"))
        .project(vec![
            col("n.node_uuid"),
            (lit(1_i64) / col("n.weight")).alias("inverse"),
        ])
        .unwrap()
        .build()
        .unwrap();
    for left in [failing, computed] {
        let (after, transformed) = rewritten(joined(left, "n", predicate.clone()));
        // The property scan still takes its hint.
        assert!(transformed);
        assert_eq!(node_anchor(&after, "n"), None);
    }
}

#[test]
fn only_a_node_scan_that_supplies_the_join_key_is_anchored() {
    let predicate = col("p.ident").eq(lit(7_i64));
    // The key comes from a join of two node scans, not from one scan.
    let two = LogicalPlanBuilder::from(node_scan("n"))
        .join(
            node_scan("m"),
            JoinType::Inner,
            (
                vec![Column::from_qualified_name("n.node_uuid")],
                vec![Column::from_qualified_name("m.node_uuid")],
            ),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
    let (after, _) = rewritten(joined(two, "n", predicate.clone()));
    assert_eq!(node_anchor(&after, "n"), None);
    assert_eq!(node_anchor(&after, "m"), None);

    // A property scan on the left is not a node scan.
    let (after, _) = rewritten(joined(property_scan("n"), "n", predicate.clone()));
    assert_eq!(node_anchor(&after, "n"), None);

    // A join on anything but the node identity.
    let on_weight = LogicalPlanBuilder::from(node_scan("n"))
        .join(
            property_scan("p"),
            JoinType::Left,
            (
                vec![Column::from_qualified_name("n.weight")],
                vec![Column::from_qualified_name("p.ident")],
            ),
            None,
        )
        .unwrap()
        .filter(predicate)
        .unwrap()
        .build()
        .unwrap();
    let (after, _) = rewritten(on_weight);
    assert_eq!(node_anchor(&after, "n"), None);
}

#[test]
fn an_anchor_is_set_once_and_never_by_an_unsupported_equality() {
    for predicate in [
        col("p.ident").eq(lit(7_i64)),
        col("p.ident")
            .eq(lit(7_i64))
            .and(col("p.name").eq(lit("ada"))),
    ] {
        let (once, _) = rewritten(joined(node_scan("n"), "n", predicate));
        assert!(node_anchor(&once, "n").is_some());
        let (_, transformed) = rewritten(once);
        assert!(!transformed);
    }
    // The stored type is Int64, so a string is no equality the scan can use.
    let (after, transformed) = rewritten(joined(node_scan("n"), "n", col("p.ident").eq(lit("7"))));
    assert!(!transformed);
    assert_eq!(node_anchor(&after, "n"), None);
}
