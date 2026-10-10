use super::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
use datafusion::optimizer::OptimizerContext;
use graphforge_plan::GraphReadTable;

fn nodes() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]))
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
    let joined = LogicalPlanBuilder::from(nodes)
        .join_on(props, join, vec![col("n.node_uuid").eq(col("p.node_uuid"))])
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
