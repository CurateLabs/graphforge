use super::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{LogicalPlanBuilder, col};
use datafusion::optimizer::OptimizerContext;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("ident", DataType::Int64, true),
    ]))
}

fn scan(alias: &str, table: GraphReadTable) -> LogicalPlan {
    LogicalPlanBuilder::scan(alias, GraphReadSource::new(table, &schema(), None), None)
        .unwrap()
        .build()
        .unwrap()
}

fn plan(table: GraphReadTable, join: JoinType, on: (&str, &str), read: Vec<Expr>) -> LogicalPlan {
    LogicalPlanBuilder::from(scan("n", GraphReadTable::Nodes))
        .join_on(scan("p", table), join, vec![col(on.0).eq(col(on.1))])
        .unwrap()
        .project(read)
        .unwrap()
        .build()
        .unwrap()
}

fn rewritten(plan: LogicalPlan) -> (LogicalPlan, bool) {
    let result = UnusedRouteJoin
        .rewrite(plan, &OptimizerContext::new())
        .unwrap();
    (result.data, result.transformed)
}

fn keys() -> (&'static str, &'static str) {
    ("n.node_uuid", "p.node_uuid")
}

#[test]
fn a_key_join_nothing_reads_is_removed_and_the_projection_keeps_its_columns() {
    for table in [
        GraphReadTable::Properties("R".into()),
        GraphReadTable::PropertyKeys("R".into()),
    ] {
        let (after, transformed) =
            rewritten(plan(table, JoinType::Left, keys(), vec![col("n.ident")]));
        assert!(transformed);
        let LogicalPlan::Projection(projection) = &after else {
            panic!("{after}");
        };
        assert!(matches!(
            projection.input.as_ref(),
            LogicalPlan::TableScan(_)
        ));
        assert_eq!(projection.schema.fields().len(), 1);
    }
    let (_, transformed) = rewritten(plan(
        GraphReadTable::EdgePropertyKeys("R".into()),
        JoinType::Left,
        ("n.edge_uuid", "p.edge_uuid"),
        vec![col("n.node_uuid")],
    ));
    assert!(transformed);
}

#[test]
fn a_join_whose_route_column_is_read_stays() {
    let (_, transformed) = rewritten(plan(
        GraphReadTable::Properties("R".into()),
        JoinType::Left,
        keys(),
        vec![col("n.node_uuid"), col("p.ident")],
    ));
    assert!(!transformed);
}

#[test]
fn only_a_left_join_on_the_routes_key_is_removed() {
    for join in [JoinType::Inner, JoinType::Right, JoinType::Full] {
        let (_, transformed) = rewritten(plan(
            GraphReadTable::Properties("R".into()),
            join,
            keys(),
            vec![col("n.node_uuid")],
        ));
        assert!(!transformed, "{join:?}");
    }
    // Joined on a value, a route row can match many left rows' keys or none.
    let (_, transformed) = rewritten(plan(
        GraphReadTable::Properties("R".into()),
        JoinType::Left,
        ("n.node_uuid", "p.ident"),
        vec![col("n.node_uuid")],
    ));
    assert!(!transformed);
}

#[test]
fn a_join_onto_anything_but_a_property_route_stays() {
    let (_, transformed) = rewritten(plan(
        GraphReadTable::Nodes,
        JoinType::Left,
        keys(),
        vec![col("n.node_uuid")],
    ));
    assert!(!transformed);
}
