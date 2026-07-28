//! End-to-end guards for simple correlated existential subqueries.

use arrow::array::{Int64Array, StringArray};
use gf_api::GraphForge;

#[test]
fn simple_existential_subquery_filters_without_multiplying_outer_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:N {name:'A'}), (b:N {name:'B'}), \
         (x:N {name:'X'}), (y:N {name:'Y'}), (z:N {name:'Z'}), \
         (a)-[:REL]->(x), (a)-[:REL]->(y), (a)-[:REL]->(z), \
         (b)-[:OTHER]->(x)",
    )
    .expect("create existential fixture");

    let result = gf
        .execute(
            "MATCH (n:N) \
             WHERE exists { (n)-[r]->(m) WHERE type(r) = 'REL' } \
             RETURN n.name AS name",
        )
        .expect("execute simple existential subquery");
    let batch = &result.batches[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "three child matches must yield one outer row"
    );
    let names = batch
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(names.value(0), "A");
}

#[test]
fn existential_child_variables_do_not_leak() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let error = gf
        .execute("MATCH (n) WHERE exists { (n)-->(m) } RETURN m")
        .expect_err("child-local m must be unavailable after the subquery");
    assert!(
        error
            .to_string()
            .contains("variable `m` used before it was introduced")
    );
}

#[test]
fn full_existential_subquery_runs_read_pipeline_and_aggregation() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:N {name:'A'}), (b:N {name:'B'}), \
         (x:N {name:'X', prop:10}), (y:N {name:'Y'}), (z:N {name:'Z'}), \
         (a)-[:REL]->(x), (a)-[:REL]->(y), (a)-[:REL]->(z), \
         (b)-[:REL]->(x)",
    )
    .expect("create full existential fixture");

    let simple = gf
        .execute(
            "MATCH (n:N) \
             WHERE exists { MATCH (n)-->() RETURN true } \
             RETURN n.name AS name ORDER BY name",
        )
        .expect("full existential read pipeline");
    assert_eq!(simple.stats.rows_produced, 2);
    let names = simple.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(names.value(0), "A");
    assert_eq!(names.value(1), "B");

    let expression_correlated = gf
        .execute(
            "MATCH (n:N) WHERE exists { \
               MATCH (m:N {name:'X'}) \
               WHERE m.prop = n.prop \
               RETURN true \
             } \
             RETURN n.name AS name",
        )
        .expect("outer variables used only in child expressions must correlate");
    assert_eq!(expression_correlated.stats.rows_produced, 1);
    let names = expression_correlated.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(names.value(0), "X");

    let aggregate = gf
        .execute(
            "MATCH (n:N) WHERE exists { \
               MATCH (n)-->(m) \
               WITH n, count(*) AS connections \
               WHERE connections = 3 \
               RETURN true \
             } \
             RETURN n.name AS name",
        )
        .expect("full existential aggregate pipeline");
    assert_eq!(aggregate.stats.rows_produced, 1);
    let names = aggregate.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(names.value(0), "A");

    let error = gf
        .execute(
            "MATCH (n:N) WHERE exists { \
               MATCH (n)-->(m) \
               SET m.prop = 99 \
               RETURN true \
             } \
             RETURN n",
        )
        .expect_err("writes in existential subqueries must fail at compile time");
    assert!(error.to_string().contains("only read clauses"));
    let unchanged = gf
        .execute("MATCH (n:N {name:'X'}) RETURN n.prop AS prop")
        .expect("read unchanged property");
    let props = unchanged.batches[0]
        .column_by_name("prop")
        .expect("prop")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("prop is Int64");
    assert_eq!(props.value(0), 10);
}

#[test]
fn full_existential_child_variables_do_not_leak() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let error = gf
        .execute("MATCH (n) WHERE exists { MATCH (n)-->(m) RETURN true } RETURN m")
        .expect_err("full-subquery child-local m must not leak");
    assert!(
        error
            .to_string()
            .contains("variable `m` used before it was introduced")
    );
}

#[test]
fn nested_existential_subqueries_compose_without_leaking_or_multiplying_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:A {prop:1})-[:R]->(b:B {prop:1}), \
                (a)-[:R]->(:C {prop:2}), \
                (a)-[:R]->(:D {prop:3})",
    )
    .expect("create nested existential fixture");

    for query in [
        "MATCH (n) WHERE exists { \
           MATCH (m) WHERE exists { \
             (n)-[]->(m) WHERE n.prop = m.prop \
           } \
           RETURN true \
         } \
         RETURN n.prop AS prop",
        "MATCH (n) WHERE exists { \
           MATCH (m) WHERE exists { \
             MATCH (l)<-[:R]-(n)-[:R]->(m) RETURN true \
           } \
           RETURN true \
         } \
         RETURN n.prop AS prop",
        "MATCH (n) WHERE exists { \
           MATCH (m) WHERE exists { \
             MATCH (l) WHERE (l)<-[:R]-(n)-[:R]->(m) RETURN true \
           } \
           RETURN true \
         } \
         RETURN n.prop AS prop",
    ] {
        let result = gf.execute(query).expect("nested existential query");
        assert_eq!(result.stats.rows_produced, 1);
        let props = result.batches[0]
            .column_by_name("prop")
            .expect("prop")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("prop is Int64");
        assert_eq!(props.value(0), 1);
    }

    let error = gf
        .execute(
            "MATCH (n) WHERE exists { \
               MATCH (m) WHERE exists { MATCH (l)<-[:R]-(n) RETURN true } \
               RETURN true \
             } \
             RETURN l",
        )
        .expect_err("nested child-local variables must not leak");
    assert!(
        error
            .to_string()
            .contains("variable `l` used before it was introduced")
    );
}
