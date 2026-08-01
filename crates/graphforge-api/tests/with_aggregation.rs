//! End-to-end guards for aggregation in WITH.

use arrow::array::{BooleanArray, Int64Array, StringArray};
use graphforge_api::GraphForge;

#[test]
fn nested_with_aggregation_over_empty_match_keeps_group_cardinality() {
    let gf = GraphForge::new(None).expect("in-memory instance");

    let direct = gf
        .execute(
            "MATCH (me:Person)--(you:Person) \
             WITH me.age AS age, me.age + count(you.age) AS agg \
             RETURN age, agg",
        )
        .expect("direct grouping query");
    let projected = gf
        .execute(
            "MATCH (me:Person)--(you:Person) \
             WITH me.age AS age, you \
             WITH age, age + count(you.age) AS agg \
             RETURN age, agg",
        )
        .expect("projected grouping query");

    let direct_rows = direct
        .batches
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    let projected_rows = projected
        .batches
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(direct_rows, 0);
    assert_eq!(projected_rows, 0);
    assert_eq!(
        direct
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["age", "agg"]
    );
}

#[test]
fn nested_with_aggregation_groups_scalars_and_graph_values() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (:Person {age:10}), (:Person {age:10}), (:Person {age:20}), \
         (a)-[:T1]->(:X), (b)-[:T2]->(:X)",
    )
    .expect("create WITH aggregation fixture");

    let scalar = gf
        .execute(
            "MATCH (p:Person) \
             WITH p.age AS age, p.age + count(*) AS agg \
             RETURN age, agg ORDER BY age",
        )
        .expect("nested scalar aggregate");
    let batch = &scalar.batches[0];
    let ages = batch
        .column_by_name("age")
        .expect("age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("age is Int64");
    let aggs = batch
        .column_by_name("agg")
        .expect("agg")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("agg is Int64");
    assert_eq!(ages.values(), &[10, 20]);
    assert_eq!(aggs.values(), &[12, 21]);

    let compound = gf
        .execute(
            "MATCH (p:Person) \
             WITH p.age AS age, p.age + 1 AS next_age, count(*) + 1 AS total \
             RETURN age, next_age, total ORDER BY age",
        )
        .expect("compound grouping key");
    let batch = &compound.batches[0];
    let next_ages = batch
        .column_by_name("next_age")
        .expect("next_age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("next_age is Int64");
    let counts = batch
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("total is Int64");
    assert_eq!(next_ages.values(), &[11, 21]);
    assert_eq!(counts.values(), &[3, 2]);

    let relationships = gf
        .execute(
            "MATCH ()-[r1]->(:X) \
             WITH r1 AS r2, count(*) AS c \
             MATCH ()-[r2]->() \
             RETURN count(*) AS count",
        )
        .expect("relationship grouping key");
    let counts = relationships.batches[0]
        .column_by_name("count")
        .expect("count")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert_eq!(counts.value(0), 2);

    let nodes = gf
        .execute(
            "MATCH (p:Person) \
             WITH p, count(*) + 1 AS c \
             RETURN p.age AS age, c ORDER BY age",
        )
        .expect("node grouping key in compound aggregate");
    let batch = &nodes.batches[0];
    let ages = batch
        .column_by_name("age")
        .expect("age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("age is Int64");
    let counts = batch
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c is Int64");
    assert_eq!(ages.values(), &[10, 10, 20]);
    assert_eq!(counts.values(), &[2, 2, 2]);

    let relationships = gf
        .execute(
            "MATCH ()-[r]->(:X) \
             WITH r, count(*) + 1 AS c \
             RETURN type(r) AS kind, c ORDER BY kind",
        )
        .expect("relationship grouping key in compound aggregate");
    let batch = &relationships.batches[0];
    let kinds = batch
        .column_by_name("kind")
        .expect("kind")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("kind is Utf8");
    assert_eq!(kinds.value(0), "T1");
    assert_eq!(kinds.value(1), "T2");
}

#[test]
fn nested_with_aggregation_rewrites_predicates_and_rejects_nested_aggregates() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {age:10}), (:Person {age:10}), (:Person {age:20})")
        .expect("create predicate fixture");

    let result = gf
        .execute(
            "MATCH (p:Person) \
             WITH p.age AS age, \
                  CASE WHEN p.age > 10 THEN count(*) ELSE 0 END AS total \
             RETURN age, total ORDER BY age",
        )
        .expect("CASE grouping reference is rebound");
    let batch = &result.batches[0];
    let totals = batch
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("total is Int64");
    assert_eq!(totals.values(), &[0, 1]);

    let quantified = gf
        .execute(
            "WITH 15 AS threshold \
             MATCH (p:Person) \
             WITH threshold, any(x IN collect(p.age) WHERE x > threshold) AS result \
             RETURN result",
        )
        .expect("quantifier-local variable stays out of grouping references");
    let result = quantified.batches[0]
        .column_by_name("result")
        .expect("result")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("result is Boolean");
    assert!(result.value(0));

    let error = gf
        .execute("MATCH (p:Person) WITH count(*) + sum(count(*)) AS total RETURN total")
        .expect_err("aggregate nesting must be rejected recursively");
    assert!(
        error
            .to_string()
            .contains("an aggregate function may not contain another aggregate function")
    );

    gf.execute("CREATE (:Source)-[:LINK]->(:Target)")
        .expect("create pattern fixture");
    gf.execute(
        "MATCH (p:Source) \
         WITH [(p)-->(m) | count(m)] AS values \
         RETURN values",
    )
    .expect_err("aggregation in a pattern-comprehension body must remain invalid");
}
