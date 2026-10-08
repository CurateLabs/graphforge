//! Aggregates over no input rows, and over only null values, follow openCypher
//! (#1887 D13): `count` is 0, `sum` is 0, `collect` is `[]`, and `avg`, `min`,
//! `max` and the percentiles are null.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

/// The single result row as `(column, rendered value or None for null)`.
fn row(gf: &GraphForge, query: &str) -> Vec<(String, Option<String>)> {
    let result = gf
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let rows: usize = result
        .batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 1, "{query}: a global aggregate returns one row");
    let batch = result
        .batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("the row");
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            (
                field.name().clone(),
                (!array.is_null(0)).then(|| array_value_to_string(array, 0).expect("render")),
            )
        })
        .collect()
}

fn graph() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:P {id: 1, x: 1.5}), (:P {id: 2, x: 2.5})")
        .expect("create nodes");
    gf
}

const EVERY_AGGREGATE: &str = "RETURN count(n) AS count, count(*) AS count_star, \
     count(DISTINCT n.id) AS count_distinct, sum(n.id) AS sum_int, \
     sum(n.x) AS sum_float, sum(DISTINCT n.id) AS sum_distinct, avg(n.id) AS avg, \
     avg(DISTINCT n.id) AS avg_distinct, min(n.id) AS min, max(n.id) AS max, \
     collect(n.id) AS collect, collect(DISTINCT n.id) AS collect_distinct, \
     percentileDisc(n.id, 0.5) AS percentile_disc, percentileCont(n.id, 0.5) AS percentile_cont";

fn expected() -> Vec<(String, Option<String>)> {
    [
        ("count", Some("0")),
        ("count_star", Some("0")),
        ("count_distinct", Some("0")),
        ("sum_int", Some("0")),
        ("sum_float", Some("0.0")),
        ("sum_distinct", Some("0")),
        ("avg", None),
        ("avg_distinct", None),
        ("min", None),
        ("max", None),
        ("collect", Some("[]")),
        ("collect_distinct", Some("[]")),
        ("percentile_disc", None),
        ("percentile_cont", None),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.map(str::to_owned)))
    .collect()
}

#[test]
fn every_aggregate_over_an_empty_match() {
    let gf = graph();
    assert_eq!(
        row(
            &gf,
            &format!("MATCH (n:P) WHERE n.id > 99 {EVERY_AGGREGATE}")
        ),
        expected()
    );
}

#[test]
fn every_aggregate_over_an_empty_with_pipeline() {
    let gf = graph();
    assert_eq!(
        row(
            &gf,
            &format!("MATCH (n:P) WITH n WHERE n.id > 99 {EVERY_AGGREGATE}")
        ),
        expected()
    );
}

#[test]
fn sum_of_only_null_values_is_zero() {
    let gf = graph();
    assert_eq!(
        row(&gf, "MATCH (n:P) RETURN sum(n.missing) AS s"),
        vec![("s".to_owned(), Some("0".to_owned()))]
    );
    assert_eq!(
        row(&gf, "MATCH (n:P) RETURN sum(null) AS s"),
        vec![("s".to_owned(), Some("0".to_owned()))]
    );
    assert_eq!(
        row(&gf, "UNWIND [null, null] AS v RETURN sum(v) AS s"),
        vec![("s".to_owned(), Some("0".to_owned()))]
    );
}

#[test]
fn empty_totals_compose_into_expressions() {
    let gf = graph();
    assert_eq!(
        row(
            &gf,
            "MATCH (n:P) WHERE n.id > 99 RETURN sum(n.id) + 1 AS total, \
             sum(n.id) = 0 AS is_zero"
        ),
        vec![
            ("total".to_owned(), Some("1".to_owned())),
            ("is_zero".to_owned(), Some("true".to_owned())),
        ]
    );
}

#[test]
fn grouped_aggregates_over_empty_input_return_no_rows() {
    let gf = graph();
    let result = gf
        .execute("MATCH (n:P) WHERE n.id > 99 RETURN n.id AS id, sum(n.id) AS s")
        .expect("grouped aggregate");
    let rows: usize = result
        .batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 0);
}

#[test]
fn non_empty_sums_are_unchanged() {
    let gf = graph();
    assert_eq!(
        row(&gf, "MATCH (n:P) RETURN sum(n.id) AS s, sum(n.x) AS f"),
        vec![
            ("s".to_owned(), Some("3".to_owned())),
            ("f".to_owned(), Some("4.0".to_owned())),
        ]
    );
}
