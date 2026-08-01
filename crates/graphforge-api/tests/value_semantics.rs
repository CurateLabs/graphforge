//! End-to-end guards for openCypher value semantics fixed under #962.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::GraphForge;

fn single_batch(query: &str) -> RecordBatch {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = gf.execute(query).expect(query);
    assert_eq!(result.stats.rows_produced, 1, "{query}");
    result.batches[0].clone()
}

fn bool_cell(batch: &RecordBatch, name: &str) -> Option<bool> {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("{name} is not Boolean"));
    if col.is_null(0) {
        None
    } else {
        Some(col.value(0))
    }
}

fn i64_cell(batch: &RecordBatch, name: &str) -> i64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} is not Int64"))
        .value(0)
}

fn string_cell<'a>(batch: &'a RecordBatch, name: &str) -> &'a str {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} is not Utf8"))
        .value(0)
}

fn f64_cell(batch: &RecordBatch, name: &str) -> f64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{name} is not Float64"))
        .value(0)
}

fn nullable_f64_cell(batch: &RecordBatch, name: &str) -> Option<f64> {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{name} is not Float64"));
    if col.is_null(0) {
        None
    } else {
        Some(col.value(0))
    }
}

fn list_i64_cell(batch: &RecordBatch, name: &str) -> Option<Vec<i64>> {
    let list = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{name} is not List"));
    if list.is_null(0) {
        return None;
    }
    let values_array = list.value(0);
    let values = values_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} values are not Int64"));
    Some((0..values.len()).map(|i| values.value(i)).collect())
}

#[test]
fn boolean_operators_use_kleene_truth_tables() {
    let batch = single_batch(
        "RETURN false AND null AS a, null AND false AS b, true OR null AS c, null OR true AS d",
    );
    assert_eq!(bool_cell(&batch, "a"), Some(false));
    assert_eq!(bool_cell(&batch, "b"), Some(false));
    assert_eq!(bool_cell(&batch, "c"), Some(true));
    assert_eq!(bool_cell(&batch, "d"), Some(true));
}

#[test]
fn nan_and_cross_type_comparisons_follow_cypher_semantics() {
    let batch = single_batch(
        "RETURN 0.0 / 0.0 = 0.0 / 0.0 AS eq, \
                0.0 / 0.0 <> 0.0 / 0.0 AS neq, \
                0.0 / 0.0 > 1 AS gt, \
                '1' < 1 AS cross",
    );
    assert_eq!(bool_cell(&batch, "eq"), Some(false));
    assert_eq!(bool_cell(&batch, "neq"), Some(true));
    assert_eq!(bool_cell(&batch, "gt"), Some(false));
    assert_eq!(bool_cell(&batch, "cross"), None);
}

#[test]
fn to_float_rejects_non_finite_strings() {
    let batch = single_batch(
        "RETURN toFloat('NaN') AS nan, \
                toFloat('Infinity') AS inf, \
                toFloat('-Infinity') AS neg_inf, \
                toFloat('1.25') AS finite, \
                toFloat(0.0 / 0.0) AS numeric_nan",
    );
    assert_eq!(nullable_f64_cell(&batch, "nan"), None);
    assert_eq!(nullable_f64_cell(&batch, "inf"), None);
    assert_eq!(nullable_f64_cell(&batch, "neg_inf"), None);
    assert_eq!(nullable_f64_cell(&batch, "finite"), Some(1.25));
    assert!(
        nullable_f64_cell(&batch, "numeric_nan")
            .expect("numeric NaN remains a float")
            .is_nan()
    );
}

#[test]
fn range_and_slice_null_bounds_follow_cypher_semantics() {
    let batch = single_batch(
        "RETURN range(0, -10, -3) AS r, \
                [1, 2, 3][null..] AS s1, \
                [1, 2, 3][..null] AS s2, \
                [1, 2, 3][1..] AS tail, \
                [1, 2, 3][..2] AS head",
    );
    assert_eq!(list_i64_cell(&batch, "r"), Some(vec![0, -3, -6, -9]));
    assert_eq!(list_i64_cell(&batch, "s1"), None);
    assert_eq!(list_i64_cell(&batch, "s2"), None);
    assert_eq!(list_i64_cell(&batch, "tail"), Some(vec![2, 3]));
    assert_eq!(list_i64_cell(&batch, "head"), Some(vec![1, 2]));
}

#[test]
fn homogeneous_list_append_preserves_quantifier_element_type() {
    let batch = single_batch(
        "WITH [1, 3] AS xs \
         UNWIND [2] AS x \
         WITH xs + x AS ys \
         RETURN any(y IN ys WHERE y % 2 = 0) AS has_even",
    );
    assert_eq!(bool_cell(&batch, "has_even"), Some(true));
}

#[test]
fn collect_and_distinct_ignore_null_inputs() {
    let batch = single_batch(
        "UNWIND [null, 1, null, 1] AS x \
         RETURN collect(x) AS c, collect(DISTINCT x) AS d, count(DISTINCT x) AS n",
    );
    assert_eq!(list_i64_cell(&batch, "c"), Some(vec![1, 1]));
    assert_eq!(list_i64_cell(&batch, "d"), Some(vec![1]));
    assert_eq!(i64_cell(&batch, "n"), 1);
}

#[test]
fn sum_and_avg_distinct_preserve_distinct_inputs() {
    let batch = single_batch(
        "UNWIND [null, 1, 1, 2] AS x \
         RETURN sum(x) AS s, sum(DISTINCT x) AS sd, avg(x) AS a, avg(DISTINCT x) AS ad",
    );
    assert_eq!(i64_cell(&batch, "s"), 4);
    assert_eq!(i64_cell(&batch, "sd"), 3);
    assert!((f64_cell(&batch, "a") - (4.0 / 3.0)).abs() < 1e-12);
    assert!((f64_cell(&batch, "ad") - 1.5).abs() < 1e-12);
}

#[test]
fn relationship_variables_compare_by_identity() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:REL]->(:B)")
        .expect("create relationship");
    let result = gf
        .execute("MATCH ()-[r:REL]->(), ()-[s:REL]->() RETURN r = s AS same")
        .expect("relationship equality");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(bool_cell(&result.batches[0], "same"), Some(true));
}

#[test]
fn entity_identity_comparison_is_kind_aware() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:REL]->(:B)")
        .expect("create relationship");
    let result = gf
        .execute("MATCH (n:A)-[r:REL]->() RETURN n = r AS eq, n <> r AS neq")
        .expect("node relationship comparison");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(bool_cell(&result.batches[0], "eq"), Some(false));
    assert_eq!(bool_cell(&result.batches[0], "neq"), Some(true));

    let result = gf
        .execute("MATCH (n:A) OPTIONAL MATCH (n)-[r:NOPE]->() RETURN n = r AS eq, n <> r AS neq")
        .expect("optional null relationship comparison");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(bool_cell(&result.batches[0], "eq"), None);
    assert_eq!(bool_cell(&result.batches[0], "neq"), None);
}

#[test]
fn simple_case_uses_cross_type_equality_per_arm() {
    let batch =
        single_batch("RETURN CASE '0' WHEN 0 THEN 'coerced' ELSE 'different' END AS result");
    assert_eq!(string_cell(&batch, "result"), "different");

    let batch =
        single_batch("RETURN CASE true WHEN 1 THEN 'coerced' ELSE 'different' END AS result");
    assert_eq!(string_cell(&batch, "result"), "different");
}

#[test]
fn in_precedence_truth_tables_decode_tagged_nested_lists() {
    let batch = single_batch(
        "UNWIND [true, false, null] AS a \
         UNWIND [true, false, null] AS b \
         UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c \
         WITH collect((a = b IN c) = (a = (b IN c))) AS eq, \
              collect((a = b IN c) <> ((a = b) IN c)) AS neq \
         RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result",
    );
    assert_eq!(bool_cell(&batch, "result"), Some(true));

    let batch = single_batch(
        "UNWIND [true, false, null] AS a \
         UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS b \
         WITH collect((NOT a IN b) = (NOT (a IN b))) AS eq, \
              collect((NOT a IN b) <> ((NOT a) IN b)) AS neq \
         RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result",
    );
    assert_eq!(bool_cell(&batch, "result"), Some(true));

    let batch = single_batch(
        "UNWIND [true, false, null] AS a \
         UNWIND [true, false, null] AS b \
         UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c \
         WITH collect((a AND b IN c) = (a AND (b IN c))) AS eq, \
              collect((a AND b IN c) <> ((a AND b) IN c)) AS neq \
         RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result",
    );
    assert_eq!(bool_cell(&batch, "result"), Some(true));
}
