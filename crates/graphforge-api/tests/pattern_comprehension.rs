//! End-to-end guards for correlated pattern-comprehension collection.

use arrow::array::{Array, Int64Array, ListArray, StringArray};
use graphforge_api::GraphForge;

#[test]
fn pattern_comprehension_collects_matches_nulls_and_empty_lists() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:N {name:'A'}), (b:N {name:'B', value:'set'}), \
         (c:N {name:'C'}), (d:N {name:'D'}), \
         (a)-[:T]->(b), (a)-[:T]->(c)",
    )
    .expect("create pattern-comprehension fixture");

    let result = gf
        .execute(
            "MATCH (n:N) \
             RETURN n.name AS name, [(n)-[:T]->(m) | m.value] AS values \
             ORDER BY name",
        )
        .expect("execute pattern comprehension");
    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 4);
    let values = batch
        .column_by_name("values")
        .expect("values")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("values is a list");

    let first = values.value(0);
    let first = first
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("values contain strings");
    assert_eq!(first.len(), 2);
    assert_eq!(
        first.null_count(),
        1,
        "projected null must remain in the list"
    );
    assert_eq!(
        (0..first.len())
            .filter(|&i| !first.is_null(i) && first.value(i) == "set")
            .count(),
        1,
        "the non-null projected value must be retained"
    );
    assert_eq!(values.value(1).len(), 0);
    assert_eq!(values.value(2).len(), 0);
    assert_eq!(values.value(3).len(), 0);
}

#[test]
fn pattern_comprehension_preserves_duplicate_outer_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:N {name:'A'}), (b:N), (c:N), (d:N), (e:N), \
         (a)-[:DRIVES]->(b), (a)-[:DRIVES]->(c), \
         (a)-[:T]->(d), (a)-[:T]->(e)",
    )
    .expect("create duplicate-outer fixture");

    let result = gf
        .execute(
            "MATCH (n:N)-[:DRIVES]->() \
             RETURN size([(n)-[:T]->() | 1]) AS child_count",
        )
        .expect("execute duplicate-outer pattern comprehension");
    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 2, "outer duplicates must not collapse");
    let counts = batch
        .column_by_name("child_count")
        .expect("child_count")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("child_count is Int64");
    assert_eq!(counts.values(), &[2, 2]);
}

#[test]
fn list_element_pattern_comprehension_is_lexical_and_preserves_nulls() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:X {name:'A'}), (am:M), (ay:Y), \
         (b:X {name:'B'}), (bm:M), \
         (a)-[:T]->(am), (am)-[:T]->(ay), \
         (b)-[:T]->(bm)",
    )
    .expect("create list-element pattern fixture");

    let result = gf
        .execute(
            "MATCH p = (x:X)-[:T]->() \
             RETURN x.name AS before, \
                    [x IN nodes(p) | size([(x)-[:T]->(:Y) | 1])] AS counts, \
                    x.name AS after \
             ORDER BY before",
        )
        .expect("execute graph-valued list comprehension");
    let batch = &result.batches[0];
    let before = batch
        .column_by_name("before")
        .expect("before")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("before is Utf8");
    let after = batch
        .column_by_name("after")
        .expect("after")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("after is Utf8");
    assert_eq!(before.value(0), "A");
    assert_eq!(before.value(1), "B");
    assert_eq!(after.value(0), "A", "loop x must not leak over outer x");
    assert_eq!(after.value(1), "B", "loop x must not leak over outer x");

    let counts = batch
        .column_by_name("counts")
        .expect("counts")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("counts is a List");
    let a = counts.value(0);
    let a = a.as_any().downcast_ref::<Int64Array>().expect("A counts");
    assert_eq!(a.values(), &[0, 1]);
    let b = counts.value(1);
    let b = b.as_any().downcast_ref::<Int64Array>().expect("B counts");
    assert_eq!(b.values(), &[0, 0], "no nested matches become empty lists");

    let null_result = gf
        .execute(
            "MATCH p = (n:X)-[:T]->() \
             RETURN n.name AS name, \
                    [x IN CASE WHEN n.name = 'B' THEN null ELSE nodes(p) END \
                       | size([(x)-[:T]->(:Y) | 1])] AS counts \
             ORDER BY name",
        )
        .expect("execute null graph-valued list comprehension");
    let null_counts = null_result.batches[0]
        .column_by_name("counts")
        .expect("counts")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("counts is a List");
    assert!(!null_counts.is_null(0));
    assert!(null_counts.is_null(1), "null source list must stay null");
}
