//! End-to-end guards for list validation and list-comprehension residuals.

use std::collections::HashMap;

use arrow::array::{Array, Int64Array, ListArray, StringArray};
use gf_api::{GraphForge, IrLiteral};

#[test]
fn parameterized_list_subscript_indexes_runtime_list() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = HashMap::from([
        (
            "items".to_owned(),
            IrLiteral::List(vec![
                IrLiteral::Int(10),
                IrLiteral::Int(20),
                IrLiteral::Int(30),
            ]),
        ),
        ("idx".to_owned(), IrLiteral::Int(-1)),
    ]);

    let result = gf
        .execute_with_params("WITH $items AS items RETURN items[$idx] AS value", &params)
        .expect("parameterized list index");
    let values = result.batches[0]
        .column_by_name("value")
        .expect("value")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value is Int64");
    assert_eq!(values.value(0), 30);
}

#[test]
fn list_subscript_rejects_static_non_integer_index() {
    let gf = GraphForge::new(None).expect("in-memory instance");

    let err = gf
        .execute("RETURN [1, 2, 3]['bad'] AS value")
        .expect_err("string list index should be rejected");
    assert!(
        err.to_string()
            .contains("list subscript index must be an integer or null"),
        "unexpected error: {err}"
    );
}

#[test]
fn collect_nodes_feed_list_comprehension_property_access() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name: 'Alice'})")
        .expect("create node");

    let result = gf
        .execute("MATCH (n:Person) WITH collect(n) AS nodes RETURN [x IN nodes | x.name] AS names")
        .expect("collected node list comprehension");
    let names = result.batches[0]
        .column_by_name("names")
        .expect("names")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("names is a ListArray");
    let values = names.value(0);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("names items are Utf8");
    assert_eq!(values.value(0), "Alice");
}

#[test]
fn list_comprehension_rejects_aggregate_projection() {
    let gf = GraphForge::new(None).expect("in-memory instance");

    let err = gf
        .execute("MATCH (n) RETURN [x IN [1, 2, 3] | count(*)]")
        .expect_err("aggregate projection should be rejected");
    assert!(
        err.to_string().contains(
            "aggregate function may not be used inside a list comprehension filter or projection"
        ),
        "unexpected error: {err}"
    );
}
