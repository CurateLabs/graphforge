//! End-to-end guards for map/property value-access semantics fixed under #1029.

use std::collections::HashMap;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use graphforge_api::{GraphForge, IrLiteral};

fn execute_one(gf: &GraphForge, query: &str) -> RecordBatch {
    let result = gf.execute(query).expect(query);
    assert_eq!(result.stats.rows_produced, 1, "{query}");
    result.batches[0].clone()
}

fn execute_one_with_params(
    gf: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
) -> RecordBatch {
    let result = gf.execute_with_params(query, params).expect(query);
    assert_eq!(result.stats.rows_produced, 1, "{query}");
    result.batches[0].clone()
}

fn assert_null(batch: &RecordBatch, name: &str) {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"));
    assert!(
        *col.data_type() == DataType::Null || col.is_null(0),
        "{name} should be null, got {:?}",
        ScalarValue::try_from_array(col, 0)
    );
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

fn str_cell(batch: &RecordBatch, name: &str) -> String {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} is not Utf8"))
        .value(0)
        .to_owned()
}

#[test]
fn static_map_access_absent_key_and_null_container_return_null() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let batch = execute_one(
        &gf,
        "WITH {existing: 42, notMissing: null} AS m, null AS n \
         RETURN m.missing AS missing, m.notMissing AS not_missing, \
                m.existing AS existing, n.anything AS null_container",
    );

    assert_null(&batch, "missing");
    assert_null(&batch, "not_missing");
    assert_null(&batch, "null_container");
    assert_eq!(i64_cell(&batch, "existing"), 42);
}

#[test]
fn static_relationship_property_access_absent_key_returns_null() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:REL {existing: 42, missing: null}]->()")
        .expect("create relationship");
    let batch = execute_one(
        &gf,
        "MATCH ()-[r]->() \
         RETURN r.missing AS missing, r.missingToo AS missing_too, r.existing AS existing",
    );

    assert_null(&batch, "missing");
    assert_null(&batch, "missing_too");
    assert_eq!(i64_cell(&batch, "existing"), 42);
}

#[test]
fn dynamic_map_subscript_uses_runtime_key_lookup() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let batch = execute_one(
        &gf,
        "WITH {name: 'Mats', Name: 'Pontus'} AS m, 'Name' AS k \
         RETURN m[k] AS value, m['nAMe'] AS missing",
    );

    assert_eq!(str_cell(&batch, "value"), "Pontus");
    assert_null(&batch, "missing");
}

#[test]
fn dynamic_entity_property_subscript_uses_computed_key() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({name: 'Apa'})").expect("create node");
    let batch = execute_one(
        &gf,
        "MATCH (n {name: 'Apa'}) \
         RETURN n['nam' + 'e'] AS value, n['missing'] AS missing",
    );

    assert_eq!(str_cell(&batch, "value"), "Apa");
    assert_null(&batch, "missing");
}

#[test]
fn mixed_case_entity_property_access_preserves_exact_field_name() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({displayName: 'Node'})-[:REL {displayName: 'Edge'}]->({name: 'Other'})")
        .expect("create graph");
    let batch = execute_one(
        &gf,
        "MATCH (n)-[r:REL]->() \
         WHERE n.displayName = 'Node' \
         RETURN n.displayName AS node_value, r.displayName AS edge_value",
    );

    assert_eq!(str_cell(&batch, "node_value"), "Node");
    assert_eq!(str_cell(&batch, "edge_value"), "Edge");
}

#[test]
fn dynamic_node_property_subscript_uses_parameter_key() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({name: 'Apa'})").expect("create node");
    let params = HashMap::from([("key".to_owned(), IrLiteral::Str("name".to_owned()))]);
    let batch = execute_one_with_params(
        &gf,
        "MATCH (n {name: 'Apa'}) RETURN n[$key] AS value",
        &params,
    );

    assert_eq!(str_cell(&batch, "value"), "Apa");
}

#[test]
fn dynamic_relationship_property_subscript_uses_parameter_key() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:REL {name: 'Apa'}]->()")
        .expect("create relationship");
    let params = HashMap::from([("key".to_owned(), IrLiteral::Str("name".to_owned()))]);
    let batch = execute_one_with_params(&gf, "MATCH ()-[r]->() RETURN r[$key] AS value", &params);

    assert_eq!(str_cell(&batch, "value"), "Apa");
}

#[test]
fn dynamic_map_subscript_uses_parameter_key() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = HashMap::from([
        (
            "expr".to_owned(),
            IrLiteral::Map(vec![
                ("name".to_owned(), IrLiteral::Str("Mats".to_owned())),
                ("Name".to_owned(), IrLiteral::Str("Pontus".to_owned())),
            ]),
        ),
        ("key".to_owned(), IrLiteral::Str("Name".to_owned())),
    ]);
    let batch = execute_one_with_params(&gf, "WITH $expr AS m RETURN m[$key] AS value", &params);

    assert_eq!(str_cell(&batch, "value"), "Pontus");
}

#[test]
fn dynamic_map_subscript_rejects_non_string_parameter_key() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = HashMap::from([
        (
            "expr".to_owned(),
            IrLiteral::Map(vec![("name".to_owned(), IrLiteral::Str("Mats".to_owned()))]),
        ),
        ("key".to_owned(), IrLiteral::Int(0)),
    ]);
    let err = gf
        .execute_with_params("WITH $expr AS m RETURN m[$key] AS value", &params)
        .expect_err("integer map key should error");

    assert!(
        err.to_string()
            .contains("dynamic map/property access key must be a string"),
        "unexpected error: {err}"
    );
}

#[test]
fn dynamic_subscript_null_key_and_null_container_return_null() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let batch = execute_one(
        &gf,
        "WITH {name: 'Mats'} AS m, null AS k, null AS n \
         RETURN m[k] AS null_key, n['name'] AS null_container",
    );

    assert_null(&batch, "null_key");
    assert_null(&batch, "null_container");
}

#[test]
fn create_property_rejects_map_values() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for query in [
        "CREATE ({bad: {nested: 1}})",
        "CREATE ({bad: [{nested: 1}]})",
        "CREATE ()-[:REL {bad: {nested: 1}}]->()",
    ] {
        let err = gf.execute(query).expect_err(query);
        let msg = err.to_string();
        assert!(
            msg.contains("cannot store map values")
                || msg.contains("cannot be stored as a property value")
                || msg.contains("invalid property type"),
            "unexpected error for {query}: {err}"
        );
    }
}

#[test]
fn set_property_rejects_map_values() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({name: 'A'})").expect("create node");
    let err = gf
        .execute("MATCH (n {name: 'A'}) SET n.bad = {nested: 1}")
        .expect_err("map-valued SET should fail");

    assert!(
        err.to_string().contains("invalid property type"),
        "unexpected error: {err}"
    );
}
