//! End-to-end guards for Cypher percentile aggregates fixed under #1054.

use std::collections::HashMap;

use arrow::array::{Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use gf_api::{GraphForge, IrLiteral};

fn execute_one_with_params(
    gf: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
) -> RecordBatch {
    let result = gf.execute_with_params(query, params).expect(query);
    assert_eq!(result.stats.rows_produced, 1, "{query}");
    result.batches[0].clone()
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

fn i64_cell(batch: &RecordBatch, name: &str) -> i64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} is not Int64"))
        .value(0)
}

#[test]
fn percentile_disc_returns_discrete_ranked_value() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({price: 10}), ({price: 20}), ({price: 30})")
        .expect("seed prices");

    let params = HashMap::from([("p".to_owned(), IrLiteral::Float(0.5))]);
    let batch = execute_one_with_params(
        &gf,
        "MATCH (n) RETURN percentileDisc(n.price, $p) AS p",
        &params,
    );

    assert_eq!(i64_cell(&batch, "p"), 20);
}

#[test]
fn percentile_cont_interpolates_between_ranked_values() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({price: 10.0}), ({price: 20.0}), ({price: 30.0})")
        .expect("seed prices");

    let params = HashMap::from([("p".to_owned(), IrLiteral::Float(0.25))]);
    let batch = execute_one_with_params(
        &gf,
        "MATCH (n) RETURN percentileCont(n.price, $p) AS p",
        &params,
    );

    assert_eq!(f64_cell(&batch, "p"), 15.0);
}

#[test]
fn percentile_rejects_out_of_range_argument_at_runtime() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ({price: 10.0})").expect("seed price");

    let params = HashMap::from([("p".to_owned(), IrLiteral::Float(1.1))]);
    let err = gf
        .execute_with_params("MATCH (n) RETURN percentileCont(n.price, $p) AS p", &params)
        .expect_err("out-of-range percentile should fail");
    let err = err.to_string();

    assert!(
        err.starts_with("execution error"),
        "expected runtime error, got {err}"
    );
    assert!(
        err.contains("percentile argument must be a finite number between 0.0 and 1.0"),
        "unexpected error: {err}"
    );
}
