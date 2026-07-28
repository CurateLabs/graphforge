//! Deterministic XOR-chain scaling gate (#1263).

use std::sync::{Arc, Mutex};

use arrow::array::{Array, BooleanArray};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use gf_api::GraphForge;
use gf_core::OntologyMode;
use gf_ir::{BinaryOpKind, Binder, ExprId, GraphPlan, IrExpr, RuntimeCatalog};

#[derive(Debug, Clone, Copy)]
struct Work {
    ir_xor_nodes: usize,
    logical_xor_nodes: usize,
}

fn xor_query(operands: usize, include_null: bool) -> (String, Option<bool>) {
    assert!(operands >= 2);
    let values = (0..operands)
        .map(|index| {
            if include_null && index == operands / 2 {
                None
            } else {
                Some(index % 3 != 1)
            }
        })
        .collect::<Vec<_>>();
    let expression = values
        .iter()
        .map(|value| match value {
            Some(true) => "true",
            Some(false) => "false",
            None => "null",
        })
        .collect::<Vec<_>>()
        .join(" XOR ");
    let expected = values
        .into_iter()
        .try_fold(false, |acc, value| value.map(|value| acc ^ value));
    (format!("RETURN {expression} AS value"), expected)
}

fn bind(query: &str) -> GraphPlan {
    let ast = gf_cypher::parse(query).expect("XOR benchmark query parses");
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    Binder::new(None, catalog, OntologyMode::Exploratory)
        .bind(&ast)
        .expect("XOR benchmark query binds")
}

fn deterministic_work(plan: &GraphPlan) -> Work {
    let ir_xor_nodes = (0..plan.exprs.len())
        .filter(|index| {
            matches!(
                plan.exprs
                    .get(ExprId(u32::try_from(*index).expect("test arena fits u32"))),
                IrExpr::BinaryOp {
                    op: BinaryOpKind::Xor,
                    ..
                }
            )
        })
        .count();
    let logical = gf_rel::lower(plan).expect("XOR benchmark query lowers");
    let mut logical_xor_nodes = 0;
    logical
        .apply(|node| {
            for expression in node.expressions() {
                expression.apply(|expression| {
                    if matches!(
                        expression,
                        datafusion::logical_expr::Expr::ScalarFunction(function)
                            if function.func.name() == "cypher_xor"
                    ) {
                        logical_xor_nodes += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("logical-plan traversal succeeds");
    Work {
        ir_xor_nodes,
        logical_xor_nodes,
    }
}

fn result_value(forge: &GraphForge, query: &str) -> Option<bool> {
    let result = forge.execute(query).expect("XOR benchmark query executes");
    assert_eq!(result.stats.rows_produced, 1);
    let values = result.batches[0]
        .column_by_name("value")
        .expect("value column")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("value is boolean");
    (!values.is_null(0)).then(|| values.value(0))
}

#[test]
fn xor_chain_work_is_linear_and_results_preserve_nulls() {
    let forge = GraphForge::new(None).expect("in-memory forge");
    let mut measured = Vec::new();

    for operands in [11, 22] {
        let (query, expected) = xor_query(operands, true);
        let plan = bind(&query);
        let work = deterministic_work(&plan);
        assert_eq!(work.ir_xor_nodes, operands - 1, "{work:?}");
        assert_eq!(work.logical_xor_nodes, operands - 1, "{work:?}");
        assert_eq!(result_value(&forge, &query), expected);

        let (non_null_query, non_null_expected) = xor_query(operands, false);
        assert_eq!(
            result_value(&forge, &non_null_query),
            non_null_expected,
            "non-null XOR parity for {operands} operands"
        );
        measured.push(work);
    }

    assert!(measured[1].ir_xor_nodes <= measured[0].ir_xor_nodes * 3);
    assert!(measured[1].logical_xor_nodes <= measured[0].logical_xor_nodes * 3);
}
