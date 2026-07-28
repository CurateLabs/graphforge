//! Integration tests for `UnwindExec` (#582).
//!
//! The general read path and `IrExpr::ListLiteral` lowering are not wired yet
//! (#583 / deferred), so these drive the physical node DIRECTLY: build an input
//! batch (optionally carrying a `ListArray` column), a `list_expr` (a column
//! ref or a list literal), construct `UnwindExec`, run it via `collect`, and
//! assert the explosion + null/empty semantics.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, ListArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::logical_plan::LogicalTableSource;
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlanBuilder, col};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;

use gf_exec::UnwindExec;
use gf_plan::UnwindNode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `Int64` element field type used throughout these tests.
fn elem_field() -> Field {
    Field::new("elem", DataType::Int64, true)
}

/// Build a `LogicalPlan` scan exposing `schema`, qualified `var_0`, to give the
/// `UnwindNode` a valid input schema for its output-schema construction.
fn scan(schema: Arc<Schema>) -> datafusion::logical_expr::LogicalPlan {
    LogicalPlanBuilder::scan("var_0", Arc::new(LogicalTableSource::new(schema)), None)
        .unwrap()
        .build()
        .unwrap()
}

/// A physical input plan over `batch`.
async fn input_exec(ctx: &SessionContext, batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
    ctx.read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

/// Build a nullable `Int64` `ListArray` from per-row option-lists
/// (`None` row = null list).
fn list_array(rows: Vec<Option<Vec<i64>>>) -> ListArray {
    ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(
        rows.into_iter()
            .map(|opt| opt.map(|v| v.into_iter().map(Some).collect::<Vec<_>>())),
    )
}

/// Collect the exploded `elem` column (last column) as options.
fn elem_values(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let last = b.num_columns() - 1;
        let col = b
            .column(last)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push(if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests — list literal expression
// ---------------------------------------------------------------------------

/// Run UNWIND of a constant list literal over a single dummy input row.
async fn run_literal(values: &[i64]) -> Vec<Option<i64>> {
    let ctx = SessionContext::new();
    // One dummy input row (single `tag` column we don't read).
    let in_schema = Arc::new(Schema::new(vec![Field::new(
        "tag",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        in_schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![7u64]))],
    )
    .unwrap();

    let scalars: Vec<ScalarValue> = values
        .iter()
        .map(|&v| ScalarValue::Int64(Some(v)))
        .collect();
    // A list-literal Expr: ScalarValue::List wrapping a single-row ListArray.
    let list = ScalarValue::new_list(&scalars, &DataType::Int64, true);
    let list_expr = DfExpr::Literal(ScalarValue::List(list), None);

    let node = UnwindNode::new(Arc::new(scan(in_schema)), list_expr, "var_1", &elem_field());
    let input = input_exec(&ctx, batch).await;
    let exec = Arc::new(UnwindExec::new(&node, input));
    elem_values(&collect(exec, ctx.task_ctx()).await.unwrap())
}

#[tokio::test]
async fn literal_list_three_elements() {
    // UNWIND [1,2,3] AS x → 3 rows.
    assert_eq!(
        run_literal(&[1, 2, 3]).await,
        vec![Some(1), Some(2), Some(3)]
    );
}

// ---------------------------------------------------------------------------
// Tests — UNWIND over an input list column (per-row lists)
// ---------------------------------------------------------------------------

/// Run UNWIND of the input column `items` (a ListArray), returning
/// `(id, Option<elem>)` per output row so we can check input-column replication.
async fn run_column(rows: Vec<(u64, Option<Vec<i64>>)>) -> Vec<(u64, Option<i64>)> {
    let ctx = SessionContext::new();
    let (ids, lists): (Vec<u64>, Vec<Option<Vec<i64>>>) = rows.into_iter().unzip();
    let items = list_array(lists);
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("items", items.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        in_schema.clone(),
        vec![Arc::new(UInt64Array::from(ids)), Arc::new(items)],
    )
    .unwrap();

    let node = UnwindNode::new(
        Arc::new(scan(in_schema)),
        col("items"),
        "var_1",
        &elem_field(),
    );
    let input = input_exec(&ctx, batch).await;
    let exec = Arc::new(UnwindExec::new(&node, input));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    let mut result = Vec::new();
    for b in &out {
        let id = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let last = b.num_columns() - 1;
        let elem = b
            .column(last)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            let e = if elem.is_null(i) {
                None
            } else {
                Some(elem.value(i))
            };
            result.push((id.value(i), e));
        }
    }
    result
}

#[tokio::test]
async fn empty_list_yields_no_rows() {
    // UNWIND [] AS x → 0 rows for that input row.
    let rows = run_column(vec![(1, Some(vec![]))]).await;
    assert!(rows.is_empty(), "empty list → zero rows");
}

#[tokio::test]
async fn null_list_yields_no_rows() {
    // UNWIND null AS x → 0 rows for that input row.
    let rows = run_column(vec![(1, None)]).await;
    assert!(rows.is_empty(), "null list → zero rows");
}

#[tokio::test]
async fn per_row_fan_out_preserves_input_columns() {
    // Row 1 → [10,20]; row 2 → [30,40,50] ⇒ 5 output rows, each carrying its id.
    let rows = run_column(vec![(1, Some(vec![10, 20])), (2, Some(vec![30, 40, 50]))]).await;
    assert_eq!(
        rows,
        vec![
            (1, Some(10)),
            (1, Some(20)),
            (2, Some(30)),
            (2, Some(40)),
            (2, Some(50)),
        ]
    );
}

#[tokio::test]
async fn mixed_null_empty_and_nonempty_rows() {
    // null and empty lists drop their rows; only the non-empty one explodes.
    let rows = run_column(vec![(1, None), (2, Some(vec![])), (3, Some(vec![99]))]).await;
    assert_eq!(rows, vec![(3, Some(99))]);
}
