//! Integration tests for `OptionalMatchExec` (#581).
//!
//! The general read path (`MATCH … OPTIONAL MATCH …`) is not wired yet (#583),
//! and the lowerer cannot yet produce non-empty join keys (fixed single-hop
//! join lowering is also #583). So these drive the physical node DIRECTLY:
//! hand-build an outer and an inner input that share a `node_id` join-key
//! column, construct `OptionalMatchExec`, run it via `collect`, and assert the
//! LEFT-join + null-shaping behaviour — unmatched outer rows keep their columns
//! and get **null** in every inner column.

use std::sync::Arc;

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::logical_plan::LogicalTableSource;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;

use gf_exec::OptionalMatchExec;
use gf_plan::OptionalMatchNode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A `LogicalPlan` scan over `schema` qualified `var_<var>` (mirrors a scan the
/// lowerer would emit), used only to give the node a qualified input schema.
fn scan(var: u32, schema: Arc<Schema>) -> datafusion::logical_expr::LogicalPlan {
    LogicalPlanBuilder::scan(
        format!("var_{var}"),
        Arc::new(LogicalTableSource::new(schema)),
        None,
    )
    .unwrap()
    .build()
    .unwrap()
}

/// A physical `ExecutionPlan` over `batch`, via `MemTable::scan` (preserves the
/// raw arrow schema, including duplicate bare column names).
async fn mem_exec(ctx: &SessionContext, batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
    let mem = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    TableProvider::scan(&mem, &ctx.state(), None, &[], None)
        .await
        .unwrap()
}

fn u64s(v: Vec<u64>) -> Arc<UInt64Array> {
    Arc::new(UInt64Array::from(v))
}

/// Outer schema: a single `node_id` join-key column.
fn outer_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

/// Inner schema: the shared `node_id` key (col 0) + a payload `b_val` (col 1).
fn inner_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::UInt64, false),
        Field::new("b_val", DataType::UInt64, false),
    ]))
}

/// Build the `OptionalMatchNode` for the single-key shape: outer `var_0` keyed
/// on `node_id`, inner `var_1` whose non-key output column is `b_val`.
fn single_key_node() -> OptionalMatchNode {
    let outer = scan(0, outer_schema());
    let inner = scan(1, inner_schema());
    // join key: outer col 0 (node_id) == inner col 0 (node_id).
    let join_keys = vec![(0usize, 0usize)];
    // kept inner columns (shared key column 0 excluded): just `b_val` (col 1).
    let inner_keep_idx = vec![1usize];
    OptionalMatchNode::new(Arc::new(outer), Arc::new(inner), join_keys, inner_keep_idx)
}

/// Run the node over the given outer node_ids and inner (key, b_val) rows.
/// Returns `(node_id, Option<b_val>)` per output row (None = nulled inner).
async fn run(
    node: &OptionalMatchNode,
    outer_ids: Vec<u64>,
    inner_rows: Vec<(u64, u64)>,
) -> Vec<(u64, Option<u64>)> {
    let ctx = SessionContext::new();

    let outer_batch = RecordBatch::try_new(outer_schema(), vec![u64s(outer_ids)]).unwrap();
    let (keys, vals): (Vec<u64>, Vec<u64>) = inner_rows.into_iter().unzip();
    let inner_batch = RecordBatch::try_new(inner_schema(), vec![u64s(keys), u64s(vals)]).unwrap();

    let outer = mem_exec(&ctx, outer_batch).await;
    let inner = mem_exec(&ctx, inner_batch).await;
    let exec = Arc::new(OptionalMatchExec::new(node, outer, inner));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    let mut rows = Vec::new();
    for b in &out {
        // Output: col 0 = outer node_id, col 1 = inner b_val (nullable).
        let nid = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let bval = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            let v = if bval.is_null(i) {
                None
            } else {
                Some(bval.value(i))
            };
            rows.push((nid.value(i), v));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn null_shapes_unmatched_outer_rows() {
    // Outer {a1=1, a2=2}; inner matches only a1 (→ b_val 100).
    let node = single_key_node();
    let mut rows = run(&node, vec![1, 2], vec![(1, 100)]).await;
    rows.sort();
    // a1 keeps its real inner value; a2 is preserved with a NULL inner column.
    assert_eq!(rows, vec![(1, Some(100)), (2, None)]);
}

#[tokio::test]
async fn fan_out_one_outer_to_many_inner() {
    // a1 matches two inner rows → two output rows, both with outer a1.
    let node = single_key_node();
    let mut rows = run(&node, vec![1], vec![(1, 100), (1, 200)]).await;
    rows.sort();
    assert_eq!(rows, vec![(1, Some(100)), (1, Some(200))]);
}

#[tokio::test]
async fn left_semantics_no_match_keeps_outer() {
    // Inner keyed only on a different node → the outer row survives, nulled.
    let node = single_key_node();
    let rows = run(&node, vec![2], vec![(1, 100)]).await;
    assert_eq!(
        rows,
        vec![(2, None)],
        "LEFT (not INNER): outer row preserved"
    );
}

#[tokio::test]
async fn empty_inner_nulls_all_outer() {
    let node = single_key_node();
    let mut rows = run(&node, vec![1, 2], vec![]).await;
    rows.sort();
    assert_eq!(rows, vec![(1, None), (2, None)]);
}

#[tokio::test]
async fn inner_stream_with_zero_batches_null_shapes() {
    // Regression: a child ExecutionPlan that yields ZERO batches (not one
    // empty-row batch) must still null-shape, not error. MemTable with an empty
    // partition (`vec![vec![]]`) produces such a stream.
    let node = single_key_node();
    let ctx = SessionContext::new();

    let outer_batch = RecordBatch::try_new(outer_schema(), vec![u64s(vec![1, 2])]).unwrap();
    let outer = mem_exec(&ctx, outer_batch).await;

    // Inner: a MemTable over the inner schema with NO batches in its partition.
    let inner_mem = MemTable::try_new(inner_schema(), vec![vec![]]).unwrap();
    let inner: Arc<dyn ExecutionPlan> =
        TableProvider::scan(&inner_mem, &ctx.state(), None, &[], None)
            .await
            .unwrap();

    let exec = Arc::new(OptionalMatchExec::new(&node, outer, inner));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    let mut rows = Vec::new();
    for b in &out {
        let nid = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let bval = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((
                nid.value(i),
                if bval.is_null(i) {
                    None
                } else {
                    Some(bval.value(i))
                },
            ));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![(1u64, None), (2, None)],
        "empty inner stream → null-shaped outer rows"
    );
}

#[tokio::test]
async fn multi_key_requires_full_tuple_match() {
    // Two-column key: outer (k1, k2), inner (k1, k2, b_val). A row matches only
    // when BOTH key columns are equal. The join works on column *indices*, so
    // the logical schemas only need to be valid DFSchemas (distinct field names
    // under one qualifier); the physical batches below carry the real data.
    let outer_logical_schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::UInt64, false),
        Field::new("k2", DataType::UInt64, false),
    ]));
    let inner_logical_schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::UInt64, false),
        Field::new("k2", DataType::UInt64, false),
        Field::new("b_val", DataType::UInt64, false),
    ]));
    // Physical batches use UInt64 columns; names are irrelevant to the join.
    let outer_schema = outer_logical_schema.clone();
    let inner_schema = inner_logical_schema.clone();
    let outer_logical = scan(0, outer_logical_schema);
    let inner_logical = scan(1, inner_logical_schema);
    let node = OptionalMatchNode::new(
        Arc::new(outer_logical),
        Arc::new(inner_logical),
        vec![(0, 0), (1, 1)], // both columns are keys
        vec![2usize],         // kept inner column: b_val (key cols 0,1 excluded)
    );

    let ctx = SessionContext::new();
    let outer_batch = RecordBatch::try_new(
        outer_schema,
        vec![u64s(vec![1, 1]), u64s(vec![10, 99])], // (1,10) and (1,99)
    )
    .unwrap();
    let inner_batch = RecordBatch::try_new(
        inner_schema,
        vec![u64s(vec![1]), u64s(vec![10]), u64s(vec![500])], // only (1,10) present
    )
    .unwrap();
    let outer = mem_exec(&ctx, outer_batch).await;
    let inner = mem_exec(&ctx, inner_batch).await;
    let exec = Arc::new(OptionalMatchExec::new(&node, outer, inner));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    // Output cols: 2 outer key cols + 1 inner b_val (key cols excluded).
    let mut rows = Vec::new();
    for b in &out {
        let k1 = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let k2 = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        let bval = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            let v = if bval.is_null(i) {
                None
            } else {
                Some(bval.value(i))
            };
            rows.push((k1.value(i), k2.value(i), v));
        }
    }
    rows.sort();
    // (1,10) matches → 500 ; (1,99) does not (k2 differs) → null.
    assert_eq!(rows, vec![(1, 10, Some(500)), (1, 99, None)]);
}
