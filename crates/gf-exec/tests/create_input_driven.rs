//! Input-driven `GraphCreateExec` tests (#703): the CREATE runs once per input
//! row, referencing MATCH-bound vars' identities (read from the input columns)
//! and minting new vars/edges per row.
//!
//! These drive `GraphCreateExec` directly over a synthetic input plan carrying
//! `var_<n>.node_uuid` / `var_<n>.node_id` columns — the shape a preceding
//! MATCH produces — so the reference path is exercised before the lowering fold
//! (which wires a real MATCH input) lands.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::logical_plan::{LogicalPlan, LogicalTableSource};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use gf_core::OntologyMode;
use gf_core::uuid::{Uuid, new_v7, to_bytes};
use gf_ir::Direction;
use gf_plan::{GraphCreateNode, ResolvedEdgeSpec, ResolvedNodeSpec};
use gf_storage::GraphWriter;

const TS: i64 = 1_700_000_000_000_000;
const PERSON: gf_core::TypeId = gf_core::TypeId(0);

/// Write `count` standalone Person nodes; return their (uuid, node_id) pairs.
fn write_persons(dir: &Path, count: usize) -> Vec<(Uuid, u64)> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let out: Vec<(Uuid, u64)> = (0..count)
        .map(|_| {
            let u = new_v7();
            let id = w.create_node(u, PERSON).unwrap();
            (u, id)
        })
        .collect();
    w.flush().unwrap();
    out
}

/// A logical scan over `var_0.{node_uuid, node_id}` — the columns a preceding
/// `MATCH (a)` would expose for the bound var 0.
fn ref_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("node_id", DataType::UInt64, false),
    ]))
}

fn ref_input_plan() -> LogicalPlan {
    let table = Arc::new(LogicalTableSource::new(ref_input_schema()));
    LogicalPlanBuilder::scan("var_0", table, None)
        .unwrap()
        .build()
        .unwrap()
}

/// Physical input batch: one row per matched Person (var 0), carrying its
/// `node_uuid` + `node_id`.
fn ref_input_batch(persons: &[(Uuid, u64)]) -> RecordBatch {
    let uuids: Vec<[u8; 16]> = persons.iter().map(|(u, _)| to_bytes(u)).collect();
    let uuid_arr = FixedSizeBinaryArray::try_from_iter(uuids.into_iter()).unwrap();
    let ids = UInt64Array::from(persons.iter().map(|(_, id)| *id).collect::<Vec<_>>());
    RecordBatch::try_new(ref_input_schema(), vec![Arc::new(uuid_arr), Arc::new(ids)]).unwrap()
}

async fn run(node: GraphCreateNode, input_batch: RecordBatch) -> (u64, u64) {
    let ctx = SessionContext::new();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(input_batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = Arc::new(gf_exec::GraphCreateExec::new(&node, input));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();
    let b = &out[0];
    let nodes = b
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    let edges = b
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    (nodes, edges)
}

#[tokio::test]
async fn create_per_matched_row_references_and_mints() {
    // 3 existing Persons (the MATCH frontier). For each, CREATE (a)-[:KNOWS]->(b):
    // `a` is a reference (var 0, from the input row), `b` is a fresh mint (var 1).
    let dir = TempDir::new().unwrap();
    let persons = write_persons(dir.path(), 3);

    let node = GraphCreateNode::new(
        Arc::new(ref_input_plan()),
        vec![
            // var 0: the MATCH-bound `a` — referenced, not minted.
            ResolvedNodeSpec {
                var: 0,
                label_ids: vec![0],
                label_names: vec!["Person".to_owned()],
                properties: vec![],
                computed_properties: vec![],
                is_reference: true,
            },
            // var 1: the new `b` — minted per row.
            ResolvedNodeSpec {
                var: 1,
                label_ids: vec![0],
                label_names: vec!["Person".to_owned()],
                properties: vec![],
                computed_properties: vec![],
                is_reference: false,
            },
        ],
        vec![ResolvedEdgeSpec {
            var: 2,
            src: 0,
            dst: 1,
            rel_type_id: Some(0),
            rel_type_name: Some("KNOWS".to_owned()),
            direction: Direction::Out,
            properties: vec![],
            computed_properties: vec![],
        }],
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    );

    let (nodes, edges) = run(node, ref_input_batch(&persons)).await;
    // Per row: 1 mint (`b`) + 1 edge; the reference `a` is not counted.
    assert_eq!((nodes, edges), (3, 3), "one new b + one edge per matched a");

    // The graph grew from 3 → 6 Persons.
    let node_batches = gf_storage::read_nodes(dir.path()).unwrap();
    let total: usize = node_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 6, "3 original + 3 minted b");
}

#[tokio::test]
async fn create_over_empty_match_creates_nothing() {
    // Zero matched rows → zero creates.
    let dir = TempDir::new().unwrap();
    write_persons(dir.path(), 3);

    let node = GraphCreateNode::new(
        Arc::new(ref_input_plan()),
        vec![ResolvedNodeSpec {
            var: 1,
            label_ids: vec![0],
            label_names: vec!["Person".to_owned()],
            properties: vec![],
            computed_properties: vec![],
            is_reference: false,
        }],
        vec![],
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    );

    // Empty input batch (no matched rows).
    let empty = RecordBatch::new_empty(ref_input_schema());
    let (nodes, edges) = run(node, empty).await;
    assert_eq!((nodes, edges), (0, 0), "empty match creates nothing");
}

#[tokio::test]
async fn standalone_create_over_unit_row_creates_once() {
    // No MATCH: a single unit-row input (mirrors the lowerer's source-free base)
    // drives exactly one create.
    let dir = TempDir::new().unwrap();

    let unit = LogicalPlanBuilder::empty(true).build().unwrap();
    let node = GraphCreateNode::new(
        Arc::new(unit),
        vec![ResolvedNodeSpec {
            var: 0,
            label_ids: vec![0],
            label_names: vec!["Person".to_owned()],
            properties: vec![],
            computed_properties: vec![],
            is_reference: false,
        }],
        vec![],
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    );

    // A single unit row (no columns).
    let unit_schema = Arc::new(Schema::empty());
    let unit_batch = RecordBatch::try_new_with_options(
        unit_schema,
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let (nodes, edges) = run(node, unit_batch).await;
    assert_eq!((nodes, edges), (1, 0), "standalone CREATE mints once");
}

#[tokio::test]
async fn null_referenced_node_uuid_errors() {
    // A reference spec whose matched node_uuid is NULL must fail fast (a null
    // slot still yields 16 bytes, which would otherwise decode to a bogus UUID).
    let dir = TempDir::new().unwrap();

    let node = GraphCreateNode::new(
        Arc::new(ref_input_plan()),
        vec![ResolvedNodeSpec {
            var: 0,
            label_ids: vec![0],
            label_names: vec!["Person".to_owned()],
            properties: vec![],
            computed_properties: vec![],
            is_reference: true,
        }],
        vec![],
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    );

    // One input row whose node_uuid is null (e.g. an unmatched OPTIONAL MATCH).
    // Use a nullable schema so the null column is legal at the Arrow layer.
    let nullable_schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("node_id", DataType::UInt64, true),
    ]));
    let uuid_arr =
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(std::iter::once(None::<[u8; 16]>), 16)
            .unwrap();
    let ids = UInt64Array::from(vec![Some(1u64)]);
    let batch =
        RecordBatch::try_new(nullable_schema, vec![Arc::new(uuid_arr), Arc::new(ids)]).unwrap();

    let ctx = SessionContext::new();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = Arc::new(gf_exec::GraphCreateExec::new(&node, input));
    let err = collect(exec, ctx.task_ctx()).await;
    assert!(
        err.is_err(),
        "null matched node_uuid must error, got {err:?}"
    );
}

#[tokio::test]
async fn create_accumulates_across_multiple_input_batches() {
    // #747: the streamed drive writes per batch and sums the totals across all
    // batches (the input is no longer collected into one vec first). Feed a
    // multi-batch input (3 rows over 2 batches) into a per-row mint; expect 3
    // nodes created total.
    let dir = TempDir::new().unwrap();

    let node = GraphCreateNode::new(
        Arc::new(ref_input_plan()),
        // Mint one new node per input row (var distinct from the input's var 0;
        // no reference, so it doesn't read the input columns).
        vec![ResolvedNodeSpec {
            var: 1,
            label_ids: vec![0],
            label_names: vec!["Person".to_owned()],
            properties: vec![],
            computed_properties: vec![],
            is_reference: false,
        }],
        vec![],
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    );

    // Two batches: 2 rows + 1 row, same schema as ref_input_plan (var_0).
    let b1 = ref_input_batch(&[(new_v7(), 10), (new_v7(), 11)]);
    let b2 = ref_input_batch(&[(new_v7(), 12)]);

    let ctx = SessionContext::new();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batches(vec![b1, b2])
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = Arc::new(gf_exec::GraphCreateExec::new(&node, input));
    let out = collect(exec, ctx.task_ctx()).await.unwrap();
    let nodes = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        nodes, 3,
        "one mint per input row, summed across both batches"
    );

    // All 3 minted nodes are persisted.
    let node_batches = gf_storage::read_nodes(dir.path()).unwrap();
    let total: usize = node_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 3);
}
