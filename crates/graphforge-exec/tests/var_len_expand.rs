//! Integration tests for `VarLenExpandExec` (#580).
//!
//! The general read path (`MATCH … RETURN`) is not wired yet (#583), so these
//! drive the physical node directly: write a small graph with [`GraphWriter`],
//! build a source frontier as an in-memory `ExecutionPlan`, construct
//! `VarLenExpandExec`, run it via [`collect`], and assert on the reached
//! destination `node_id`s.
//!
//! Each fixture's `create_node` returns the surrogate `node_id` (starting at 1),
//! which is what the source frontier seeds with and what the output exposes.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_ir::Direction;
use graphforge_plan::{VarLenExpandNode, var_len_edge_list_field};
use graphforge_storage::{GraphWriter, TOPOLOGY_NODES_SCHEMA};

use graphforge_exec::{AdjacencyProvider, ScanBuildAdjacencyProvider, VarLenExpandExec};

/// Construct the exec node with a fresh scan-build provider over the node's
/// project dir — the pre-index behavior these tests pin (the session-injected
/// persistent provider is exercised by `tests/persistent_adjacency.rs`).
fn make_exec(node: &VarLenExpandNode, input: Arc<dyn ExecutionPlan>) -> Arc<VarLenExpandExec> {
    let provider: Arc<dyn AdjacencyProvider> =
        Arc::new(ScanBuildAdjacencyProvider::new(node.dir.clone(), node.mode));
    Arc::new(VarLenExpandExec::new(node, input, provider))
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const TS: i64 = 1_700_000_000_000_000;
const PERSON: graphforge_core::TypeId = graphforge_core::TypeId(0);

/// Write `count` nodes joined into a `KNOWS` chain `n1 -> n2 -> … -> nN`.
/// Returns the surrogate `node_id`s in chain order.
fn write_chain(dir: &Path, count: usize) -> Vec<u64> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..count).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|u| w.create_node(*u, PERSON).unwrap())
        .collect();
    for pair in uuids.windows(2) {
        w.create_edge(new_v7(), "KNOWS", &pair[0], &pair[1])
            .unwrap();
    }
    w.flush().unwrap();
    ids
}

/// Write a directed 3-cycle `n1 -> n2 -> n3 -> n1` (all `KNOWS`).
/// Returns the surrogate `node_id`s.
fn write_cycle(dir: &Path) -> Vec<u64> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let u: Vec<Uuid> = (0..3).map(|_| new_v7()).collect();
    let ids: Vec<u64> = u
        .iter()
        .map(|x| w.create_node(*x, PERSON).unwrap())
        .collect();
    w.create_edge(new_v7(), "KNOWS", &u[0], &u[1]).unwrap();
    w.create_edge(new_v7(), "KNOWS", &u[1], &u[2]).unwrap();
    w.create_edge(new_v7(), "KNOWS", &u[2], &u[0]).unwrap();
    w.flush().unwrap();
    ids
}

/// Source frontier schema: a single `node_id` column (what the BFS seeds from).
fn frontier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

/// Build a `VarLenExpandNode` for the given parameters, with an output schema
/// that extends the frontier with the destination node columns (qualified
/// `var_<dst>`), matching what the lowerer produces.
fn make_node(
    dir: &Path,
    direction: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    src_var: u32,
    dst_var: u32,
) -> VarLenExpandNode {
    make_node_with(
        dir,
        "KNOWS",
        OntologyMode::Strict,
        direction,
        min_hops,
        max_hops,
        src_var,
        dst_var,
    )
}

/// [`make_node`] with an explicit relation type and ontology mode (the
/// exploratory end-to-end coverage, #762).
#[allow(clippy::too_many_arguments)]
fn make_node_with(
    dir: &Path,
    rel_type_name: &str,
    mode: OntologyMode,
    direction: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    src_var: u32,
    dst_var: u32,
) -> VarLenExpandNode {
    use datafusion::logical_expr::LogicalPlanBuilder;

    // The logical input is a scan exposing the frontier schema qualified
    // `var_<src>`, mirroring a NodeScan(src).
    let src_alias = format!("var_{src_var}");
    let table = Arc::new(
        datafusion::logical_expr::logical_plan::LogicalTableSource::new(frontier_schema()),
    );
    let input = LogicalPlanBuilder::scan(src_alias, table, None)
        .unwrap()
        .build()
        .unwrap();

    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    // edge_var is distinct from src/dst (always 0/1 in these tests); the
    // executor produces a trailing `var_<edge>.rels` List column.
    let edge_var = 2;
    VarLenExpandNode::new(
        Arc::new(input),
        rel_type_name,
        min_hops,
        max_hops,
        src_var,
        dst_var,
        edge_var,
        direction,
        Some(0), // rel_ty (unused by the executor; typed mode reads KNOWS.parquet)
        dir.to_path_buf(),
        mode,
        dst_fields,
        var_len_edge_list_field(&[]),
    )
}

/// Run `VarLenExpandExec` over a frontier of `seed` node_ids and return the
/// multiset of reached destination `node_id`s.
async fn run_expand(node: &VarLenExpandNode, seeds: &[u64]) -> Vec<u64> {
    let ctx = SessionContext::new();

    // Source frontier as an in-memory physical plan.
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(seeds.to_vec()))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let exec = make_exec(node, input);
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    // Destination node_id is the second `node_id` column (after the frontier's).
    let mut reached = Vec::new();
    for b in &out {
        // The frontier contributes column 0 (node_id); the destination node's
        // node_id is the topology column at frontier_width + 1.
        let dst_node_id_idx = frontier_schema().fields().len() + 1;
        let col = b
            .column(dst_node_id_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            reached.push(col.value(i));
        }
    }
    reached
}

fn set(v: &[u64]) -> HashSet<u64> {
    v.iter().copied().collect()
}

/// Run the expand and return, per output row, the length of its edge-list
/// (the trailing `var_<edge>.rels` `List<Struct>` column) — i.e. the hop count.
/// Also asserts the column is a `List<Struct>` whose `edge_uuid` child is
/// `FixedSizeBinary(16)` (the UUID-only relationship-list contract, #709).
async fn run_expand_edge_list_lengths(node: &VarLenExpandNode, seeds: &[u64]) -> Vec<usize> {
    use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StructArray};
    use arrow::datatypes::DataType;

    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(seeds.to_vec()))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = make_exec(node, input);
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    let mut lengths = Vec::new();
    for b in &out {
        // The edge list is the LAST column.
        let list = b
            .column(b.num_columns() - 1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("trailing column is a ListArray");
        // Struct element with a FixedSizeBinary(16) `edge_uuid` child.
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let edge_uuid = values
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("edge_uuid child is FixedSizeBinary");
        assert_eq!(edge_uuid.value_length(), 16);
        assert!(matches!(
            list.data_type(),
            DataType::List(f) if matches!(f.data_type(), DataType::Struct(_))
        ));
        for i in 0..b.num_rows() {
            // The edge list is always present (never a null list slot) — a
            // 0-hop path is an EMPTY list, not null.
            assert!(list.is_valid(i), "edge-list row {i} must be non-null");
            lengths.push(list.value(i).len());
        }
    }
    lengths
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_1_to_2_on_chain() {
    // n1 -> n2 -> n3 -> n4 -> n5 ; seed {n1}, KNOWS*1..2.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 1, Some(2), 0, 1);

    let reached = run_expand(&node, &[ids[0]]).await;
    // 1 hop → n2 ; 2 hops → n3.
    assert_eq!(set(&reached), set(&[ids[1], ids[2]]));
    assert_eq!(reached.len(), 2, "one path each at hop 1 and hop 2");
}

#[tokio::test]
async fn unbounded_on_acyclic_chain_terminates() {
    // seed {n1}, KNOWS* (unbounded) on an acyclic chain → all downstream nodes.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 1, None, 0, 1);

    let reached = run_expand(&node, &[ids[0]]).await;
    assert_eq!(set(&reached), set(&ids[1..]));
}

#[tokio::test]
async fn unbounded_on_cycle_terminates() {
    // n1 -> n2 -> n3 -> n1 ; seed {n1}, KNOWS* unbounded.
    // Per-path edge dedup bounds path length to |E| = 3, so this terminates.
    let dir = TempDir::new().unwrap();
    let ids = write_cycle(dir.path());
    let node = make_node(dir.path(), Direction::Out, 1, None, 0, 1);

    let reached = run_expand(&node, &[ids[0]]).await;
    // Reachable from n1: n2 (1 hop), n3 (2 hops), n1 (3 hops, full cycle).
    assert_eq!(set(&reached), set(&ids));
    // Exactly one simple path of each length 1/2/3 from n1.
    assert_eq!(reached.len(), 3, "cycle must terminate with bounded rows");
}

#[tokio::test]
async fn min_hops_excludes_one_hop_neighbours() {
    // seed {n1}, KNOWS*2..3 → excludes the 1-hop neighbour n2.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 2, Some(3), 0, 1);

    let reached = run_expand(&node, &[ids[0]]).await;
    // 2 hops → n3 ; 3 hops → n4. n2 (1 hop) excluded.
    assert_eq!(set(&reached), set(&[ids[2], ids[3]]));
    assert!(
        !reached.contains(&ids[1]),
        "1-hop neighbour must be excluded"
    );
}

#[tokio::test]
async fn direction_in_reaches_predecessors() {
    // n1 -> n2 -> n3 -> n4 -> n5 ; seed {n3}, <-[:KNOWS*1..2]- reaches n2, n1.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::In, 1, Some(2), 0, 1);

    let reached = run_expand(&node, &[ids[2]]).await;
    assert_eq!(set(&reached), set(&[ids[1], ids[0]]));
}

#[tokio::test]
async fn empty_graph_yields_no_rows() {
    let dir = TempDir::new().unwrap();
    // No nodes/edges written.
    let node = make_node(dir.path(), Direction::Out, 1, None, 0, 1);
    let reached = run_expand(&node, &[1]).await;
    assert!(reached.is_empty(), "no edges → no expansions");
}

#[tokio::test]
async fn seeds_from_source_var_not_first_node_id() {
    // Regression: a chained expansion's input carries MULTIPLE node_id columns.
    // The BFS must seed from the source variable's column (`var_0.node_id`), not
    // the first node_id it finds. Here a decoy `var_9.node_id` precedes the real
    // source `var_0.node_id`; the decoy holds n5 (a dead-end leaf), the source
    // holds n1. Correct behaviour expands from n1 → {n2, n3}; the old
    // `index_of("node_id")` would have seeded from the decoy n5 → nothing.
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5); // n1..n5, KNOWS chain

    // Logical input schema: var_9.node_id (decoy) then var_0.node_id (source).
    let decoy = LogicalPlanBuilder::scan(
        "var_9",
        Arc::new(LogicalTableSource::new(frontier_schema())),
        None,
    )
    .unwrap();
    let source = LogicalPlanBuilder::scan(
        "var_0",
        Arc::new(LogicalTableSource::new(frontier_schema())),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let input_plan = decoy.cross_join(source).unwrap().build().unwrap();

    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    let node = VarLenExpandNode::new(
        Arc::new(input_plan),
        "KNOWS",
        1,
        Some(2),
        0, // src_var → must resolve var_0.node_id (column index 1)
        1, // dst_var
        2, // edge_var → trailing var_2.rels List column
        Direction::Out,
        Some(0),
        dir.path().to_path_buf(),
        OntologyMode::Strict,
        dst_fields,
        var_len_edge_list_field(&[]),
    );

    // Physical input batch: two node_id columns — decoy=n5 (dead end), source=n1.
    let two_col_schema = Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::UInt64, false),
        Field::new("node_id", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        two_col_schema,
        vec![
            Arc::new(UInt64Array::from(vec![ids[4]])), // var_9 decoy = n5
            Arc::new(UInt64Array::from(vec![ids[0]])), // var_0 source = n1
        ],
    )
    .unwrap();

    // Build the source ExecutionPlan via MemTable::scan directly: it preserves
    // the raw two-column arrow schema (DataFrame/read_batch would reject two
    // identically-named columns by re-qualifying them).
    let ctx = SessionContext::new();
    let mem = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    let input: Arc<dyn ExecutionPlan> =
        datafusion::catalog::TableProvider::scan(&mem, &ctx.state(), None, &[], None)
            .await
            .unwrap();
    let exec = make_exec(&node, input);
    let out = collect(exec, ctx.task_ctx()).await.unwrap();

    // Destination node_id column = 2 input node_id cols + node_id position (1) = idx 3.
    let mut reached = Vec::new();
    for b in &out {
        let col = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            reached.push(col.value(i));
        }
    }
    assert_eq!(
        set(&reached),
        set(&[ids[1], ids[2]]),
        "must expand from the source var (n1 → n2,n3), not the decoy first node_id (n5)"
    );
}

#[tokio::test]
async fn min_hops_zero_includes_source_self() {
    // seed {n1}, KNOWS*0..1 → the 0-hop source-to-self path (n1) plus the
    // 1-hop neighbour (n2). Verifies Cypher `*0..` semantics.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 0, Some(1), 0, 1);

    let reached = run_expand(&node, &[ids[0]]).await;
    assert_eq!(set(&reached), set(&[ids[0], ids[1]]));
    assert!(
        reached.contains(&ids[0]),
        "min_hops=0 must emit the source-to-self (0-hop) path"
    );
}

// ---------------------------------------------------------------------------
// Edge-list binding (#709)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn edge_list_records_hop_counts() {
    // n1 -> n2 -> n3 -> n4 -> n5 ; seed {n1}, KNOWS*1..2.
    // The edge var binds to the relationship list, so each output row's list
    // length equals its hop count: one 1-hop path (→n2) and one 2-hop path (→n3).
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 1, Some(2), 0, 1);

    let mut lengths = run_expand_edge_list_lengths(&node, &[ids[0]]).await;
    lengths.sort_unstable();
    assert_eq!(
        lengths,
        vec![1, 2],
        "edge-list length == hop count per path"
    );
}

#[tokio::test]
async fn edge_list_zero_hop_self_path_is_empty_list() {
    // KNOWS*0..1 → the 0-hop self path has an EMPTY (non-null) edge list, and
    // the 1-hop path has a single-edge list.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 5);
    let node = make_node(dir.path(), Direction::Out, 0, Some(1), 0, 1);

    let mut lengths = run_expand_edge_list_lengths(&node, &[ids[0]]).await;
    lengths.sort_unstable();
    assert_eq!(
        lengths,
        vec![0, 1],
        "0-hop self path → empty list; 1-hop → one edge"
    );
}

#[tokio::test]
async fn edge_list_carries_edge_properties() {
    // #755: the var-length edge-list struct carries the relation's persisted
    // edge properties, materialised per hop by edge_uuid (NULL for an edge with
    // no property row). Chain n1 -[since=2020]-> n2 -[no props]-> n3.
    use std::collections::HashMap;

    use arrow::array::{Int64Array, ListArray, StructArray};
    use graphforge_ir::IrLiteral;

    let dir = TempDir::new().unwrap();
    let (n1, _n2, _n3, e1, e2) = {
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let (u1, u2, u3) = (new_v7(), new_v7(), new_v7());
        let n1 = w.create_node(u1, PERSON).unwrap();
        let n2 = w.create_node(u2, PERSON).unwrap();
        let n3 = w.create_node(u3, PERSON).unwrap();
        let (e1, e2) = (new_v7(), new_v7());
        w.create_edge(e1, "KNOWS", &u1, &u2).unwrap();
        w.create_edge(e2, "KNOWS", &u2, &u3).unwrap();
        // Only the first edge gets a `since` property.
        w.set_edge_properties(
            &e1,
            Some("KNOWS"),
            HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
        )
        .unwrap();
        w.flush().unwrap();
        (n1, n2, n3, e1, e2)
    };
    let _ = (e1, e2);

    // Build a node whose edge-list struct advertises the `since` prop field
    // (the lowerer discovers this from edge_properties/KNOWS.parquet; here we
    // supply it directly to drive the exec's materialisation).
    let node = {
        use datafusion::logical_expr::LogicalPlanBuilder;
        let table = Arc::new(
            datafusion::logical_expr::logical_plan::LogicalTableSource::new(frontier_schema()),
        );
        let input = LogicalPlanBuilder::scan("var_0", table, None)
            .unwrap()
            .build()
            .unwrap();
        let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
        VarLenExpandNode::new(
            Arc::new(input),
            "KNOWS",
            2,
            Some(2),
            0,
            1,
            2,
            Direction::Out,
            Some(0),
            dir.path().to_path_buf(),
            OntologyMode::Strict,
            dst_fields,
            var_len_edge_list_field(&[Field::new("since", DataType::Int64, true)]),
        )
    };

    // Seed n1; the only 2-hop path is n1→n2→n3, edges [e1, e2].
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(vec![n1]))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let out = collect(make_exec(&node, input), ctx.task_ctx())
        .await
        .unwrap();

    // One 2-hop path; its edge-list struct's `since` child is [2020, NULL].
    let total: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "one 2-hop path n1→n2→n3");
    let b = &out[0];
    let list = b
        .column(b.num_columns() - 1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("trailing column is a ListArray");
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let since = values
        .column_by_name("since")
        .expect("struct has a `since` child (#755)")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("since child is Int64");
    // Flattened hops in path order: e1 (since=2020), e2 (no row → NULL).
    assert_eq!(since.len(), 2, "two hops flattened");
    assert_eq!(since.value(0), 2020, "first edge has since=2020");
    assert!(since.is_null(1), "second edge has no `since` → NULL");
}

// ---------------------------------------------------------------------------
// Adjacency provider migration (#762)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn undirected_reaches_both_sides() {
    // n1 -> n2 -> n3 ; seed {n2}, KNOWS*1..1 Undirected reaches both the
    // predecessor and the successor.
    let dir = TempDir::new().unwrap();
    let ids = write_chain(dir.path(), 3);
    let node = make_node(dir.path(), Direction::Undirected, 1, Some(1), 0, 1);
    let reached = run_expand(&node, &[ids[1]]).await;
    assert_eq!(set(&reached), set(&[ids[0], ids[2]]));
}

#[tokio::test]
async fn exploratory_mode_filters_rel_types_end_to_end() {
    // KNOWS and OWNS rows share `_exploratory.parquet`; a KNOWS expansion must
    // not traverse the decoy OWNS edge.
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    let ids: Vec<u64> = [a, b, c]
        .iter()
        .map(|u| w.create_node(*u, PERSON).unwrap())
        .collect();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
    w.flush().unwrap();

    let node = make_node_with(
        dir.path(),
        "KNOWS",
        OntologyMode::Exploratory,
        Direction::Out,
        1,
        Some(1),
        0,
        1,
    );
    let reached = run_expand(&node, &[ids[0]]).await;
    assert_eq!(set(&reached), set(&[ids[1]]), "OWNS edge not traversed");
}

#[tokio::test]
async fn explain_display_shows_adjacency_status() {
    // The node's plan-display line carries `adjacency=hit|miss|building`
    // (#762). The scan-build provider always reports `building` until the
    // persistent index lands (#761).
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 2);
    let node = make_node(dir.path(), Direction::Out, 1, Some(1), 0, 1);

    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(vec![1u64]))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = make_exec(&node, input);

    let line = datafusion::physical_plan::displayable(exec.as_ref())
        .one_line()
        .to_string();
    assert!(line.contains("adjacency=building"), "got: {line}");
    assert!(line.contains("rel=KNOWS"), "got: {line}");
}
