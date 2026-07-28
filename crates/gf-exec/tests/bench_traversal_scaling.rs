//! Traversal scaling benchmark + I/O smoke gate (#767 T1, #838 latency).
//!
//! The criterion: with the adjacency index present, a localized k-hop traversal
//! reads and decodes work proportional to its neighborhood, independent of total
//! graph size — no full table scan, and (after #838) flat decode + latency.
//!
//! This file drives [`VarLenExpandExec`] directly (the differential corpus
//! already pins end-to-end correctness; here we measure operator I/O), over a
//! deterministic ring graph, comparing two providers on the *same* fixture:
//! - **index (Hit)**: [`PersistentAdjacencyProvider`] over a built CSR index —
//!   serves adjacency from the index and reads only the traversed edge records
//!   (an `edge_id`-filtered read), so `edge_full_reads == 0`;
//! - **scan-build (Miss)**: [`ScanBuildAdjacencyProvider`] — reads the whole
//!   edge file to build adjacency in memory, so `edge_full_reads >= 1` and
//!   `edge_full_rows >= total_edges`.
//!
//! The [`io_stats`](gf_storage::io_stats) counters make that contrast machine-
//! checkable. The non-ignored [`io_smoke`] test pins it on a tiny fixture every
//! CI run; the `#[ignore]`, release-only scaling tests
//! ([`scaling_localized_traversal_is_neighborhood_proportional`] and the
//! scattered worst-case counterpart) run via `make bench-traversal`.
//!
//! Counter-soundness constraints honored here (per the #767 completeness audit):
//! the Miss baseline runs through `VarLenExpandExec`/`ScanBuildAdjacencyProvider`
//! (a single-hop *join* would full-scan via an uninstrumented path); Miss
//! assertions use `>=` (a Miss can do more than one full read); the fixture is
//! sparse enough that a Hit's traversed neighborhood stays well under half the
//! edge file (above that, the filtered read's fallback would count as a full
//! read); and `io_stats::reset()` runs after all fixture writes, immediately
//! before the measured `collect`, single-threaded under `GUARD`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use gf_core::uuid::{Uuid, new_v7};
use gf_core::{OntologyMode, TypeId};
use gf_ir::Direction;
use gf_plan::{VarLenExpandNode, var_len_edge_list_field};
use gf_storage::adjacency::build_adjacency_index;
use gf_storage::{GraphWriter, TOPOLOGY_NODES_SCHEMA, io_stats};

use gf_exec::{
    AdjacencyProvider, PersistentAdjacencyProvider, ScanBuildAdjacencyProvider, VarLenExpandExec,
};

const TS: i64 = 1_700_000_000_000_000;
const PERSON: TypeId = TypeId(0);

/// Serializes the process-global [`io_stats`] counters across parallel tests.
static GUARD: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Ring-graph fixture
// ---------------------------------------------------------------------------

/// Build a deterministic ring-successor graph: node `i` has a `KNOWS` edge to
/// each of `(i + 1) ..= (i + fan_out)` (mod `n`), giving every node out-degree
/// exactly `fan_out` and `n * fan_out` edges total. The k-hop neighborhood of
/// any seed is structurally identical at every `n`, so per-hop cost is
/// independent of total edges (the property #767 measures).
///
/// Edges are inserted in a **permuted** order (a stride walk by a large coprime)
/// so a node's out-edges do not land in one contiguous `edge_id` run — otherwise
/// Parquet row-group pruning on `edge_id` would be unrealistically perfect and
/// overstate the index win. Returns `num_seeds` evenly-spaced seed `node_id`s.
fn generate_ring_graph(dir: &Path, n: usize, fan_out: usize, num_seeds: usize) -> Vec<u64> {
    assert!(n > fan_out, "fan_out must be < n");
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..n).map(|_| new_v7()).collect();
    let node_ids: Vec<u64> = uuids
        .iter()
        .map(|u| w.create_node(*u, PERSON).unwrap())
        .collect();

    // Edge list in (src_index, dst_index) form, then walked in a strided order.
    let total = n * fan_out;
    // Largest prime below a round number, coprime to any `total` we use here.
    let stride: u64 = 1_000_003;
    for step in 0..total {
        let scrambled =
            u64::try_from(step).unwrap().wrapping_mul(stride) % u64::try_from(total).unwrap();
        let e = usize::try_from(scrambled).unwrap();
        let src = e / fan_out;
        let dst = (src + 1 + (e % fan_out)) % n;
        w.create_edge(new_v7(), "KNOWS", &uuids[src], &uuids[dst])
            .unwrap();
    }
    w.flush().unwrap();

    let step = (n / num_seeds).max(1);
    (0..num_seeds).map(|k| node_ids[(k * step) % n]).collect()
}

/// Like [`generate_ring_graph`] but inserts edges in **natural** order (node
/// `i`'s out-edges consecutively, so `edge_id` is contiguous per source) and
/// returns a **localized** seed block — `num_seeds` consecutive nodes in the
/// middle of the ring. A k-hop traversal from a contiguous block stays within a
/// narrow `node_id` / `edge_id` window, so the parquet page index skips the
/// pages outside it: reads (and latency) are proportional to the neighborhood,
/// not the total — the realistic "explore from a node" case.
fn generate_ring_natural(dir: &Path, n: usize, fan_out: usize, num_seeds: usize) -> Vec<u64> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..n).map(|_| new_v7()).collect();
    let node_ids: Vec<u64> = uuids
        .iter()
        .map(|u| w.create_node(*u, PERSON).unwrap())
        .collect();
    for src in 0..n {
        for k in 1..=fan_out {
            w.create_edge(new_v7(), "KNOWS", &uuids[src], &uuids[(src + k) % n])
                .unwrap();
        }
    }
    w.flush().unwrap();
    // A contiguous seed block mid-ring; wrap so a small GF_BENCH_N* override
    // can't index out of bounds (`num_seeds` is tiny vs any real scale).
    let base = n / 2;
    (0..num_seeds).map(|k| node_ids[(base + k) % n]).collect()
}

// ---------------------------------------------------------------------------
// VarLenExpandExec harness (mirrors tests/var_len_expand.rs)
// ---------------------------------------------------------------------------

fn frontier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

fn make_node(dir: &Path, min_hops: u16, max_hops: Option<u16>) -> VarLenExpandNode {
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let (src_var, dst_var, edge_var) = (0u32, 1u32, 2u32);
    let table = Arc::new(LogicalTableSource::new(frontier_schema()));
    let input = LogicalPlanBuilder::scan(format!("var_{src_var}"), table, None)
        .unwrap()
        .build()
        .unwrap();
    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    VarLenExpandNode::new(
        Arc::new(input),
        "KNOWS",
        min_hops,
        max_hops,
        src_var,
        dst_var,
        edge_var,
        Direction::Out,
        Some(0),
        dir.to_path_buf(),
        OntologyMode::Strict,
        dst_fields,
        var_len_edge_list_field(&[]),
    )
}

/// Run a `*min..max` expansion from `seeds` under `provider`, measuring the I/O
/// it performs. Resets the counters immediately before `collect` (under the
/// caller-held [`GUARD`]) so only the traversal's reads are attributed. Returns
/// the sorted reached destination `node_id`s and the I/O snapshot.
async fn run_measured(
    node: &VarLenExpandNode,
    provider: Arc<dyn AdjacencyProvider>,
    seeds: &[u64],
) -> (Vec<u64>, io_stats::IoSnapshot) {
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
    let exec = Arc::new(VarLenExpandExec::new(node, input, provider));

    io_stats::reset();
    let out = collect(exec, ctx.task_ctx()).await.unwrap();
    let snap = io_stats::snapshot();

    let dst_idx = frontier_schema().fields().len() + 1;
    let mut reached = Vec::new();
    for b in &out {
        let col = b
            .column(dst_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            reached.push(col.value(i));
        }
    }
    reached.sort_unstable();
    (reached, snap)
}

fn persistent(dir: &Path) -> Arc<dyn AdjacencyProvider> {
    Arc::new(PersistentAdjacencyProvider::new(
        dir.to_path_buf(),
        OntologyMode::Strict,
    )) as Arc<dyn AdjacencyProvider>
}

fn scan_build(dir: &Path) -> Arc<dyn AdjacencyProvider> {
    Arc::new(ScanBuildAdjacencyProvider::new(
        dir.to_path_buf(),
        OntologyMode::Strict,
    )) as Arc<dyn AdjacencyProvider>
}

// ---------------------------------------------------------------------------
// CI smoke gate: the index Hit issues no full edge scan; scan-build does
// ---------------------------------------------------------------------------

/// Deterministic, tiny-fixture proof of the T1 I/O contract, run on every CI
/// build. Scale (the flat-per-hop timing) is the `#[ignore]` follow-up; the
/// invariant here is size-independent.
///
/// Synchronous so the process-global-counter `GUARD` is never held across an
/// `.await`; the async traversal runs on a local current-thread runtime.
#[test]
fn io_smoke_index_hit_issues_no_full_edge_scan() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let (n, fan_out) = (256usize, 8usize);
    let total_edges = u64::try_from(n * fan_out).unwrap();
    let seeds = generate_ring_graph(dir.path(), n, fan_out, 4);
    build_adjacency_index(dir.path(), TS).unwrap();

    let node = make_node(dir.path(), 1, Some(2));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Index Hit: adjacency from CSR, only traversed edge records read.
    let (hit_reached, hit) = rt.block_on(run_measured(&node, persistent(dir.path()), &seeds));
    assert_eq!(
        hit.edge_full_reads, 0,
        "index Hit must not full-scan the edge file: {hit:?}"
    );
    assert_eq!(hit.edge_full_rows, 0, "{hit:?}");
    assert!(
        hit.edge_filtered_reads >= 1 && hit.edge_filtered_rows > 0,
        "the traversal still reads its traversed edge records: {hit:?}"
    );
    assert!(
        hit.edge_filtered_rows * 2 < total_edges,
        "neighborhood ({}) must stay well under half the edge file ({total_edges}) \
         or the filtered read falls back to a full scan",
        hit.edge_filtered_rows
    );
    // Node side (#838): the Hit reads only the reached destination node records,
    // never the whole node table.
    let total_nodes = u64::try_from(n).unwrap();
    assert_eq!(
        hit.node_full_reads, 0,
        "index Hit must not full-scan the node file: {hit:?}"
    );
    assert!(
        hit.node_filtered_reads >= 1 && hit.node_filtered_rows > 0,
        "the traversal reads its reached node records: {hit:?}"
    );
    assert!(
        hit.node_filtered_rows * 2 < total_nodes,
        "reached neighborhood ({}) must stay well under half the node file ({total_nodes})",
        hit.node_filtered_rows
    );

    // Scan-build Miss baseline: full edge-file scan to build adjacency.
    let (miss_reached, miss) = rt.block_on(run_measured(&node, scan_build(dir.path()), &seeds));
    assert!(
        miss.edge_full_reads >= 1,
        "scan-build must full-scan the edge file: {miss:?}"
    );
    assert!(
        miss.edge_full_rows >= total_edges,
        "scan-build reads at least every edge ({total_edges}): {miss:?}"
    );

    // Same traversal, same answer regardless of provider.
    assert_eq!(
        hit_reached, miss_reached,
        "index and scan-build must reach the same destinations"
    );
    assert!(!hit_reached.is_empty(), "the traversal reached something");
}

// ---------------------------------------------------------------------------
// Scaling benchmark (#[ignore], release-only): traversal edge I/O is
// independent of the total edge count, with the index warm.
// ---------------------------------------------------------------------------

/// Median wall-clock of `runs` traversals (`*1..=hops`) under `provider`,
/// reusing the provider across runs so its CSR view cache stays warm.
async fn median_expand(
    dir: &Path,
    provider: &Arc<dyn AdjacencyProvider>,
    seeds: &[u64],
    hops: u16,
    runs: usize,
) -> std::time::Duration {
    let node = make_node(dir, 1, Some(hops));
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
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
        let exec = Arc::new(VarLenExpandExec::new(&node, input, Arc::clone(provider)));
        let start = std::time::Instant::now();
        let _ = collect(exec, ctx.task_ctx()).await.unwrap();
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// One scale's measurements over a warm index: the `*1..3` I/O snapshot (the
/// edge-count-independent signal — both edge AND node reads are now
/// neighborhood-proportional, #830 + #838) plus median wall-clock per hop count
/// (reported for context; timing in CI is noisy so it is not asserted).
struct ScaleResult {
    edges: usize,
    nodes: usize,
    io: io_stats::IoSnapshot,
    h1: std::time::Duration,
    h2: std::time::Duration,
    h3: std::time::Duration,
}

async fn bench_scale(n: usize, fan_out: usize, num_seeds: usize, localized: bool) -> ScaleResult {
    let dir = TempDir::new().unwrap();
    let seeds = if localized {
        generate_ring_natural(dir.path(), n, fan_out, num_seeds)
    } else {
        generate_ring_graph(dir.path(), n, fan_out, num_seeds)
    };
    build_adjacency_index(dir.path(), TS).unwrap();
    let provider = persistent(dir.path());
    // Warm the CSR view cache (and the OS page cache) before measuring.
    let _ = median_expand(dir.path(), &provider, &seeds, 1, 1).await;

    // The edge-count-independent signal: traversed edge records for *1..3.
    let node3 = make_node(dir.path(), 1, Some(3));
    let (_, io) = run_measured(&node3, Arc::clone(&provider), &seeds).await;

    ScaleResult {
        edges: n * fan_out,
        nodes: n,
        io,
        h1: median_expand(dir.path(), &provider, &seeds, 1, 5).await,
        h2: median_expand(dir.path(), &provider, &seeds, 2, 5).await,
        h3: median_expand(dir.path(), &provider, &seeds, 3, 5).await,
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    // Fail fast on a present-but-malformed override: a typo'd GF_BENCH_N*
    // should not silently produce numbers for the wrong scale.
    match std::env::var(key) {
        Ok(v) => v
            .parse::<usize>()
            .unwrap_or_else(|e| panic!("invalid {key}={v:?}: expected a positive integer ({e})")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("invalid {key}: value is not valid Unicode")
        }
    }
}

fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn print_row(label: &str, t: &ScaleResult) {
    println!(
        "| {label} | {} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} |",
        t.edges,
        t.nodes,
        t.io.edge_filtered_rows,
        t.io.edge_scanned_rows,
        t.io.node_filtered_rows,
        t.io.node_scanned_rows,
        ms(t.h1),
        ms(t.h2),
        ms(t.h3),
    );
}

/// The headline result (#838): a **localized** k-hop traversal over a warm index
/// reads — and decodes — work proportional to its neighborhood, **independent of
/// total graph size**. Wall-clock is therefore ~flat across a 10× growth.
///
/// `node_filtered_rows`/`edge_filtered_rows` (rows *materialized*) are identical
/// across scales by the ring's vertex-transitivity. The decode-cost proof is
/// `*_scanned_rows` (rows the predicate evaluated = pages the page index did not
/// skip): for a clustered id range these stay bounded as the file grows, so the
/// read is sub-linear — the floor #838 removes. Exact dense node-row selection
/// also bounds scattered node hydration; permuted edge ids remain page-scan
/// bound (see [`scaling_scattered_node_ids_are_pruned`]).
///
/// Run via `make bench-traversal` (release). Override scale with
/// `GF_BENCH_N1` / `GF_BENCH_N2` (node counts, fan-out 16).
#[ignore = "scaling benchmark — run via `make bench-traversal` (release)"]
#[test]
fn scaling_localized_traversal_is_neighborhood_proportional() {
    let _g = GUARD.lock().unwrap(); // run_measured touches the global counters
    let fan_out = 16usize;
    let n1 = env_usize("GF_BENCH_N1", 62_500); // ~1M edges
    let n2 = env_usize("GF_BENCH_N2", 625_000); // ~10M edges
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let small = rt.block_on(bench_scale(n1, fan_out, 8, true));
    let large = rt.block_on(bench_scale(n2, fan_out, 8, true));

    println!(
        "\n## Localized traversal scaling (#838, fan-out {fan_out}, 8 clustered seeds, warm index)\n"
    );
    println!(
        "| scale | edges | nodes | edge_filtered | edge_scanned | node_filtered | \
         node_scanned | *1..1 ms | *1..2 ms | *1..3 ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    print_row("1M", &small);
    print_row("10M", &large);
    println!(
        "\n_Generated by `cargo test -p gf-exec --release --test bench_traversal_scaling \
         -- --ignored --nocapture --test-threads=1`._\n"
    );

    // No full scan of either table on a Hit.
    assert_eq!(small.io.edge_full_reads, 0);
    assert_eq!(large.io.edge_full_reads, 0);
    assert_eq!(small.io.node_full_reads, 0);
    assert_eq!(large.io.node_full_reads, 0);
    // Rows materialized: identical across the 10× growth (same neighborhood).
    assert_eq!(small.io.edge_filtered_rows, large.io.edge_filtered_rows);
    assert_eq!(small.io.node_filtered_rows, large.io.node_filtered_rows);
    // Decode footprint: sub-linear — the rows the predicate scanned stay bounded
    // as the graph grows 10×, rather than tracking the total. This is the #838
    // latency win (a full read would scan every row, i.e. `nodes`).
    assert!(small.io.node_scanned_rows > 0, "the node read happened");
    assert!(
        large.io.node_scanned_rows <= 3 * small.io.node_scanned_rows.max(1),
        "localized node decode must stay bounded across 10× growth: {} (1M) vs {} (10M)",
        small.io.node_scanned_rows,
        large.io.node_scanned_rows
    );
    assert!(
        large.io.node_scanned_rows * 4 < u64::try_from(large.nodes).unwrap(),
        "localized node decode ({}) must be far below the total node count ({})",
        large.io.node_scanned_rows,
        large.nodes
    );
    assert!(
        large.io.edge_scanned_rows <= 3 * small.io.edge_scanned_rows.max(1),
        "localized edge decode must stay bounded across 10× growth: {} (1M) vs {} (10M)",
        small.io.edge_scanned_rows,
        large.io.edge_scanned_rows
    );
}

/// A traversal from many **scattered** seeds reaches node ids spanning the
/// whole table and edge ids spanning the permuted edge file. Exact dense node
/// row selection keeps node decode proportional to the reached set; edge decode
/// remains page-scan bound. Rows materialized stay neighborhood-proportional
/// and neither table uses a full-read fallback.
#[ignore = "scaling benchmark — run via `make bench-traversal` (release)"]
#[test]
fn scaling_scattered_node_ids_are_pruned() {
    let _g = GUARD.lock().unwrap();
    let fan_out = 16usize;
    let n1 = env_usize("GF_BENCH_N1", 62_500);
    let n2 = env_usize("GF_BENCH_N2", 625_000);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let small = rt.block_on(bench_scale(n1, fan_out, 64, false));
    let large = rt.block_on(bench_scale(n2, fan_out, 64, false));

    println!("\n## Scattered traversal (worst case: 64 spread seeds, permuted edges)\n");
    println!(
        "| scale | edges | nodes | edge_filtered | edge_scanned | node_filtered | \
         node_scanned | *1..1 ms | *1..2 ms | *1..3 ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    print_row("1M", &small);
    print_row("10M", &large);
    println!();

    // Rows materialized still identical (the #767 claim); no full reads.
    assert_eq!(small.io.edge_full_reads, 0);
    assert_eq!(large.io.edge_full_reads, 0);
    assert_eq!(small.io.node_full_reads, 0);
    assert_eq!(large.io.node_full_reads, 0);
    assert_eq!(small.io.edge_filtered_rows, large.io.edge_filtered_rows);
    assert_eq!(small.io.node_filtered_rows, large.io.node_filtered_rows);
    assert_eq!(small.io.node_dense_row_selection_reads, 1);
    assert_eq!(large.io.node_dense_row_selection_reads, 1);
    assert_eq!(small.io.node_metadata_fallbacks, 0);
    assert_eq!(large.io.node_metadata_fallbacks, 0);
    assert_eq!(small.io.node_validation_fallbacks, 0);
    assert_eq!(large.io.node_validation_fallbacks, 0);
    assert!(small.io.node_scanned_rows > 0);
    assert!(
        large.io.node_scanned_rows <= 3 * small.io.node_scanned_rows,
        "scattered node decode must stay bounded across 10x growth: {} vs {}",
        small.io.node_scanned_rows,
        large.io.node_scanned_rows
    );
    assert!(
        large.io.edge_scanned_rows > small.io.edge_scanned_rows,
        "permuted scattered edge ids remain page-scan bound"
    );
}
