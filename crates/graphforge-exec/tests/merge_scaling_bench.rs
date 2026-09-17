//! Node MERGE scaling benchmark (#1400).
//!
//! Before the fix, `run_merge_phase` called `find_matching_merge_nodes` once
//! per input row, and that function read the **entire** node topology from
//! disk (`graphforge_storage::read_nodes`) on every call — so a MERGE over a
//! fixed row count cost `rows * graph_size`. The sibling relationship-MERGE
//! path already hoisted its equivalent read once per statement; this test
//! pins the node path to the same shape: a MERGE over a fixed row count
//! should take roughly the same wall-clock time regardless of how large the
//! pre-existing graph is.
//!
//! `#[ignore]`d like `bench_traversal_scaling.rs` — run explicitly with
//! `cargo test -p graphforge-exec --release --test merge_scaling_bench -- --ignored --nocapture`.
//! Prints wall-clock numbers rather than asserting a hard bound, since
//! absolute timings depend on the host; the property under test is the
//! *shape* (flat vs. linear in graph size), which the printed ratio makes
//! visible.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::new_v7;
use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, RuntimeCatalog};
use graphforge_storage::{GraphCatalog, GraphWriter};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;
const PERSON: EntityTypeId = match EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};

/// Seed `n` plain `Person` nodes (no MERGE-matchable property), flushed once.
/// These exist purely to inflate the on-disk node topology the MERGE below
/// has to read past — none of them match the MERGE pattern used in the
/// benchmark, so every timing difference between graph sizes comes from the
/// topology (and property-partition) read, not from match-handling work.
fn seed_filler_nodes(dir: &Path, n: usize) {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Exploratory, TS).unwrap();
    for _ in 0..n {
        w.create_node(new_v7(), PERSON).unwrap();
    }
    w.flush().unwrap();
}

fn bind(query: &str, rt: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rt), OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder
        .bind(&ast)
        .unwrap_or_else(|e| panic!("bind {query:?}: {e:?}"))
}

async fn time_merge(
    dir: &Path,
    rt: &Arc<Mutex<RuntimeCatalog>>,
    rows: usize,
) -> std::time::Duration {
    let stmt = format!("UNWIND range(1, {rows}) AS i MERGE (m:Merged {{uid: i}}) RETURN count(m)");
    let plan = bind(&stmt, rt);
    let catalog = GraphCatalog::open(dir, None, &rt.lock().unwrap()).expect("open catalog");
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.to_path_buf(),
        OntologyMode::Exploratory,
    )
    .expect("session");
    let start = Instant::now();
    session
        .execute_write_statement(&plan)
        .await
        .unwrap_or_else(|e| panic!("merge failed: {e}"));
    start.elapsed()
}

/// Time a fixed-row-count MERGE against two graph sizes and print both, plus
/// their ratio, so the shape (flat vs. linear-in-graph-size) is visible from
/// the printed numbers.
#[tokio::test]
#[ignore = "wall-clock benchmark; run explicitly (see module docs)"]
async fn node_merge_does_not_scale_with_graph_size() {
    const ROWS: usize = 200;
    const SMALL: usize = 2_000;
    const LARGE: usize = 40_000;

    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let small_dir = TempDir::new().unwrap();
    seed_filler_nodes(small_dir.path(), SMALL);
    // Warm up (page cache, JIT-ish effects) with a tiny MERGE that isn't timed.
    let _ = time_merge(small_dir.path(), &rt, 1).await;
    let small_elapsed = time_merge(small_dir.path(), &rt, ROWS).await;

    let large_dir = TempDir::new().unwrap();
    seed_filler_nodes(large_dir.path(), LARGE);
    let _ = time_merge(large_dir.path(), &rt, 1).await;
    let large_elapsed = time_merge(large_dir.path(), &rt, ROWS).await;

    let ratio = large_elapsed.as_secs_f64() / small_elapsed.as_secs_f64().max(1e-9);
    println!(
        "MERGE {ROWS} rows over {SMALL} filler nodes: {:?}",
        small_elapsed
    );
    println!(
        "MERGE {ROWS} rows over {LARGE} filler nodes: {:?}",
        large_elapsed
    );
    println!(
        "graph size ratio {}x, wall-clock ratio {:.2}x",
        LARGE / SMALL,
        ratio
    );
}
