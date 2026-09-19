//! Node MERGE scaling correctness gate (#1400).
//!
//! The wall-clock scaling shape previously exercised here now runs through Divan
//! (`benches/merge_scaling.rs`, `make bench-merge-scaling`). This integration
//! test keeps the deterministic topology-read bound that must stay in product CI.

use std::path::Path;
use std::sync::{Arc, Mutex};

use graphforge_core::OntologyMode;
use graphforge_core::uuid::new_v7;
use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, RuntimeCatalog};
use graphforge_storage::{GraphCatalog, GraphWriter, io_stats};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

/// Serializes tests in this file that reset/read the process-global
/// [`io_stats`] counters.
static IO_STATS_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const TS: i64 = 1_700_000_000_000_000;
const PERSON: EntityTypeId = match EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};

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

async fn execute_merge_range(dir: &Path, rt: &Arc<Mutex<RuntimeCatalog>>, start: i64, end: i64) {
    let stmt =
        format!("UNWIND range({start}, {end}) AS i MERGE (m:Merged {{uid: i}}) RETURN count(m)");
    let plan = bind(&stmt, rt);
    let catalog = GraphCatalog::open(dir, None, &rt.lock().unwrap()).expect("open catalog");
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.to_path_buf(),
        OntologyMode::Exploratory,
    )
    .expect("session");
    session
        .execute_write_statement(&plan)
        .await
        .unwrap_or_else(|e| panic!("merge failed: {e}"));
}

/// Direct, noise-immune proof of the acceptance criterion: "a MERGE over N
/// rows performs a bounded number of topology reads, not N." Counts actual
/// `graphforge_storage::read_nodes` invocations via the process-global
/// [`io_stats`] `node_full_reads` counter.
#[tokio::test]
async fn node_merge_topology_read_count_is_bounded_not_per_row() {
    let _guard = IO_STATS_GUARD.lock().await;
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let dir = TempDir::new().unwrap();
    seed_filler_nodes(dir.path(), 500);

    io_stats::reset();
    execute_merge_range(dir.path(), &rt, 0, 0).await;
    let reads_for_one_row = io_stats::snapshot().node_full_reads;

    io_stats::reset();
    execute_merge_range(dir.path(), &rt, 1_000, 1_049).await;
    let reads_for_fifty_rows = io_stats::snapshot().node_full_reads;

    println!(
        "node_full_reads: {reads_for_one_row} for a 1-row MERGE, {reads_for_fifty_rows} for a 50-row MERGE"
    );
    assert_eq!(
        reads_for_fifty_rows, reads_for_one_row,
        "MERGE topology reads must not scale with row count: a 1-row MERGE did \
         {reads_for_one_row} read(s), a 50-row MERGE did {reads_for_fifty_rows}"
    );
}
