//! #1987: a query whose intermediate state scales with the data stays within
//! its memory budget, or fails typed naming the consumer, through the real
//! facade. The fixture is a 4-regular ring (`i -> i+1..i+4`), so
//! `LINK*1..12` enumerates on the order of `4^12` paths per seed: the path
//! state cannot fit a 16 MiB pool and must not be held outside it.

#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, GraphForge,
    GraphForgeOptions, ResourcePolicyMode,
};

const NODES: usize = 256;
const DEGREE: usize = 4;
const BUDGET_BYTES: u64 = 16 * 1024 * 1024;
/// Labelled bound: resident growth during the query must stay within this
/// multiple of the pool budget. The pool charges the path state; the rest of
/// what the query touches (a 256-node, 1,024-edge graph, runtime buffers) is a
/// small constant, so 8x is generous yet finite. Unaccounted path state at
/// `4^12` paths per seed would exceed it by orders of magnitude.
const RSS_GROWTH_MULTIPLE: u64 = 8;

fn uuid(tag: u128, index: usize) -> graphforge_core::uuid::Uuid {
    graphforge_core::uuid::Uuid::from_u128((tag << 120) | (index as u128 + 1))
}

fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    pages * 4096
}

fn build_ring(forge: &GraphForge) {
    let mut construction = forge.begin_graph_construction(Default::default()).unwrap();
    let mut identities = FixedSizeBinaryBuilder::with_capacity(NODES, 16);
    for node in 0..NODES {
        identities
            .append_value(uuid(0x20, node).as_bytes())
            .unwrap();
    }
    let nodes = RecordBatch::try_new(
        CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; NODES])),
        ],
    )
    .unwrap();
    construction.append_nodes("nodes", &nodes).unwrap();

    let rows = NODES * DEGREE;
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for edge in 0..rows {
        let source = edge / DEGREE;
        let target = (source + edge % DEGREE + 1) % NODES;
        ids.append_value(uuid(0x21, edge).as_bytes()).unwrap();
        sources.append_value(uuid(0x20, source).as_bytes()).unwrap();
        targets.append_value(uuid(0x20, target).as_bytes()).unwrap();
    }
    let edges = RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; rows])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ],
    )
    .unwrap();
    construction.append_edges("edges", &edges).unwrap();
    construction.seal_and_publish().unwrap();
    forge.index_adjacency().unwrap();
}

#[test]
fn path_enumeration_beyond_the_budget_fails_typed_and_stays_within_the_rss_bound() {
    let project = tempfile::TempDir::new().unwrap();
    let forge = GraphForge::new_with_options(
        Some(project.path().to_str().expect("project path is UTF-8")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                memory_budget_bytes: Some(BUDGET_BYTES),
                tokio_worker_threads: Some(2),
                compute_threads: Some(1),
                io_concurrency: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    build_ring(&forge);

    let baseline = resident_bytes();
    let peak = Arc::new(AtomicU64::new(baseline));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (Arc::clone(&peak), Arc::clone(&done));
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(resident_bytes(), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let outcome = forge.execute("MATCH (a)-[:LINK*1..12]->(b) RETURN count(*) AS paths");
    done.store(true, Ordering::Relaxed);
    sampler.join().unwrap();

    let growth = peak.load(Ordering::Relaxed).saturating_sub(baseline);
    assert!(
        growth <= RSS_GROWTH_MULTIPLE * BUDGET_BYTES,
        "resident growth {growth} bytes exceeds {RSS_GROWTH_MULTIPLE}x the {BUDGET_BYTES} byte budget"
    );
    let message = outcome
        .expect_err("12-hop path enumeration over a 4-regular ring cannot fit 16 MiB")
        .to_string();
    assert!(
        message.contains("VarLenExpand path state") && message.contains("Resources exhausted"),
        "{message}"
    );
}
