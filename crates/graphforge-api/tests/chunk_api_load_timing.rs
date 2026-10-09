//! Timing driver for an initial load through the chunk API
//! (`GraphConstructionSession::append_*`). Not a gate: run it explicitly,
//! on a quiet host for numbers worth keeping.
//!
//! ```text
//! GF_CHUNK_SCALE=20 cargo test -p graphforge-api --release \
//!     --test chunk_api_load_timing -- --ignored --nocapture
//! ```
//!
//! It loads `2^scale` nodes and `16 * 2^scale` edges in `GF_CHUNK_ROWS`-row
//! chunks (default 65,536), publishes, reopens, and prints one line with the
//! accept and seal-and-publish times. The same file builds against an older
//! tree, so the line is comparable across revisions.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
};

/// A bijection on 64 bits, so identities are distinct and arrive unsorted.
fn scramble(index: u64) -> u128 {
    u128::from(index.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn node_uuid(index: u64) -> [u8; 16] {
    (0x1000_0000_0000_0000_0000_0000_0000_0000_u128 | scramble(index)).to_be_bytes()
}

fn edge_uuid(index: u64) -> [u8; 16] {
    (0x2000_0000_0000_0000_0000_0000_0000_0000_u128 | scramble(index)).to_be_bytes()
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "timing driver; run explicitly"]
fn chunk_api_load_timing() {
    let scale = env("GF_CHUNK_SCALE", 14);
    let rows = usize::try_from(env("GF_CHUNK_ROWS", 65_536)).unwrap();
    let nodes = 1_u64 << scale;
    let edges = nodes * 16;
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let forge = GraphForge::new(Some(path)).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: rows,
            max_run_records: 4 * rows,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();

    let accept = Instant::now();
    let mut chunks = 0_u64;
    for start in (0..nodes).step_by(rows) {
        let end = (start + rows as u64).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity((end - start) as usize, 16);
        for node in start..end {
            ids.append_value(node_uuid(node)).unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_NODE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; (end - start) as usize])),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
        chunks += 1;
    }
    for start in (0..edges).step_by(rows) {
        let end = (start + rows as u64).min(edges);
        let count = (end - start) as usize;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(count, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(count, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(count, 16);
        for edge in start..end {
            let source = scramble(edge) as u64 % nodes;
            let target = scramble(edge ^ 0x5555_5555) as u64 % nodes;
            ids.append_value(edge_uuid(edge)).unwrap();
            sources.append_value(node_uuid(source)).unwrap();
            targets.append_value(node_uuid(target)).unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; count])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
        chunks += 1;
    }
    let accept_ms = accept.elapsed().as_millis();

    let publish = Instant::now();
    session.seal_and_publish().unwrap();
    let publish_ms = publish.elapsed().as_millis();
    drop(session);
    drop(forge);

    let reopened = GraphForge::new(Some(path)).unwrap();
    let count = |query: &str| {
        reopened.execute(query).unwrap().batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };
    let counted_nodes = count("MATCH (n) RETURN count(n) AS n");
    let counted_edges = count("MATCH ()-[r]->() RETURN count(r) AS n");
    assert_eq!(counted_nodes as u64, nodes);
    assert_eq!(counted_edges as u64, edges);
    println!(
        "chunk_api_load scale={scale} nodes={nodes} edges={edges} chunks={chunks} \
         accept_ms={accept_ms} seal_publish_ms={publish_ms} total_ms={}",
        accept_ms + publish_ms
    );
}
