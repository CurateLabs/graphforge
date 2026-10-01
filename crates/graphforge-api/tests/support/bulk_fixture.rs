//! Test-only bulk Graph500-like fixtures published through the construction
//! path, shared by the fixed-hop scale gate and the #1688 candidate tests.

#![allow(dead_code, reason = "each test binary uses a subset")]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
};
use graphforge_core::uuid::Uuid;

/// Rows per construction batch.
pub(crate) const WRITE_WINDOW: usize = 32 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BulkFixtureEvidence {
    pub(crate) node_rows: usize,
    pub(crate) edge_rows: usize,
    pub(crate) node_batches: usize,
    pub(crate) edge_batches: usize,
    pub(crate) accepted_chunks: u64,
    pub(crate) input_rows: u64,
    pub(crate) peak_batch_rows: u64,
}

/// Construct scale fixtures through the same bounded Arrow publication path
/// used by ordinary high-volume ingestion. Scalar `GraphWriter::create_edge`
/// deliberately checks its in-flight topology window for duplicate UUIDs and
/// is therefore not a realistic bulk-ingestion primitive.
pub(crate) fn generate_bulk_graph(dir: &Path, nodes: usize, fan_out: usize) -> BulkFixtureEvidence {
    assert!(nodes > fan_out);
    let forge = GraphForge::new(Some(dir.to_str().expect("temp path is UTF-8"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();

    let mut node_batches = 0;
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(nodes);
        let rows = end - start;
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for node in start..end {
            identities
                .append_value(fixture_node_uuid(node).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_NODE_SCHEMA),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; rows])),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
        node_batches += 1;
    }

    let edge_rows = nodes.saturating_mul(fan_out);
    let mut edge_batches = 0;
    for start in (0..edge_rows).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(edge_rows);
        let rows = end - start;
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            let offset = edge % fan_out + 1;
            identities
                .append_value(fixture_edge_uuid(edge).as_bytes())
                .unwrap();
            sources
                .append_value(fixture_node_uuid(source).as_bytes())
                .unwrap();
            targets
                .append_value(fixture_node_uuid((source + offset) % nodes).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
        edge_batches += 1;
    }

    session.seal_and_publish().unwrap();
    let progress = session.progress();
    drop(session);
    forge.index_adjacency().unwrap();
    drop(forge);

    BulkFixtureEvidence {
        node_rows: nodes,
        edge_rows,
        node_batches,
        edge_batches,
        accepted_chunks: progress.accepted_chunks,
        input_rows: progress.evidence.input_rows,
        peak_batch_rows: progress.evidence.peak_batch_rows,
    }
}

pub(crate) fn fixture_node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1)
}

pub(crate) fn fixture_edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1)
}

/// Encoded node files under `dir` (construction staging included): the
/// published topology spans one file per 65,536-row window.
pub(crate) fn encoded_node_files(dir: &Path) -> usize {
    fn walk(path: &Path, found: &mut usize) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                walk(&child, found);
            } else if child.extension().is_some_and(|ext| ext == "parquet")
                && child
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "nodes")
                && child
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "topology")
            {
                *found += 1;
            }
        }
    }
    let mut found = 0;
    walk(dir, &mut found);
    found
}
