//! Test-only bulk Graph500-like fixtures published through the construction
//! path, shared by the fixed-hop scale gate and the #1688 candidate tests.

#![allow(dead_code, reason = "each test binary uses a subset")]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
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
    generate_bulk_graph_with_index(dir, nodes, fan_out, true)
}

/// [`generate_bulk_graph`] with the trailing `index_adjacency` optional.
/// That call publishes one more compact (V2) generation, so a fixture that must
/// stay exactly as the construction session published it (with its shipped
/// adjacency CSR) passes `false`.
pub(crate) fn generate_bulk_graph_with_index(
    dir: &Path,
    nodes: usize,
    fan_out: usize,
    index_adjacency: bool,
) -> BulkFixtureEvidence {
    generate(dir, nodes, fan_out, index_adjacency, Properties::None)
}

/// [`generate_bulk_graph_with_index`] without the trailing `index_adjacency`,
/// where every node carries a `name` ([`fixture_node_name`]) and, with
/// `edge_properties`, every edge a `note` ([`fixture_edge_note`]), so the
/// published generation holds property fragments that grow with the data.
pub(crate) fn generate_bulk_graph_with_properties(
    dir: &Path,
    nodes: usize,
    fan_out: usize,
    edge_properties: bool,
) -> BulkFixtureEvidence {
    let properties = if edge_properties {
        Properties::NodesAndEdges
    } else {
        Properties::Nodes
    };
    generate(dir, nodes, fan_out, false, properties)
}

/// The `name` property [`generate_bulk_graph_with_properties`] gives node
/// `index`: unique, and carrying 16 bytes of pseudo-random hex that a
/// compressor cannot remove, so the property payload grows with the nodes.
pub(crate) fn fixture_node_name(index: usize) -> String {
    let mix = |mut state: u64| {
        state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^ (state >> 31)
    };
    let seed = index as u64;
    format!(
        "entity-{index}-{:016x}{:016x}",
        mix(seed.wrapping_mul(2).wrapping_add(1)),
        mix(seed.wrapping_mul(2).wrapping_add(2))
    )
}

/// The `note` property [`generate_bulk_graph_with_properties`] gives edge `index`.
pub(crate) fn fixture_edge_note(index: usize) -> String {
    format!("link-{index}")
}

fn with_property(schema: &Schema, name: &str) -> Arc<Schema> {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Properties {
    None,
    Nodes,
    NodesAndEdges,
}

fn generate(
    dir: &Path,
    nodes: usize,
    fan_out: usize,
    index_adjacency: bool,
    properties: Properties,
) -> BulkFixtureEvidence {
    assert!(nodes > fan_out);
    let node_properties = properties != Properties::None;
    let edge_properties = properties == Properties::NodesAndEdges;
    let node_schema = if node_properties {
        with_property(&CONSTRUCTION_NODE_SCHEMA, "name")
    } else {
        Arc::clone(&CONSTRUCTION_NODE_SCHEMA)
    };
    let edge_schema = if edge_properties {
        with_property(&CONSTRUCTION_EDGE_SCHEMA, "note")
    } else {
        Arc::clone(&CONSTRUCTION_EDGE_SCHEMA)
    };
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
        let mut columns = vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; rows])),
        ];
        if node_properties {
            columns.push(Arc::new(StringArray::from_iter_values(
                (start..end).map(fixture_node_name),
            )));
        }
        let batch = RecordBatch::try_new(Arc::clone(&node_schema), columns).unwrap();
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
        let mut columns = vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; rows])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ];
        if edge_properties {
            columns.push(Arc::new(StringArray::from_iter_values(
                (start..end).map(fixture_edge_note),
            )));
        }
        let batch = RecordBatch::try_new(Arc::clone(&edge_schema), columns).unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
        edge_batches += 1;
    }

    session.seal_and_publish().unwrap();
    let progress = session.progress();
    drop(session);
    if index_adjacency {
        forge.index_adjacency().unwrap();
    }
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
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

pub(crate) fn fixture_edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
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
