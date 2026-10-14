//! A hop decodes each ordinal identity block once per generation, however many
//! blocks its destinations span (#1984).
//!
//! The v4 ordinal identity authority authenticates its 64 KiB blocks on first
//! read and retains them under a byte budget. When that budget was a fixed
//! 1 MiB (16 blocks), a hop whose destinations span more blocks evicted each
//! block before it was wanted again, so every query decoded the whole
//! identity artifact again. The counters are exact and deterministic; the gate
//! never measures time.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Builder, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_exec::demand;

/// Bytes of one authenticated ordinal identity block.
const BLOCK_BYTES: u64 = 64 * 1024;
/// Node ordinals one block holds: 16-byte UUIDs.
const NODES_PER_BLOCK: usize = 4_096;
/// More blocks than the 16 a fixed 1 MiB cache held.
const BLOCKS: usize = 24;
const NODES: usize = BLOCKS * NODES_PER_BLOCK;
/// Rows per canonical file: the construction session's batch window.
const WINDOW: usize = 8_192;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// A ring: node `i` has one edge, to node `i + 1`. A full scan therefore names
/// every node as a destination exactly once, in ordinal order, so its
/// destinations sweep every identity block.
fn construct(dir: &Path) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(edge).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((edge + 1) % NODES).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

fn open(project: &Path) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Every node's destination UUID, from the ordinal identity authority.
const DESTINATIONS: &str = "MATCH (a:Entity)-[e:LINK]->(b) RETURN b.node_uuid AS id";

struct Counted {
    ids: Vec<Vec<u8>>,
    identity_bytes_read: u64,
    node_reads_started: u64,
}

fn run(forge: &GraphForge) -> Counted {
    let _capture = LifecycleIoCapture::install();
    let (result, snapshot) = demand::capture(|| forge.execute(DESTINATIONS));
    let result = result.expect("statement executes");
    let hop = snapshot.hops.values().next().expect("one hop");
    let mut ids = Vec::new();
    for batch in &result.batches {
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("id is a UUID");
        ids.extend((0..column.len()).map(|row| column.value(row).to_vec()));
    }
    ids.sort();
    Counted {
        ids,
        identity_bytes_read: hop.identity_bytes_read,
        node_reads_started: hop.node_reads_started,
    }
}

fn sorted_uuids(nodes: impl Iterator<Item = usize>) -> Vec<Vec<u8>> {
    let mut ids = nodes
        .map(|node| node_uuid(node).as_bytes().to_vec())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

/// A hop whose destinations span more blocks than a fixed 1 MiB cache holds
/// decodes each block once on the first query, and none on the next.
#[test]
fn a_hop_over_more_blocks_than_a_fixed_cache_holds_decodes_each_block_once() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    let forge = open(dir.path());

    let first = run(&forge);
    assert_eq!(first.ids, sorted_uuids(0..NODES));
    assert_eq!(first.node_reads_started, 0, "no node file backs the hop");
    assert_eq!(
        first.identity_bytes_read,
        BLOCKS as u64 * BLOCK_BYTES,
        "every block is decoded exactly once"
    );

    // Repeat queries on the same generation are served from the blocks the
    // handle already authenticated. A cache of 16 blocks swept in order by 24
    // evicts each block before it is wanted again and re-reads all 24.
    for _ in 0..2 {
        let again = run(&forge);
        assert_eq!(again.ids, first.ids);
        assert_eq!(again.identity_bytes_read, 0);
    }
}

/// A write is a new generation: its handle starts empty and decodes each
/// block once again, and a deleted ordinal answers as deleted, never from a
/// block the previous generation retained.
#[test]
fn a_new_generation_decodes_each_block_once_and_omits_deleted_nodes() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    let forge = open(dir.path());
    assert_eq!(run(&forge).identity_bytes_read, BLOCKS as u64 * BLOCK_BYTES);
    assert_eq!(run(&forge).identity_bytes_read, 0);

    // Node 9 and its two edges are gone: nothing reaches node 9 (from 8), and
    // node 10 loses its inbound edge (from 9).
    forge
        .execute("MATCH (n:Entity {ident: 9}) DETACH DELETE n")
        .expect("the delete executes");

    let after = run(&forge);
    assert_eq!(
        after.ids,
        sorted_uuids((0..NODES).filter(|node| *node != 9 && *node != 10))
    );
    assert_eq!(after.node_reads_started, 0);
    assert!(
        after.identity_bytes_read >= BLOCKS as u64 * BLOCK_BYTES,
        "the new generation re-reads every block: {}",
        after.identity_bytes_read
    );
    let bytes_for_generation = after.identity_bytes_read;
    assert_eq!(run(&forge).identity_bytes_read, 0, "{bytes_for_generation}");
}
