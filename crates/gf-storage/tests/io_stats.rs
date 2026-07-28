//! Tests for the [`io_stats`](gf_storage::io_stats) read counters (#767): each
//! direct reader records the right category, the filtered-read fallback counts
//! as a *full* scan (not a cheap filtered read), and the empty-set short-circuit
//! records nothing.
//!
//! The counters are process-global, so every test serializes on `GUARD` and
//! `reset()`s under the lock before measuring. This is the binary's only set of
//! tests, so no other suite races these statics.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use tempfile::TempDir;

use gf_core::uuid::{Uuid, new_v7};
use gf_core::{OntologyMode, TypeId};
use gf_storage::io_stats;
use gf_storage::{GraphWriter, read_edges, read_edges_filtered, read_nodes, read_nodes_filtered};

const TS: i64 = 1_700_000_000_000_000;

/// Serializes access to the process-global counters across parallel tests.
static GUARD: Mutex<()> = Mutex::new(());

/// A KNOWS chain of `n` edges (and `n + 1` nodes) through the normal writer.
fn write_chain(dir: &Path, n: usize) -> Vec<u64> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..=n).map(|_| new_v7()).collect();
    for u in &uuids {
        w.create_node(*u, TypeId(0)).unwrap();
    }
    let mut edge_ids = Vec::new();
    for pair in uuids.windows(2) {
        edge_ids.push(
            w.create_edge(new_v7(), "KNOWS", &pair[0], &pair[1])
                .unwrap(),
        );
    }
    w.flush().unwrap();
    edge_ids
}

fn ids(v: &[u64]) -> HashSet<u64> {
    v.iter().copied().collect()
}

#[test]
fn read_edges_records_a_full_edge_read() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 20);

    io_stats::reset();
    read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.edge_full_reads, 1);
    assert_eq!(s.edge_full_rows, 20);
    assert_eq!(s.edge_filtered_reads, 0);
    assert_eq!(s.node_full_reads, 0);
}

#[test]
fn read_nodes_records_a_full_node_read() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 20); // 21 nodes

    io_stats::reset();
    read_nodes(dir.path()).unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.node_full_reads, 1);
    assert_eq!(s.node_full_rows, 21);
    assert_eq!(s.edge_full_reads, 0);
    assert_eq!(s.edge_filtered_reads, 0);
}

#[test]
fn filtered_pushdown_records_only_materialized_rows() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let all = write_chain(dir.path(), 20);

    // A small fraction (3 of 20) takes the predicate-pushdown path.
    io_stats::reset();
    read_edges_filtered(
        dir.path(),
        "KNOWS",
        OntologyMode::Strict,
        &ids(&[all[2], all[7], all[15]]),
    )
    .unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.edge_filtered_reads, 1);
    assert_eq!(
        s.edge_filtered_rows, 3,
        "only the 3 requested rows materialize"
    );
    assert_eq!(
        s.edge_full_reads, 0,
        "the pushdown path must not count as a full scan"
    );
}

#[test]
fn filtered_fallback_counts_as_a_full_scan() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let all = write_chain(dir.path(), 10);

    // Requesting >50% of the rows trips the fallback: it reads the whole file,
    // so it is recorded as a full read of the full row count — not a cheap
    // filtered read of the 8 requested rows.
    io_stats::reset();
    read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &ids(&all[..8])).unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.edge_full_reads, 1);
    assert_eq!(s.edge_full_rows, 10, "the fallback scanned the whole file");
    assert_eq!(s.edge_filtered_reads, 0);
}

#[test]
fn empty_id_set_records_no_read() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 5);

    io_stats::reset();
    read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &HashSet::new()).unwrap();
    assert_eq!(io_stats::snapshot(), io_stats::IoSnapshot::default());
}

#[test]
fn reset_then_snapshot_round_trips() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 3);

    read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
    assert_ne!(io_stats::snapshot(), io_stats::IoSnapshot::default());
    io_stats::reset();
    assert_eq!(io_stats::snapshot(), io_stats::IoSnapshot::default());
}

// --- read_nodes_filtered (#838): node-side mirror of the edge filtered read ---

#[test]
fn filtered_node_pushdown_records_only_materialized_rows() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 20); // 21 nodes, node_ids 1..=21 (monotonic)

    // A small fraction (3 of 21) takes the predicate-pushdown path.
    io_stats::reset();
    read_nodes_filtered(dir.path(), &ids(&[3, 7, 15])).unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.node_filtered_reads, 1);
    assert_eq!(
        s.node_filtered_rows, 3,
        "only the 3 requested node rows materialize"
    );
    assert_eq!(
        s.node_full_reads, 0,
        "pushdown must not count as a full node scan"
    );
    assert_eq!(
        s.edge_full_reads, 0,
        "node read must not touch edge counters"
    );
    assert_eq!(s.node_scanned_rows, 3, "only exact row ordinals decode");
    assert_eq!(s.node_dense_row_selection_reads, 1);
    assert_eq!(s.node_row_group_predicate_reads, 0);
    assert_eq!(s.node_row_groups_considered, 1);
    assert_eq!(s.node_row_groups_selected, 1);
    assert_eq!(s.node_exact_rows_selected, 3);
    assert_eq!(s.node_metadata_fallbacks, 0);
    assert_eq!(s.node_validation_fallbacks, 0);
}

#[test]
fn filtered_node_fallback_counts_as_a_full_scan() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 10); // 11 nodes

    // Requesting >50% of the rows trips the fallback: full read, full row count.
    io_stats::reset();
    read_nodes_filtered(dir.path(), &ids(&[1, 2, 3, 4, 5, 6, 7])).unwrap();
    let s = io_stats::snapshot();
    assert_eq!(s.node_full_reads, 1);
    assert_eq!(
        s.node_full_rows, 11,
        "the fallback scanned the whole node file"
    );
    assert_eq!(s.node_filtered_reads, 0);
}

#[test]
fn filtered_node_empty_set_records_no_read() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 5);

    io_stats::reset();
    read_nodes_filtered(dir.path(), &HashSet::new()).unwrap();
    assert_eq!(io_stats::snapshot(), io_stats::IoSnapshot::default());
}
