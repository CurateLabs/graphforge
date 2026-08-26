//! Tests for the [`io_stats`](graphforge_storage::io_stats) read counters (#767): each
//! direct reader records the right category, the filtered-read fallback counts
//! as a *full* scan (not a cheap filtered read), and the empty-set short-circuit
//! records nothing.
//!
//! The counters are process-global, so every test serializes on `GUARD` and
//! `reset()`s under the lock before measuring. This is the binary's only set of
//! tests, so no other suite races these statics.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Mutex;

use tempfile::TempDir;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_storage::io_stats;
use graphforge_storage::{
    GraphWriter, read_edges, read_edges_filtered, read_nodes, read_nodes_filtered,
};

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

fn rewrite_node_ids(path: &Path, batches: &[RecordBatch], ids: Vec<u64>) {
    let schema = batches[0].schema();
    let node_id = schema.index_of("node_id").unwrap();
    let mut columns = batches[0].columns().to_vec();
    columns[node_id] = std::sync::Arc::new(UInt64Array::from(ids)) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .set_data_page_row_count_limit(2)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn dense_selection_uses_shard_local_id_range_and_gaps_fall_back() {
    let _g = GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    for _ in 0..8 {
        first.create_node(new_v7(), TypeId(0)).unwrap();
    }
    first.flush().unwrap();
    let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    for _ in 0..12 {
        second.create_node(new_v7(), TypeId(0)).unwrap();
    }
    second.flush().unwrap();
    let paths = graphforge_storage::topology_node_files(dir.path()).unwrap();
    assert_eq!(paths.len(), 2);
    let shard = paths[1].clone();
    let shard_batches = graphforge_storage::read_nodes(dir.path()).unwrap();
    let shard_batch = shard_batches.last().unwrap().clone();
    rewrite_node_ids(&shard, &[shard_batch.clone()], (9..=20).collect());
    std::fs::remove_file(&paths[0]).unwrap();

    io_stats::reset();
    let selected = read_nodes_filtered(dir.path(), &ids(&[8, 9, 14, 20, 21])).unwrap();
    let returned = selected
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(returned, [9, 14, 20]);
    let dense = io_stats::snapshot();
    assert_eq!(dense.node_dense_row_selection_reads, 1);
    assert_eq!(dense.node_exact_rows_selected, 3);
    assert_eq!(dense.node_metadata_fallbacks, 0);
    assert_eq!(dense.node_validation_fallbacks, 0);

    let mut gapped = (9..=20).collect::<Vec<_>>();
    gapped[5] = 99;
    rewrite_node_ids(&shard, &[shard_batch], gapped);
    io_stats::reset();
    let selected = read_nodes_filtered(dir.path(), &ids(&[9, 99])).unwrap();
    let mut returned = selected
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    returned.sort_unstable();
    assert_eq!(returned, [9, 99]);
    let fallback = io_stats::snapshot();
    assert_eq!(fallback.node_dense_row_selection_reads, 0);
    assert_eq!(fallback.node_row_group_predicate_reads, 1);
    assert_eq!(fallback.node_metadata_fallbacks, 1);
}
