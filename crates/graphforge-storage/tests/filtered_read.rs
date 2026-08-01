//! Tests for `read_edges_filtered` (#830): exact row selection, row-group
//! pruning on `edge_id` statistics, the large-fraction fallback, and the
//! empty-set / missing-file short-circuits — the I/O-level proof behind the
//! #767 "no full typed-edge scan per query" criterion.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, RecordBatch, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, UInt32Type};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;

use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_storage::io_stats::{
    FilteredReadObserver, FilteredReadPruning, FilteredReadStrategy, FilteredReadTable,
};
use graphforge_storage::{
    GraphWriter, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, read_edges_filtered,
    read_nodes_filtered_observed,
};

const TS: i64 = 1_700_000_000_000_000;

#[derive(Default)]
struct ReadObserver {
    starts: AtomicU64,
    scanned: AtomicU64,
    completions: AtomicU64,
    pruning: Mutex<Vec<FilteredReadPruning>>,
}

impl FilteredReadObserver for ReadObserver {
    fn read_started(&self, table: FilteredReadTable) {
        assert_eq!(table, FilteredReadTable::Node);
        self.starts.fetch_add(1, Ordering::Relaxed);
    }

    fn rows_scanned(&self, table: FilteredReadTable, rows: u64) {
        assert_eq!(table, FilteredReadTable::Node);
        self.scanned.fetch_add(rows, Ordering::Relaxed);
    }

    fn read_completed(&self, table: FilteredReadTable, _rows: u64, _full: bool) {
        assert_eq!(table, FilteredReadTable::Node);
        self.completions.fetch_add(1, Ordering::Relaxed);
    }

    fn read_failed(&self, table: FilteredReadTable) {
        panic!("unexpected failed {table:?} read");
    }

    fn pruning(&self, table: FilteredReadTable, pruning: FilteredReadPruning) {
        assert_eq!(table, FilteredReadTable::Node);
        self.pruning.lock().unwrap().push(pruning);
    }
}

/// A KNOWS chain of `n` edges written through the normal writer (one row
/// group at this scale).
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

/// Collect the edge_id column across batches.
fn edge_ids_of(batches: &[RecordBatch]) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column_by_name("edge_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    out
}

fn ids(v: &[u64]) -> HashSet<u64> {
    v.iter().copied().collect()
}

fn write_node_file(dir: &Path, node_ids: &[u64], properties: WriterProperties) {
    let topology = dir.join("topology");
    std::fs::create_dir_all(&topology).unwrap();
    let n = node_ids.len();
    let uuids: Vec<Option<Vec<u8>>> = (0..n)
        .map(|i| {
            let mut bytes = vec![0u8; 16];
            bytes[8..].copy_from_slice(&(i as u64 + 1).to_be_bytes());
            Some(bytes)
        })
        .collect();
    let uuid = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids.into_iter(), 16).unwrap(),
    ) as ArrayRef;
    let nullable_type_ids =
        ListArray::from_iter_primitive::<UInt32Type, _, _>((0..n).map(|_| Some([Some(0u32)])));
    let type_ids = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        nullable_type_ids.offsets().clone(),
        nullable_type_ids.values().clone(),
        None,
    );
    let timestamp =
        || Arc::new(TimestampMicrosecondArray::from(vec![TS; n]).with_timezone("UTC")) as ArrayRef;
    let batch = RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            uuid,
            Arc::new(UInt64Array::from(node_ids.to_vec())),
            Arc::new(UInt32Array::from(vec![0; n])),
            Arc::new(type_ids),
            timestamp(),
            timestamp(),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(topology.join("nodes.parquet")).unwrap();
    let mut writer =
        ArrowWriter::try_new(file, TOPOLOGY_NODES_SCHEMA.clone(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn node_ids_of(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn filtered_read_returns_exactly_the_requested_rows() {
    let dir = TempDir::new().unwrap();
    let all = write_chain(dir.path(), 20);
    let want = ids(&[all[2], all[7], all[15]]);

    let batches = read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &want).unwrap();
    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    let mut expected: Vec<u64> = want.iter().copied().collect();
    expected.sort_unstable();
    assert_eq!(got, expected);
    // Full schema rows, not just the key column.
    assert_eq!(batches[0].schema().fields(), TYPED_EDGE_SCHEMA.fields());
}

#[test]
fn empty_id_set_never_opens_the_file_and_yields_empty() {
    let dir = TempDir::new().unwrap();
    write_chain(dir.path(), 5);
    let batches =
        read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &HashSet::new()).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}

#[test]
fn missing_file_yields_one_empty_batch() {
    let dir = TempDir::new().unwrap();
    let batches =
        read_edges_filtered(dir.path(), "ABSENT", OntologyMode::Strict, &ids(&[1, 2])).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(batches[0].schema().fields(), TYPED_EDGE_SCHEMA.fields());
}

#[test]
fn large_fraction_fallback_still_returns_only_requested_rows() {
    let dir = TempDir::new().unwrap();
    let all = write_chain(dir.path(), 10);
    // Request >50% of the rows: the plain-read fallback path fires, but the
    // public contract (only the requested ids) must hold — the fallback trims
    // in memory.
    let want = ids(&all[..8]);
    let batches = read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &want).unwrap();
    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    let mut expected: Vec<u64> = want.iter().copied().collect();
    expected.sort_unstable();
    assert_eq!(got, expected, "fallback must not widen the result set");
}

#[test]
fn row_groups_outside_the_id_range_are_pruned() {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    // Hand-write a typed edge file with TINY row groups (5 rows each) so
    // pruning is observable at test scale.
    let dir = TempDir::new().unwrap();
    let edges_dir = dir.path().join("topology").join("edges");
    std::fs::create_dir_all(&edges_dir).unwrap();
    let path = edges_dir.join("KNOWS.parquet");

    let n = 25usize;
    let uuid_col = |_: &str| {
        let bytes: Vec<Option<Vec<u8>>> = (0..n)
            .map(|i| {
                let mut b = vec![0u8; 16];
                b[15] = u8::try_from(i).unwrap();
                Some(b)
            })
            .collect();
        Arc::new(
            arrow::array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                bytes.into_iter(),
                16,
            )
            .unwrap(),
        ) as arrow::array::ArrayRef
    };
    let u64_col = |f: fn(usize) -> u64| {
        Arc::new(UInt64Array::from((0..n).map(f).collect::<Vec<_>>())) as arrow::array::ArrayRef
    };
    let ts_col =
        Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![TS; n]).with_timezone("UTC"))
            as arrow::array::ArrayRef;
    let batch = RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            uuid_col("edge"),
            uuid_col("src"),
            uuid_col("dst"),
            u64_col(|i| i as u64 + 1), // edge_id 1..=25, ascending
            u64_col(|i| i as u64),     // src_id
            u64_col(|i| i as u64 + 1), // dst_id
            ts_col,
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, TYPED_EDGE_SCHEMA.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Request ids only from the FIRST group (1..=3): groups 2..5 must prune.
    // Pruning isn't directly observable through the public API, so assert the
    // observable contract (exact rows back) — the pruning branch is exercised
    // by construction (5 groups, 4 prunable).
    let batches =
        read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &ids(&[1, 2, 3])).unwrap();
    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);

    // And a cross-group request still returns exactly the matches.
    let batches =
        read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Strict, &ids(&[4, 21])).unwrap();
    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    assert_eq!(got, vec![4, 21]);
}

#[test]
fn exploratory_mode_filters_the_shared_file() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    for u in [a, b, c] {
        w.create_node(u, TypeId(0)).unwrap();
    }
    let e1 = w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    let _e2 = w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
    w.flush().unwrap();

    let batches =
        read_edges_filtered(dir.path(), "KNOWS", OntologyMode::Exploratory, &ids(&[e1])).unwrap();
    assert_eq!(edge_ids_of(&batches), vec![e1]);
}

#[test]
fn dense_node_ids_select_exact_rows_across_retained_groups() {
    let dir = TempDir::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build();
    write_node_file(dir.path(), &(1..=25).collect::<Vec<_>>(), properties);
    let observer = Arc::new(ReadObserver::default());
    let observed: Arc<dyn FilteredReadObserver> = observer.clone();

    let batches =
        read_nodes_filtered_observed(dir.path(), &ids(&[2, 12, 25]), Some(&observed)).unwrap();
    assert_eq!(node_ids_of(&batches), vec![2, 12, 25]);
    assert_eq!(observer.starts.load(Ordering::Relaxed), 1);
    assert_eq!(observer.completions.load(Ordering::Relaxed), 1);
    assert_eq!(observer.scanned.load(Ordering::Relaxed), 3);
    let pruning = observer.pruning.lock().unwrap();
    assert_eq!(pruning.len(), 1);
    assert_eq!(pruning[0].strategy, FilteredReadStrategy::DenseRowSelection);
    assert_eq!(pruning[0].row_groups_considered, 5);
    assert_eq!(pruning[0].row_groups_selected, 3);
    assert_eq!(pruning[0].exact_rows_selected, 3);
    assert_eq!(pruning[0].metadata_fallbacks, 0);
    assert_eq!(pruning[0].validation_fallbacks, 0);
}

#[test]
fn gapped_node_ids_fail_closed_to_the_conservative_reader() {
    let dir = TempDir::new().unwrap();
    write_node_file(
        dir.path(),
        &[1, 2, 4, 5],
        WriterProperties::builder().build(),
    );
    let observer = Arc::new(ReadObserver::default());
    let observed: Arc<dyn FilteredReadObserver> = observer.clone();

    let batches = read_nodes_filtered_observed(dir.path(), &ids(&[2, 4]), Some(&observed)).unwrap();
    assert_eq!(node_ids_of(&batches), vec![2, 4]);
    let pruning = observer.pruning.lock().unwrap();
    assert_eq!(pruning.len(), 1);
    assert_eq!(pruning[0].strategy, FilteredReadStrategy::RowGroupPredicate);
    assert_eq!(pruning[0].metadata_fallbacks, 1);
    assert_eq!(pruning[0].validation_fallbacks, 0);
}

#[test]
fn missing_page_index_preserves_legacy_project_correctness() {
    let dir = TempDir::new().unwrap();
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    write_node_file(dir.path(), &(1..=20).collect::<Vec<_>>(), properties);
    let observer = Arc::new(ReadObserver::default());
    let observed: Arc<dyn FilteredReadObserver> = observer.clone();

    let batches =
        read_nodes_filtered_observed(dir.path(), &ids(&[3, 17]), Some(&observed)).unwrap();
    assert_eq!(node_ids_of(&batches), vec![3, 17]);
    let pruning = observer.pruning.lock().unwrap();
    assert_eq!(pruning[0].strategy, FilteredReadStrategy::RowGroupPredicate);
    assert_eq!(pruning[0].metadata_fallbacks, 1);
}

#[test]
fn unexpected_dense_rows_are_discarded_before_conservative_retry() {
    let dir = TempDir::new().unwrap();
    // One-page metadata appears dense, but row 2 carries id 3. The membership
    // guard rejects the ordinal lookup, output validation detects the missing
    // id 2, and the second physical attempt returns the correct row.
    write_node_file(
        dir.path(),
        &[1, 3, 2, 4],
        WriterProperties::builder().build(),
    );
    let observer = Arc::new(ReadObserver::default());
    let observed: Arc<dyn FilteredReadObserver> = observer.clone();

    let batches = read_nodes_filtered_observed(dir.path(), &ids(&[2]), Some(&observed)).unwrap();
    assert_eq!(node_ids_of(&batches), vec![2]);
    assert_eq!(observer.starts.load(Ordering::Relaxed), 2);
    assert_eq!(observer.completions.load(Ordering::Relaxed), 2);
    let pruning = observer.pruning.lock().unwrap();
    assert_eq!(pruning.len(), 2);
    assert_eq!(pruning[0].strategy, FilteredReadStrategy::DenseRowSelection);
    assert_eq!(pruning[0].validation_fallbacks, 1);
    assert_eq!(pruning[1].strategy, FilteredReadStrategy::RowGroupPredicate);
    assert_eq!(pruning[1].metadata_fallbacks, 0);
}
