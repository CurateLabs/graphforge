//! Edge files whose `edge_id` column is dense are read by row ordinal.
//!
//! A canonical edge file holds a contiguous, ascending run of edge ids, so the
//! footer statistics prove the layout and an id is its own row ordinal. These
//! tests pin the contract of that read (#1984):
//!
//! - exact rows come back, only the requested projection is decoded, and the
//!   strategy the observer reports is the dense row selection;
//! - a file that does not carry the proof (gapped ids, no page index, rows that
//!   contradict the statistics) keeps the conservative predicate read and the
//!   same answer;
//! - the decision is made per file, so a request is never judged against the
//!   size of the whole route, and a file holding none of the ids costs its
//!   footer and nothing else;
//! - the bytes a fixed frontier reads do not grow with the relation.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, UInt64Array};
use datafusion::error::DataFusionError;
use graphforge_core::OntologyMode;
use graphforge_storage::io_stats::{
    self, FilteredReadObserver, FilteredReadPruning, FilteredReadStrategy, FilteredReadTable,
};
use graphforge_storage::lifecycle_io;
use graphforge_storage::{StorageIoPhase, TYPED_EDGE_SCHEMA};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;

#[derive(Default)]
struct EdgeObserver {
    events: Mutex<Events>,
}

#[derive(Default)]
struct Events {
    started: u64,
    completed: u64,
    scanned: u64,
    failed: u64,
    pruning: Vec<FilteredReadPruning>,
}

impl FilteredReadObserver for EdgeObserver {
    fn read_started(&self, table: FilteredReadTable) {
        assert_eq!(table, FilteredReadTable::Edge);
        self.events.lock().unwrap().started += 1;
    }

    fn rows_scanned(&self, table: FilteredReadTable, rows: u64) {
        assert_eq!(table, FilteredReadTable::Edge);
        self.events.lock().unwrap().scanned += rows;
    }

    fn read_completed(&self, table: FilteredReadTable, _rows: u64, _full: bool) {
        assert_eq!(table, FilteredReadTable::Edge);
        self.events.lock().unwrap().completed += 1;
    }

    fn read_failed(&self, _table: FilteredReadTable) {
        self.events.lock().unwrap().failed += 1;
    }

    fn pruning(&self, table: FilteredReadTable, pruning: FilteredReadPruning) {
        assert_eq!(table, FilteredReadTable::Edge);
        self.events.lock().unwrap().pruning.push(pruning);
    }
}

fn observer() -> (Arc<EdgeObserver>, Arc<dyn FilteredReadObserver>) {
    let observer = Arc::new(EdgeObserver::default());
    let erased: Arc<dyn FilteredReadObserver> = observer.clone();
    (observer, erased)
}

fn ids(values: &[u64]) -> HashSet<u64> {
    values.iter().copied().collect()
}

/// Write one typed edge shard of `KNOWS` holding exactly `edge_ids`, in the
/// given order, under the canonical `topology/edges/KNOWS/<first>-<last>` name.
fn write_shard(root: &Path, edge_ids: &[u64], properties: WriterProperties) {
    let first = *edge_ids.iter().min().unwrap();
    let last = *edge_ids.iter().max().unwrap();
    let directory = root.join("topology/edges/KNOWS");
    std::fs::create_dir_all(&directory).unwrap();
    let n = edge_ids.len();
    let uuids = |salt: u8| {
        let rows: Vec<Option<Vec<u8>>> = edge_ids
            .iter()
            .map(|id| {
                // Incompressible-looking identity bytes, so a read costs bytes.
                let mut bytes = vec![salt; 16];
                let mixed = id.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                bytes[..8].copy_from_slice(&mixed.to_be_bytes());
                bytes[8..].copy_from_slice(&mixed.rotate_left(29).to_be_bytes());
                Some(bytes)
            })
            .collect();
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(rows.into_iter(), 16).unwrap(),
        ) as ArrayRef
    };
    let batch = RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            uuids(1),
            uuids(2),
            uuids(3),
            Arc::new(UInt64Array::from(edge_ids.to_vec())),
            Arc::new(UInt64Array::from(
                edge_ids.iter().map(|id| id % 1_000).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                edge_ids.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![TS; n]).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    let file =
        std::fs::File::create(directory.join(format!("{first:020}-{last:020}.parquet"))).unwrap();
    let mut writer =
        ArrowWriter::try_new(file, TYPED_EDGE_SCHEMA.clone(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn inventory(root: &Path) -> Arc<graphforge_storage::AuthenticatedPropertyInventory> {
    graphforge_storage::GraphCatalog::open(root, None, &graphforge_ir::RuntimeCatalog::new())
        .unwrap()
        .admitted_inventory()
        .unwrap()
}

fn read(
    inventory: &graphforge_storage::AuthenticatedPropertyInventory,
    requested: &HashSet<u64>,
    observer: Option<&Arc<dyn FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    graphforge_storage::read_edges_filtered_observed_from_inventory(
        inventory,
        "KNOWS",
        OntologyMode::Strict,
        requested,
        observer,
    )
}

fn edge_ids_of(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("edge_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn small_groups() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build()
}

#[test]
fn dense_edge_ids_select_exact_rows_across_retained_groups() {
    let dir = TempDir::new().unwrap();
    write_shard(dir.path(), &(1..=25).collect::<Vec<_>>(), small_groups());
    let (events, erased) = observer();

    let batches = read(&inventory(dir.path()), &ids(&[2, 12, 25]), Some(&erased)).unwrap();

    assert_eq!(edge_ids_of(&batches), vec![2, 12, 25]);
    assert_eq!(batches[0].schema().fields(), TYPED_EDGE_SCHEMA.fields());
    let events = events.events.lock().unwrap();
    assert_eq!((events.started, events.completed, events.failed), (1, 1, 0));
    assert_eq!(events.scanned, 3, "only the selected rows decode");
    assert_eq!(events.pruning.len(), 1);
    let pruning = events.pruning[0];
    assert_eq!(pruning.strategy, FilteredReadStrategy::DenseRowSelection);
    assert_eq!(pruning.row_groups_considered, 5);
    assert_eq!(pruning.row_groups_selected, 3);
    assert_eq!(pruning.exact_rows_selected, 3);
    assert_eq!(pruning.metadata_fallbacks, 0);
    assert_eq!(pruning.validation_fallbacks, 0);
}

#[test]
fn a_projected_dense_read_decodes_only_the_demanded_columns_and_the_key() {
    let dir = TempDir::new().unwrap();
    write_shard(dir.path(), &(1..=25).collect::<Vec<_>>(), small_groups());
    let (events, erased) = observer();

    // Column 0 is edge_uuid; the key is added by the reader.
    let batches = graphforge_storage::read_edges_filtered_projected_from_inventory(
        &inventory(dir.path()),
        "KNOWS",
        OntologyMode::Strict,
        &ids(&[4, 5, 6, 20]),
        &[0],
        Some(&erased),
    )
    .unwrap();

    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["edge_uuid", "edge_id"]);
    assert_eq!(edge_ids_of(&batches), vec![4, 5, 6, 20]);
    let events = events.events.lock().unwrap();
    assert_eq!(
        events.pruning[0].strategy,
        FilteredReadStrategy::DenseRowSelection
    );
    assert_eq!(events.pruning[0].exact_rows_selected, 4);
}

#[test]
fn gapped_edge_ids_fail_closed_to_the_predicate_reader() {
    let dir = TempDir::new().unwrap();
    write_shard(
        dir.path(),
        &[1, 2, 4, 5],
        WriterProperties::builder().build(),
    );
    let (events, erased) = observer();

    let batches = read(&inventory(dir.path()), &ids(&[2, 4]), Some(&erased)).unwrap();

    assert_eq!(edge_ids_of(&batches), vec![2, 4]);
    let events = events.events.lock().unwrap();
    assert_eq!(
        events.pruning.len(),
        1,
        "one physical read, no dense attempt"
    );
    assert_eq!(
        events.pruning[0].strategy,
        FilteredReadStrategy::RowGroupPredicate
    );
}

#[test]
fn a_file_without_a_page_index_keeps_the_predicate_reader() {
    let dir = TempDir::new().unwrap();
    write_shard(
        dir.path(),
        &(1..=20).collect::<Vec<_>>(),
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let (events, erased) = observer();

    let batches = read(&inventory(dir.path()), &ids(&[3, 17]), Some(&erased)).unwrap();

    assert_eq!(edge_ids_of(&batches), vec![3, 17]);
    assert_eq!(
        events.events.lock().unwrap().pruning[0].strategy,
        FilteredReadStrategy::RowGroupPredicate
    );
}

#[test]
fn rows_that_contradict_the_statistics_are_discarded_before_the_predicate_retry() {
    let dir = TempDir::new().unwrap();
    // One page whose statistics read 1..=4 over four rows, but row 2 holds id 3:
    // the ordinal of id 2 lands on the wrong row.
    write_shard(
        dir.path(),
        &[1, 3, 2, 4],
        WriterProperties::builder().build(),
    );
    let (events, erased) = observer();

    let batches = read(&inventory(dir.path()), &ids(&[2]), Some(&erased)).unwrap();

    assert_eq!(edge_ids_of(&batches), vec![2]);
    let events = events.events.lock().unwrap();
    assert_eq!((events.started, events.completed, events.failed), (2, 2, 0));
    assert_eq!(events.pruning.len(), 2);
    assert_eq!(
        events.pruning[0].strategy,
        FilteredReadStrategy::DenseRowSelection
    );
    assert_eq!(events.pruning[0].validation_fallbacks, 1);
    assert_eq!(
        events.pruning[1].strategy,
        FilteredReadStrategy::RowGroupPredicate
    );
}

/// The request is judged per file. Eight ids are more than half of a ten-row
/// file when counted against the whole route, but no file holds more than four.
#[test]
fn a_request_is_judged_per_file_not_against_the_whole_route() {
    let dir = TempDir::new().unwrap();
    write_shard(
        dir.path(),
        &(1..=10).collect::<Vec<_>>(),
        WriterProperties::builder().build(),
    );
    write_shard(
        dir.path(),
        &(11..=20).collect::<Vec<_>>(),
        WriterProperties::builder().build(),
    );
    write_shard(
        dir.path(),
        &(21..=30).collect::<Vec<_>>(),
        WriterProperties::builder().build(),
    );
    let inventory = inventory(dir.path());
    let (events, erased) = observer();
    let _capture = io_stats::CaptureScope::install();
    io_stats::reset();

    let batches = read(
        &inventory,
        &ids(&[1, 2, 3, 4, 11, 12, 13, 14]),
        Some(&erased),
    )
    .unwrap();

    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3, 4, 11, 12, 13, 14]);
    let stats = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(
        stats.edge_full_reads, 0,
        "no file was read in full: {stats:?}"
    );
    let events = events.events.lock().unwrap();
    assert_eq!(events.pruning.len(), 3);
    for pruning in &events.pruning {
        assert_eq!(pruning.strategy, FilteredReadStrategy::DenseRowSelection);
    }
    let selected: Vec<u64> = events
        .pruning
        .iter()
        .map(|pruning| pruning.exact_rows_selected)
        .collect();
    assert_eq!(
        selected,
        vec![4, 4, 0],
        "the third file holds none of the ids"
    );
    assert_eq!(events.pruning[2].row_groups_selected, 0);
}

#[test]
fn a_request_covering_most_of_one_file_is_still_read_in_full() {
    let dir = TempDir::new().unwrap();
    write_shard(
        dir.path(),
        &(1..=10).collect::<Vec<_>>(),
        WriterProperties::builder().build(),
    );
    let _capture = io_stats::CaptureScope::install();
    io_stats::reset();

    let batches = read(
        &inventory(dir.path()),
        &ids(&[1, 2, 3, 4, 5, 6, 7, 8]),
        None,
    )
    .unwrap();

    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    assert_eq!(
        got,
        (1..=8).collect::<Vec<_>>(),
        "the fallback must not widen the result"
    );
    let stats = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(stats.edge_full_reads, 1);
    assert_eq!(stats.edge_full_rows, 10);
}

const SHARD_ROWS: u64 = 4_096;

/// Build a route of `shards` full shards.
fn route(root: &Path, shards: u64) {
    for shard in 0..shards {
        let first = shard * SHARD_ROWS + 1;
        write_shard(
            root,
            &(first..first + SHARD_ROWS).collect::<Vec<_>>(),
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1_024))
                .set_data_page_row_count_limit(256)
                .build(),
        );
    }
}

/// Ids spread over the first two shards, three rows in every eight, so that the
/// whole request (3,072 ids) is more than half of one shard while each shard holds 1,536.
fn fixed_frontier() -> HashSet<u64> {
    (1..=2 * SHARD_ROWS).filter(|id| id % 8 < 3).collect()
}

fn bytes_read(root: &Path, requested: &HashSet<u64>) -> (u64, Vec<u64>) {
    let inventory = inventory(root);
    let _capture = lifecycle_io::CaptureScope::install();
    let before = lifecycle_io::snapshot().expect("requested observation");
    let batches = read(&inventory, requested, None).expect("edge read");
    let delta = lifecycle_io::snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    let mut got = edge_ids_of(&batches);
    got.sort_unstable();
    (delta.phases[&StorageIoPhase::ReadPathScan].read_bytes, got)
}

/// A fixed frontier reads what it selects, not the relation: quadrupling the
/// shards adds only the footers of the shards that hold none of the ids.
#[test]
fn the_bytes_a_fixed_frontier_reads_do_not_grow_with_the_relation() {
    let requested = fixed_frontier();
    let mut sizes = Vec::new();
    for shards in [4, 8, 16] {
        let dir = TempDir::new().unwrap();
        route(dir.path(), shards);
        let (bytes, got) = bytes_read(dir.path(), &requested);
        let mut expected: Vec<u64> = requested.iter().copied().collect();
        expected.sort_unstable();
        assert_eq!(got, expected, "{shards} shards return exactly the frontier");
        eprintln!("shards={shards} read_bytes={bytes}");
        sizes.push((shards, bytes));
    }
    // A shard outside the id range costs its footer and page index. That is
    // metadata, a few KiB; reading the shard in full is ~300 KiB.
    let (_, small) = sizes[0];
    for (shards, bytes) in &sizes[1..] {
        let added = shards - 4;
        assert!(
            *bytes <= small + added * 16 * 1024,
            "reads grew with the relation: {sizes:?}"
        );
    }
}
