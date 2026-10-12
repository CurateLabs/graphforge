use super::super::read_parquet_or_empty;
use super::super::tests::Wave12Observer;
use super::super::tests::edge_rel_pairs;
use super::super::tests::write_nodes_parquet_value;
use super::super::tests::write_typed_edge;
use super::super::total_rows;
use super::FilteredReadKind;
use super::FilteredReadObservation;
use super::filtered_keys_match;
use super::read_edges_filtered;
use super::read_edges_filtered_projected_observed;
use super::read_nodes_filtered_projected_observed;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use graphforge_core::OntologyMode;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn projected_node_reader_keeps_only_demand_and_join_key() {
    let dir = TempDir::new().unwrap();
    write_nodes_parquet_value(&dir.path().join("topology/nodes.parquet"), 1, 1);
    let ids = std::collections::HashSet::from([1]);
    let batches = read_nodes_filtered_projected_observed(
        dir.path(),
        &ids,
        &[TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap()],
        None,
    )
    .unwrap();
    assert_eq!(total_rows(&batches), 1);
    assert_eq!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "node_id"]
    );
}

#[test]
fn read_edges_filtered_strict_wildcard_unions_traversed_ids() {
    let dir = TempDir::new().unwrap();
    let edges = dir.path().join("topology").join("edges");
    write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
    write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

    let want: std::collections::HashSet<u64> = [2].into_iter().collect();
    let one = read_edges_filtered(dir.path(), "*", OntologyMode::Strict, &want).unwrap();
    assert_eq!(edge_rel_pairs(&one), vec![(2, "OWNS".to_owned())]);

    let both: std::collections::HashSet<u64> = [1, 2].into_iter().collect();
    let two = read_edges_filtered(dir.path(), "*", OntologyMode::Strict, &both).unwrap();
    assert_eq!(
        edge_rel_pairs(&two),
        vec![(1, "KNOWS".to_owned()), (2, "OWNS".to_owned())]
    );

    let projected = read_edges_filtered_projected_observed(
        dir.path(),
        "*",
        OntologyMode::Strict,
        &want,
        &[EXPLORATORY_EDGE_SCHEMA.index_of("rel_type_name").unwrap()],
        None,
    )
    .unwrap();
    assert_eq!(total_rows(&projected), 1);
    assert_eq!(
        projected[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["edge_id", "rel_type_name"]
    );
}

#[test]
fn projected_edge_read_rejects_reordered_physical_schema() {
    let dir = TempDir::new().unwrap();
    let path = dir
        .path()
        .join("topology")
        .join("edges")
        .join("KNOWS.parquet");
    write_typed_edge(&path, 7, 1, 2);
    let batches = read_parquet_or_empty(&path, TYPED_EDGE_SCHEMA.clone()).unwrap();
    let order = [3, 0, 1, 2, 4, 5, 6];
    let reordered = batches[0].project(&order).unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, reordered.schema(), None).unwrap();
    writer.write(&reordered).unwrap();
    writer.close().unwrap();

    let ids = [7].into_iter().collect();
    let error = read_edges_filtered_projected_observed(
        dir.path(),
        "KNOWS",
        OntologyMode::Strict,
        &ids,
        &[TYPED_EDGE_SCHEMA.index_of("src_id").unwrap()],
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("requires canonical schema"));
}

#[test]
fn wave12_filtered_key_validation_rejects_missing_wrong_null_and_duplicate_ids() {
    let missing = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::UInt64,
        false,
    )])));
    assert!(!filtered_keys_match(
        &[missing],
        "node_id",
        &Default::default()
    ));

    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "node_id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["1"]))],
    )
    .unwrap();
    assert!(!filtered_keys_match(
        &[wrong],
        "node_id",
        &[1].into_iter().collect()
    ));

    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "node_id",
            DataType::UInt64,
            true,
        )])),
        vec![Arc::new(UInt64Array::from(vec![Some(1), None]))],
    )
    .unwrap();
    assert!(!filtered_keys_match(
        &[nullable],
        "node_id",
        &[1].into_iter().collect()
    ));

    let duplicate = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "node_id",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![1, 1]))],
    )
    .unwrap();
    assert!(!filtered_keys_match(
        &[duplicate],
        "node_id",
        &[1].into_iter().collect()
    ));
}

#[test]
fn wave12_filtered_observation_reports_completion_or_failure_once() {
    let observer = Arc::new(Wave12Observer::default());
    {
        let erased: Arc<dyn crate::io_stats::FilteredReadObserver> = observer.clone();
        let mut observation = FilteredReadObservation::new(Some(&erased), FilteredReadKind::Node);
        observation.scanned(3);
        observation.pruning(crate::io_stats::FilteredReadPruning {
            strategy: crate::io_stats::FilteredReadStrategy::RowGroupPredicate,
            row_groups_considered: 1,
            row_groups_selected: 1,
            pages_considered: 1,
            pages_selected: 1,
            page_rows_selected: 1,
            exact_rows_selected: 1,
            metadata_fallbacks: 0,
            validation_fallbacks: 0,
        });
        observation.complete(2, false);
    }
    {
        let erased: Arc<dyn crate::io_stats::FilteredReadObserver> = observer.clone();
        let _failed = FilteredReadObservation::new(Some(&erased), FilteredReadKind::Edge);
    }
    assert_eq!(
        observer.started.load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        observer.scanned.load(std::sync::atomic::Ordering::Relaxed),
        3
    );
    assert_eq!(
        observer
            .completed
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        observer.failed.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        observer.pruning.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// Records the pruning report of every physical read it observes.
#[derive(Default)]
struct PruningCapture {
    reports: std::sync::Mutex<Vec<crate::io_stats::FilteredReadPruning>>,
}

impl crate::io_stats::FilteredReadObserver for PruningCapture {
    fn read_started(&self, _: crate::io_stats::FilteredReadTable) {}

    fn rows_scanned(&self, _: crate::io_stats::FilteredReadTable, _: u64) {}

    fn read_completed(&self, _: crate::io_stats::FilteredReadTable, _: u64, _: bool) {}

    fn read_failed(&self, _: crate::io_stats::FilteredReadTable) {}

    fn pruning(
        &self,
        _: crate::io_stats::FilteredReadTable,
        pruning: crate::io_stats::FilteredReadPruning,
    ) {
        self.reports.lock().unwrap().push(pruning);
    }
}

/// A canonical dense node file holding ids `1..=rows`, written with
/// `properties`, so its page size is the caller's.
fn write_dense_nodes(dir: &std::path::Path, rows: u64, properties: WriterProperties) {
    let path = dir.join(format!("topology/nodes/{:020}-{:020}.parquet", 1_u64, rows));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let ids: Vec<u64> = (1..=rows).collect();
    let count = ids.len();
    let uuids = FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| {
        let mut bytes = vec![0_u8; 16];
        bytes[8..].copy_from_slice(&id.to_be_bytes());
        bytes
    }))
    .unwrap();
    let timestamps = TimestampMicrosecondArray::from(vec![0_i64; count])
        .with_timezone_opt(Some(Arc::from("UTC")));
    let labels = arrow::array::ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        arrow::buffer::OffsetBuffer::from_lengths(std::iter::repeat_n(1, count)),
        Arc::new(UInt32Array::from(vec![0_u32; count])),
        None,
    );
    let batch = RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(uuids),
            Arc::new(UInt64Array::from(ids)),
            Arc::new(UInt32Array::from(vec![0_u32; count])),
            Arc::new(labels),
            Arc::new(timestamps.clone()),
            Arc::new(timestamps),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        TOPOLOGY_NODES_SCHEMA.clone(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Read `wanted` from a dense node file, returning the ids that came back and
/// the one pruning report of the physical attempt.
fn read_dense_nodes(
    dir: &std::path::Path,
    wanted: &[u64],
) -> (Vec<u64>, crate::io_stats::FilteredReadPruning) {
    let capture = Arc::new(PruningCapture::default());
    let observer: Arc<dyn crate::io_stats::FilteredReadObserver> = capture.clone();
    let ids = wanted.iter().copied().collect();
    let batches = read_nodes_filtered_projected_observed(
        dir,
        &ids,
        &[TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap()],
        Some(&observer),
    )
    .unwrap();
    let mut returned = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("node_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        returned.extend(column.values().iter().copied());
    }
    let reports = capture.reports.lock().unwrap();
    assert_eq!(reports.len(), 1, "one physical attempt is expected");
    (returned, reports[0])
}

#[test]
fn dense_selection_decodes_the_pages_of_either_layout_and_returns_the_same_rows() {
    // 50,000 rows: three 20,480-row pages in the legacy layout, twenty-five
    // 2,048-row pages in the point-access layout. The ids sit in three
    // different pages of each, one of them in the short final page.
    const ROWS: u64 = 50_000;
    let wanted = [5_u64, 30_000, 49_999];
    let legacy = TempDir::new().unwrap();
    write_dense_nodes(
        legacy.path(),
        ROWS,
        crate::permanent_parquet::writer_properties().build(),
    );
    let paged = TempDir::new().unwrap();
    write_dense_nodes(
        paged.path(),
        ROWS,
        crate::permanent_parquet::point_access_writer_properties().build(),
    );

    let (legacy_ids, legacy_pruning) = read_dense_nodes(legacy.path(), &wanted);
    let (paged_ids, paged_pruning) = read_dense_nodes(paged.path(), &wanted);

    assert_eq!(legacy_ids, wanted);
    assert_eq!(paged_ids, wanted, "both layouts return the same rows");
    for pruning in [legacy_pruning, paged_pruning] {
        assert_eq!(
            pruning.strategy,
            crate::io_stats::FilteredReadStrategy::DenseRowSelection
        );
        assert_eq!(pruning.metadata_fallbacks, 0);
        assert_eq!(pruning.validation_fallbacks, 0);
        assert_eq!(pruning.exact_rows_selected, 3);
        assert_eq!(pruning.pages_selected, 3);
    }
    assert_eq!(legacy_pruning.pages_considered, 3);
    // Ids 5 and 30,000 sit in the two full pages; id 49,999 in the tail.
    assert_eq!(
        legacy_pruning.page_rows_selected,
        20_480 + 20_480 + (ROWS - 2 * 20_480)
    );
    assert_eq!(paged_pruning.pages_considered, ROWS.div_ceil(2_048));
    // Id 5 is in the first page, id 30,000 in a full page, and id 49,999 in
    // the final page of 49,152..50,000.
    assert_eq!(
        paged_pruning.page_rows_selected,
        2_048 + 2_048 + (ROWS - 24 * 2_048)
    );
    assert!(paged_pruning.page_rows_selected * 5 < legacy_pruning.page_rows_selected);
}

#[test]
fn dense_selection_decode_does_not_grow_with_the_node_table() {
    // The pages a frontier touches are the pages its ids sit in, however many
    // rows the table holds: doubling the table adds pages to the layout proof
    // but none to the decode.
    let wanted = [10_u64, 700, 1_500];
    let mut decoded = Vec::new();
    for rows in [30_000_u64, 60_000, 120_000] {
        let dir = TempDir::new().unwrap();
        write_dense_nodes(
            dir.path(),
            rows,
            crate::permanent_parquet::point_access_writer_properties().build(),
        );
        let (ids, pruning) = read_dense_nodes(dir.path(), &wanted);
        assert_eq!(ids, wanted);
        assert_eq!(
            pruning.strategy,
            crate::io_stats::FilteredReadStrategy::DenseRowSelection
        );
        assert_eq!(pruning.pages_selected, 1);
        assert_eq!(pruning.pages_considered, rows.div_ceil(2_048));
        decoded.push(pruning.page_rows_selected);
    }
    assert_eq!(decoded, [2_048; 3]);
}
