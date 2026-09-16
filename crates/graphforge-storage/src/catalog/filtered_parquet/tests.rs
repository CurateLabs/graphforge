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
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use graphforge_core::OntologyMode;
use parquet::arrow::ArrowWriter;
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
