use super::*;

#[test]
fn runtime_label_decode_rejects_cross_batch_duplicate_identity() {
    let mut catalog = graphforge_value::RuntimeCatalogData::new();
    catalog.intern_label_at("First", 1).unwrap();
    catalog.intern_label_at("Second", 1).unwrap();
    let batch = catalog.to_record_batch();
    let mut columns = batch.columns().to_vec();
    columns[2] = Arc::new(UInt32Array::from(vec![0, 0]));
    let invalid = RecordBatch::try_new(batch.schema(), columns).unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("catalog.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), invalid.schema(), None)
            .unwrap();
    writer.write(&invalid).unwrap();
    writer.close().unwrap();
    let budgets = GraphConstructionBudgets {
        max_batch_rows: 1,
        ..Default::default()
    };
    let error = read_runtime_label_ids(
        File::open(path).unwrap(),
        budgets,
        &mut GraphConstructionEncodingEvidence::default(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("unique and contiguous"),
        "{error}"
    );
}

#[test]
fn proof_counter_addition_rejects_overflow_without_clamping() {
    let mut counter = u64::MAX;
    let error = add_evidence_counter(&mut counter, 1, "mutation").unwrap_err();
    assert!(error.to_string().contains("mutation overflow"));
    assert_eq!(counter, u64::MAX);
}
