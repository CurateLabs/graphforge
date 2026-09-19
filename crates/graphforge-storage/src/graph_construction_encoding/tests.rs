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

/// Each mode runs in a fresh process so experimental environment switches never
/// race tests running in the parent harness.
#[test]
fn encode_seam_matches_bytes_and_cleans_up_refusal_and_cancellation() {
    let root = tempfile::TempDir::new().unwrap();
    for mode in ["baseline", "datafusion", "pool-refusal", "cancel"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "graph_construction_encoding::tests::encode_seam_subprocess",
                "--nocapture",
            ])
            .env("GF_ENCODE_SEAM_TEST_ROOT", root.path())
            .env("GF_ENCODE_SEAM_TEST_MODE", mode)
            .env(
                "GF_ENCODE_SEAM_SPIKE",
                if mode == "baseline" {
                    "baseline"
                } else {
                    "datafusion"
                },
            )
            .env(
                "GF_ENCODE_SEAM_POOL_BYTES",
                if mode == "pool-refusal" {
                    "0"
                } else {
                    "67108864"
                },
            )
            .env("GF_ENCODE_SEAM_METRICS", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if mode == "datafusion" {
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(
                stderr.contains("\"changed_column_estimated_bytes\":0"),
                "{stderr}"
            );
            assert!(
                stderr.contains("StreamingTableExec: partition_sizes=1"),
                "{stderr}"
            );
            assert!(stderr.contains("ENCODE_SEAM_WORKER"), "{stderr}");
        }
    }
    assert_eq!(
        std::fs::read(root.path().join("baseline/graph/result.parquet")).unwrap(),
        std::fs::read(root.path().join("datafusion/graph/result.parquet")).unwrap()
    );
}

#[test]
fn encode_seam_subprocess() {
    let Some(root) = std::env::var_os("GF_ENCODE_SEAM_TEST_ROOT") else {
        return;
    };
    let mode = std::env::var("GF_ENCODE_SEAM_TEST_MODE").unwrap();
    let path = std::path::PathBuf::from(root).join(&mode);
    std::fs::create_dir(&path).unwrap();
    let directory = StableDirectory::open(&path).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from_iter_values(0..100_000)),
            Arc::new(StringArray::from_iter_values(
                (0..100_000).map(|n| format!("value-{n:016x}-λ")),
            )),
        ],
    )
    .unwrap();
    let mut evidence = GraphConstructionEncodingEvidence::default();
    let mut observed_worker_write = false;
    let result = write_parquet(
        &directory,
        "result.parquet",
        &batch,
        std::num::NonZeroU64::new(4096).unwrap(),
        &mut evidence,
        &mut || {
            if mode != "cancel" {
                return false;
            }
            observed_worker_write |= std::fs::read_dir(path.join("graph"))
                .unwrap()
                .any(|entry| entry.unwrap().metadata().unwrap().len() > 4);
            observed_worker_write
        },
    );
    match mode.as_str() {
        "pool-refusal" | "cancel" => {
            let message = result.unwrap_err().to_string();
            if mode == "cancel" {
                assert!(observed_worker_write);
                assert!(message.contains("cancelled"), "{message}");
            } else {
                assert!(message.contains("Resources exhausted"), "{message}");
            }
            assert_eq!(std::fs::read_dir(path.join("graph")).unwrap().count(), 0);
            assert_eq!(evidence.output_write_bytes, 0);
        }
        _ => {
            let artifact = result.unwrap();
            assert_eq!(
                artifact.bytes,
                std::fs::metadata(path.join("graph/result.parquet"))
                    .unwrap()
                    .len()
            );
            assert!(evidence.fsync_operations > 1);
        }
    }
}
