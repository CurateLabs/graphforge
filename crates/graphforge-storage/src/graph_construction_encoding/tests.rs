use super::*;

#[test]
fn bounded_property_outputs_count_completed_hashes_and_private_sealed_writes_separately() {
    let temporary = tempfile::tempdir().unwrap();
    let root = StableDirectory::open(temporary.path()).unwrap();
    let logical_bytes = crate::property_overlay::bounded_object::MAX_PROPERTY_OBJECT_BYTES + 1;
    let payload = bytes::Bytes::from(vec![0x4f; logical_bytes]);
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let mut evidence = GraphConstructionEncodingEvidence::default();
    let capture = crate::concurrency_attribution::RegionCapture::start("import_command");
    let artifacts = lanes::write_parquet_chunks(
        &root,
        "properties/large.parquet",
        &batch,
        std::num::NonZeroU64::new(1 << 20).unwrap(),
        &mut evidence,
        &mut || false,
        Some(lanes::Encoded::Object(payload)),
    )
    .unwrap();
    let snapshot = capture.finish();
    assert!(artifacts.len() > 1);
    let physical_bytes: u64 = artifacts.iter().map(|artifact| artifact.bytes).sum();
    assert_eq!(
        snapshot.regions["import_command"].work["hashed_bytes"],
        2 * physical_bytes
    );
    assert_eq!(
        snapshot.regions["import_command"].work["written_bytes"],
        logical_bytes as u64 + physical_bytes
    );
    for artifact in artifacts {
        let bytes = std::fs::read(temporary.path().join("graph").join(&artifact.path)).unwrap();
        assert_eq!(artifact.sha256, hex(&Sha256::digest(&bytes)));
        assert_eq!(artifact.xxh64, crate::corruption_checksum::checksum(&bytes));
    }
}

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
            let artifacts = result.unwrap();
            let artifact = &artifacts[0];
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

#[test]
fn encoded_inventory_version_precedes_required_checksums_and_current_wire_is_strict() {
    let artifact = ConstructionEncodedArtifact {
        path: "topology/generation.json".into(),
        bytes: 2,
        sha256: "a".repeat(64),
        xxh64: crate::corruption_checksum::checksum(b"{}"),
    };
    let inventory = GraphConstructionEncoding {
        format_version: ENCODING_FORMAT_VERSION,
        root: ENCODED_ROOT.into(),
        generation: 1,
        ontology_mode: OntologyMode::default(),
        semantic_authority_sha256: None,
        shape_inputs_sha256: "b".repeat(64),
        shape_authority_sha256: "c".repeat(64),
        artifacts: vec![artifact],
        retained_artifacts: vec![ConstructionRetainedArtifact {
            source_root: "parent".into(),
            source_root_volume: 1,
            source_root_file_id: "0".repeat(32),
            source_path: "object".into(),
            source_volume: 1,
            source_file_id: "0".repeat(32),
            target_path: "target".into(),
            bytes: 2,
            sha256: "d".repeat(64),
            xxh64: crate::corruption_checksum::checksum(b"{}"),
            parent_manifest_sha256: "e".repeat(64),
        }],
        evidence: GraphConstructionEncodingEvidence::default(),
        invocation: GraphConstructionEncodingInvocationEvidence::default(),
    };
    let original = serde_json::to_value(&inventory).unwrap();
    for version in [None, Some(1), Some(3)] {
        let mut changed = original.clone();
        if let Some(version) = version {
            changed["format_version"] = version.into();
        } else {
            changed.as_object_mut().unwrap().remove("format_version");
        }
        changed["artifacts"][0]
            .as_object_mut()
            .unwrap()
            .remove("xxh64");
        let error = decode_encoding_inventory(serde_json::to_vec(&changed).unwrap().as_slice())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported encoded inventory format"),
            "{error}"
        );
    }
    for collection in ["artifacts", "retained_artifacts"] {
        for invalid in [
            None,
            Some(""),
            Some("123"),
            Some("GG00000000000000"),
            Some("00000000000000000"),
        ] {
            let mut changed = original.clone();
            if let Some(invalid) = invalid {
                changed[collection][0]["xxh64"] = invalid.into();
            } else {
                changed[collection][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("xxh64");
            }
            let error = decode_encoding_inventory(serde_json::to_vec(&changed).unwrap().as_slice())
                .unwrap_err();
            assert!(
                !error.to_string().contains("unsupported encoded"),
                "{error}"
            );
        }
    }
    assert_eq!(
        decode_encoding_inventory(serde_json::to_vec(&original).unwrap().as_slice()).unwrap(),
        inventory
    );
}

#[test]
fn encoded_final_writer_captures_identity_and_checksum_once() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let payload = vec![0x5a; COPY_BUFFER_BYTES + 17];
    let expected_sha = hex(&Sha256::digest(&payload));
    let expected_checksum = crate::corruption_checksum::checksum(&payload);
    let mut artifacts = Vec::new();
    let mut evidence = GraphConstructionEncodingEvidence::default();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    copy_artifact(
        std::io::Cursor::new(&payload),
        &directory,
        "payload.parquet",
        &mut artifacts,
        &mut evidence,
    )
    .unwrap();
    let observed = capture.snapshot();
    assert_eq!(observed.artifact_payload_sha256_bytes, payload.len() as u64);
    assert_eq!(observed.checksum_bytes, payload.len() as u64);
    assert_eq!(observed.unclassified_sha256_bytes, 0);
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].sha256, expected_sha);
    assert_eq!(artifacts[0].xxh64, expected_checksum);
    assert_eq!(artifacts[0].bytes, payload.len() as u64);
    assert_eq!(
        std::fs::read(root.path().join("graph/payload.parquet")).unwrap(),
        payload
    );
}
