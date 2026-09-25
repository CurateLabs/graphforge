use super::*;
use arrow::array::{BooleanArray, FixedSizeBinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

fn fixture_uuid(value: u128) -> Uuid {
    Uuid::from_u128(0x0189_0000_0000_7000_8000_0000_0000_0000 | value)
}

fn graph(workers: usize) -> GraphForge {
    graph_at(None, workers)
}

fn graph_at(path: Option<&str>, workers: usize) -> GraphForge {
    let mut graph = GraphForge::new_with_options(
        path,
        crate::GraphForgeOptions {
            resource: crate::ExecutionResourcePolicy {
                compute_threads: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    // Force scheduling cases even on one/two-CPU CI hosts. Resource-policy
    // admission is tested separately; this fixture exercises the private pool.
    graph.compute_pool = Arc::new(graphforge_exec::ComputePool::new(workers).unwrap());
    graph.resource_policy.compute_threads = workers;
    // Admit every forced worker, so these cases keep exercising the pool;
    // the #1586 admission limit has its own cases below.
    graph.construction_cpu_admission = admission(workers);
    graph
}

fn admission(lanes: usize) -> Arc<graphforge_storage::ConstructionCpuAdmission> {
    Arc::new(graphforge_storage::ConstructionCpuAdmission::new(
        std::num::NonZeroUsize::new(lanes).unwrap(),
    ))
}

fn nodes(ids: &[Option<Uuid>], properties: usize) -> RecordBatch {
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(ids.len(), 16);
    for id in ids {
        match id {
            Some(id) => uuids.append_value(id.as_bytes()).unwrap(),
            None => uuids.append_null(),
        }
    }
    let fields = (0..properties)
        .map(|i| Field::new(format!("flag{i:03}"), DataType::Boolean, false))
        .collect();
    let mut columns: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(uuids.finish()),
        Arc::new(StringArray::from(vec!["Person"; ids.len()])),
    ];
    columns
        .extend((0..properties).map(|_| {
            Arc::new(BooleanArray::from(vec![true; ids.len()])) as arrow::array::ArrayRef
        }));
    RecordBatch::try_new(crate::bulk_node_input_schema(fields).unwrap(), columns).unwrap()
}

fn window(graph: &GraphForge, budget: usize) -> Window<'_> {
    Window {
        graph,
        operation_uuid: fixture_uuid(1472),
        source_sequence: 7,
        kind: BulkInputKind::Node,
        cancellation: None,
        pending: Vec::new(),
        admitted_bytes: 0,
        byte_budget: budget,
        workers: graph.compute_pool.num_threads().min(4),
        probe: None,
    }
}

#[test]
fn parallel_windows_preserve_generated_uuids_properties_and_original_batch_order() {
    let batches = vec![
        nodes(&[None, Some(fixture_uuid(11))], 2),
        nodes(&[], 2),
        nodes(&[None, None, Some(fixture_uuid(12))], 2),
        nodes(&[Some(fixture_uuid(13))], 2),
    ];
    let serial = graph(1);
    let expected = batches
        .iter()
        .enumerate()
        .map(|(i, batch)| {
            (
                i as u64,
                normalize_batch(
                    &serial,
                    import_batch_operation(fixture_uuid(1472), 7, i as u64),
                    BulkInputKind::Node,
                    batch,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for workers in [1, 2, 4] {
        let graph = graph(workers);
        let mut window = window(&graph, usize::MAX);
        let mut actual = Vec::new();
        let mut consume = |i, batch| {
            actual.push((i, batch));
            Ok(())
        };
        for (i, batch) in batches.iter().enumerate() {
            window.push(i as u64, batch.clone(), &mut consume).unwrap();
        }
        window.flush(&mut consume).unwrap();
        assert_eq!(actual, expected, "workers={workers}");
    }
}

#[test]
fn late_duplicate_preserves_successful_prefix_and_earliest_refusal() {
    let graph = graph(4);
    let duplicate = fixture_uuid(21);
    let invalid = nodes(&[Some(duplicate), Some(duplicate)], 0);
    let expected = normalize_batch(
        &graph,
        import_batch_operation(fixture_uuid(1472), 7, 2),
        BulkInputKind::Node,
        &invalid,
    )
    .unwrap_err()
    .to_string();
    assert!(expected.contains("identity_conflict"), "{expected}");
    let batches = [
        nodes(&[None], 0),
        nodes(&[None], 0),
        invalid,
        nodes(&[None], 0),
    ];
    let mut window = window(&graph, usize::MAX);
    let mut consumed = Vec::new();
    let mut consume = |i, _| {
        consumed.push(i);
        Ok(())
    };
    let mut failure = None;
    for (i, batch) in batches.into_iter().enumerate() {
        if let Err(error) = window.push(i as u64, batch, &mut consume) {
            failure = Some(error);
            break;
        }
    }
    assert_eq!(failure.unwrap().to_string(), expected);
    assert_eq!(consumed, [0, 1]);
    assert!(window.pending.is_empty());
}

#[test]
fn normalization_work_counts_successful_rows_in_the_closed_receipt_contract() {
    let graph = graph(4);
    let mut window = window(&graph, usize::MAX);
    let duplicate = fixture_uuid(22);
    let capture =
        graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
    let mut consume = |_, _| Ok(());
    window
        .push(0, nodes(&[None, None], 0), &mut consume)
        .unwrap();
    window
        .push(
            1,
            nodes(&[Some(duplicate), Some(duplicate)], 0),
            &mut consume,
        )
        .unwrap();
    assert!(window.flush(&mut consume).is_err());
    let snapshot = serde_json::to_value(capture.finish()).unwrap();
    // The failed batch contributes no successfully normalized rows. The
    // certification contract exposes only its established work units.
    assert_eq!(
        snapshot["regions"]["import_command/normalization"]["work"],
        serde_json::json!({"rows": 2}),
    );
}

#[test]
fn wide_bitmap_properties_and_oversized_singletons_obey_byte_admission() {
    let graph = graph(4);
    let ids = vec![None; 256];
    let plain = nodes(&ids, 0);
    let wide = nodes(&ids, 32);
    let budget = 2 * batch_weight(BulkInputKind::Node, &plain);
    assert!(batch_weight(BulkInputKind::Node, &wide) > budget);
    let mut window = window(&graph, budget);
    let mut consumed = Vec::new();
    let mut consume = |i, _| {
        consumed.push(i);
        Ok(())
    };
    window.push(0, plain, &mut consume).unwrap();
    assert_eq!(window.pending.len(), 1);
    assert!(window.admitted_bytes <= budget);
    window.push(1, wide, &mut consume).unwrap();
    assert!(window.pending.is_empty());
    assert_eq!(window.admitted_bytes, 0);
    assert_eq!(consumed, [0, 1]);
}

#[test]
fn decode_failure_drains_prefix_and_does_not_replace_an_earlier_validation_error() {
    for earlier_invalid in [false, true] {
        let graph = graph(4);
        let id = fixture_uuid(31);
        let first = if earlier_invalid {
            nodes(&[Some(id), Some(id)], 0)
        } else {
            nodes(&[None], 0)
        };
        let mut window = window(&graph, usize::MAX);
        let mut consumed = Vec::new();
        let mut consume = |i, _| {
            consumed.push(i);
            Ok(())
        };
        let mut index = 0;
        let error = super::super::consume_source_batches(
            [
                Ok(first),
                Err(GfError::Storage("later decode failure".into())),
            ]
            .into_iter(),
            &mut |batch| {
                if let Some(batch) = batch {
                    let next = index;
                    index += 1;
                    window.push(next, batch, &mut consume)
                } else {
                    window.flush(&mut consume)
                }
            },
        )
        .unwrap_err();
        if earlier_invalid {
            assert!(matches!(error, GfError::Validation(_)), "{error}");
            assert!(consumed.is_empty());
        } else {
            assert!(error.to_string().contains("later decode failure"));
            assert_eq!(consumed, [0]);
        }
    }
}

#[test]
fn cancellation_after_first_consumed_result_never_appends_later_results() {
    let graph = graph(4);
    let token = CancellationToken::new();
    let mut window = window(&graph, usize::MAX);
    window.cancellation = Some(&token);
    let mut consumed = Vec::new();
    let mut consume = |i, _| {
        consumed.push(i);
        token.cancel();
        Ok(())
    };
    for i in 0..3 {
        window.push(i, nodes(&[None], 0), &mut consume).unwrap();
    }
    let error = window.push(3, nodes(&[None], 0), &mut consume).unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(consumed, [0]);
    assert!(window.pending.is_empty());
}

#[test]
fn failed_parallel_import_keeps_the_same_durable_prefix_after_reopen() {
    for workers in [1, 4] {
        let directory = tempfile::tempdir().unwrap();
        let graph = graph_at(directory.path().to_str(), workers);
        let operation = crate::OperationId(fixture_uuid(1472));
        let mut session = graph
            .begin_import_session(operation, crate::ImportSessionLimits::default())
            .unwrap();
        let duplicate = fixture_uuid(51);
        session
            .append_arrow(
                BulkInputKind::Node,
                &[
                    nodes(&[None], 0),
                    nodes(&[None], 0),
                    nodes(&[Some(duplicate), Some(duplicate)], 0),
                    nodes(&[None], 0),
                ],
            )
            .unwrap();
        let first_error = session.validate(&graph).unwrap_err().to_string();
        assert_eq!(session.manifest.sources[0].batches_staged, 2);
        assert_eq!(session.manifest.progress.rows_accepted, 2);
        assert_eq!(session.manifest.sources[0].inflight_batch, None);
        drop(session);
        drop(graph);
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let mut resumed = graph.resume_import_session(operation.0).unwrap();
        assert_eq!(
            resumed.validate(&graph).unwrap_err().to_string(),
            first_error
        );
        assert_eq!(resumed.manifest.sources[0].batches_staged, 2);
        assert_eq!(resumed.manifest.progress.rows_accepted, 2);
    }
}

fn payload_digests(root: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    fn visit(
        root: &Path,
        current: &Path,
        values: &mut std::collections::BTreeMap<String, Vec<u8>>,
    ) {
        use sha2::Digest;
        for entry in std::fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), values);
            } else {
                let relative = entry
                    .path()
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                // Existing ADR0038 control exception, also named by storage's
                // determinism suite: this receipt contains a random nonce.
                if relative == "topology/uuid-membership/ordinal-v4-receipt.json" {
                    continue;
                }
                values.insert(
                    relative,
                    sha2::Sha256::digest(std::fs::read(entry.path()).unwrap()).to_vec(),
                );
            }
        }
    }
    let mut values = std::collections::BTreeMap::new();
    visit(root, root, &mut values);
    values
}

#[test]
fn serial_and_parallel_import_publish_identical_payloads_with_recorded_clock_fixed() {
    let mut fingerprints = Vec::new();
    for workers in [1, 4] {
        let directory = tempfile::tempdir().unwrap();
        let graph = graph_at(directory.path().to_str(), workers);
        let mut session = graph
            .begin_import_session(
                crate::OperationId(fixture_uuid(1472)),
                crate::ImportSessionLimits::default(),
            )
            .unwrap();
        let batches = (0..4)
            .map(|i| {
                let mut base = nodes(&[Some(fixture_uuid(100 + i)), None], 0)
                    .columns()
                    .to_vec();
                base.push(Arc::new(StringArray::from(vec![Some("naïve λ"), None])));
                RecordBatch::try_new(
                    crate::bulk_node_input_schema(vec![Field::new(
                        "caption",
                        DataType::Utf8,
                        true,
                    )])
                    .unwrap(),
                    base,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        session.append_arrow(BulkInputKind::Node, &batches).unwrap();
        let edge_batches = (0..4)
            .map(|i| {
                let fixed = |value: u128| {
                    let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
                    builder
                        .append_value(fixture_uuid(value).as_bytes())
                        .unwrap();
                    Arc::new(builder.finish()) as arrow::array::ArrayRef
                };
                RecordBatch::try_new(
                    crate::bulk_edge_input_schema(Vec::new()).unwrap(),
                    vec![
                        fixed(200 + i),
                        Arc::new(StringArray::from(vec!["LINK"])),
                        fixed(100 + i),
                        fixed(100 + (i + 1) % 4),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        session
            .append_arrow(BulkInputKind::Edge, &edge_batches)
            .unwrap();
        // Pin the existing recorded input before any chunk is staged or shape
        // exists. Do not remove or bypass recovery's session binding (#1416).
        let construction = session.open_construction(&graph).unwrap();
        let construction_id = construction.session_uuid();
        drop(construction);
        let root = graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(construction_id.simple().to_string());
        let checkpoint_path = root.join("checkpoint.json");
        let mut checkpoint: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&checkpoint_path).unwrap()).unwrap();
        assert!(checkpoint.get("session_now_micros").is_some());
        checkpoint["session_now_micros"] = serde_json::json!(1_789_000_000_000_000_i64);
        std::fs::write(checkpoint_path, serde_json::to_vec(&checkpoint).unwrap()).unwrap();
        session.validate(&graph).unwrap();
        assert!(
            root.join("encoded-v1/graph/topology/uuid-membership/ordinal-v4-receipt.json")
                .exists()
        );
        let mut fingerprint = payload_digests(&root.join("encoded-v1/graph"));
        assert!(fingerprint.len() > 20);
        for name in [
            "shaped-identities.run",
            "shaped-node-details.run",
            "shaped-edge-details.run",
            "shaped-edge-endpoints.run",
        ] {
            // Encoding retires shaped payloads after authenticating their
            // successor. Compare the retained content-addressing receipts.
            let receipt = std::fs::read_dir(&root)
                .unwrap()
                .map(Result::unwrap)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("shape-receipt-")
                })
                .map(|entry| {
                    serde_json::from_slice::<serde_json::Value>(
                        &std::fs::read(entry.path()).unwrap(),
                    )
                    .unwrap()
                })
                .find(|receipt| receipt["name"] == name)
                .unwrap_or_else(|| panic!("missing receipt for {name}"));
            assert!(receipt["bytes"].as_u64().unwrap() > 0, "{name}");
            let digest = receipt["sha256"].as_str().unwrap();
            assert_eq!(digest.len(), 64);
            fingerprint.insert(format!("shape/{name}"), digest.as_bytes().to_vec());
        }
        session.commit(&graph, None).unwrap();
        drop(session);
        drop(graph);
        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), 8);
        assert_eq!(
            reopened
                .execute("MATCH ()-[r:LINK]->() RETURN r")
                .unwrap()
                .stats
                .rows_produced,
            4
        );
        fingerprints.push(fingerprint);
    }
    assert_eq!(fingerprints[0], fingerprints[1]);
}

fn normalize_all(graph: &GraphForge, batches: &[RecordBatch]) -> Vec<(u64, RecordBatch)> {
    let mut window = window(graph, usize::MAX);
    let mut consumed = Vec::new();
    let mut consume = |index, batch| {
        consumed.push((index, batch));
        Ok(())
    };
    for (index, batch) in batches.iter().enumerate() {
        window
            .push(index as u64, batch.clone(), &mut consume)
            .unwrap();
    }
    window.flush(&mut consume).unwrap();
    consumed
}

/// #1586: two imports normalizing at once on one instance share its
/// construction admission. Neither ever holds more lanes than the limit, and
/// each produces exactly what an unconstrained serial run produces.
#[test]
fn concurrent_windows_share_the_instance_construction_limit() {
    let batches = (0..12_u128)
        .map(|index| nodes(&[None, Some(fixture_uuid(100 + index))], 3))
        .collect::<Vec<_>>();
    let expected = normalize_all(&graph(1), &batches);
    let mut shared = graph(4);
    shared.construction_cpu_admission = admission(2);
    let shared = &shared;
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| normalize_all(shared, &batches));
        let second = scope.spawn(|| normalize_all(shared, &batches));
        (first.join().unwrap(), second.join().unwrap())
    });
    for outcome in [&first, &second] {
        assert_eq!(outcome.len(), expected.len());
        for ((index, batch), (expected_index, expected_batch)) in outcome.iter().zip(&expected) {
            assert_eq!(index, expected_index);
            // Generated UUIDs derive from the operation and batch index, so
            // equal inputs normalize to equal batches whatever the grouping.
            assert_eq!(batch, expected_batch, "batch {index}");
        }
    }
    // Each first flush asks for four lanes, so the limit is always reached,
    // and never passed.
    assert_eq!(shared.construction_cpu_admission.peak(), 2);
    assert_eq!(shared.construction_cpu_admission.in_use(), 0);
}

/// #1586: a normalization flush that cannot get a lane waits, notices the
/// import's cancellation, and returns the import's own cancellation error
/// without consuming anything.
#[test]
fn flush_waiting_for_construction_lanes_is_cancellable() {
    let mut graph = graph(4);
    graph.construction_cpu_admission = admission(1);
    let held = graph
        .construction_cpu_admission
        .acquire(std::num::NonZeroUsize::MIN, &mut || false)
        .unwrap();
    let token = CancellationToken::new();
    let mut window = window(&graph, usize::MAX);
    window.cancellation = Some(&token);
    let mut consumed = 0_usize;
    let started = std::time::Instant::now();
    let error = std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(50));
            token.cancel();
        });
        window
            .push(0, nodes(&[Some(fixture_uuid(900))], 1), &mut |_, _| {
                consumed += 1;
                Ok(())
            })
            .and_then(|()| {
                window.flush(&mut |_, _| {
                    consumed += 1;
                    Ok(())
                })
            })
            .unwrap_err()
    });
    assert_eq!(error.to_string(), cancelled().to_string());
    assert_eq!(consumed, 0);
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    drop(held);
    assert_eq!(graph.construction_cpu_admission.in_use(), 0);
}

/// #1586: a flush maps no more batches at once than its lease grants. With a
/// four-worker pool and four pending batches, a one-lane and a two-lane
/// admission cap the batches in flight; a four-lane admission lets all four
/// overlap.
#[test]
fn flush_maps_no_more_batches_at_once_than_its_lease() {
    let batches = (0..4_u128)
        .map(|index| nodes(&[Some(fixture_uuid(700 + index))], 1))
        .collect::<Vec<_>>();
    for (lanes, expected_peak) in [(1, 1), (2, 2), (4, 4)] {
        let mut graph = graph(4);
        graph.construction_cpu_admission = admission(lanes);
        let probe = Arc::new(InFlightProbe::default());
        let mut window = window(&graph, usize::MAX);
        window.probe = Some(Arc::clone(&probe));
        let mut consumed = Vec::new();
        let mut consume = |index, _batch| {
            consumed.push(index);
            Ok(())
        };
        for (index, batch) in batches.iter().enumerate() {
            window
                .push(index as u64, batch.clone(), &mut consume)
                .unwrap();
        }
        window.flush(&mut consume).unwrap();
        assert_eq!(consumed, vec![0, 1, 2, 3], "lanes={lanes}");
        assert_eq!(
            probe.peak.load(std::sync::atomic::Ordering::Acquire),
            expected_peak,
            "lanes={lanes}"
        );
    }
}
