// Real multi-level construction measurements, sharing the writer test fixtures.
mod lifecycle_budget {
    use super::*;

    fn small_session(path: &Path) -> GraphConstructionSession {
        GraphConstructionSession::open_with_mode(
            path,
            Uuid::from_u128(119_500),
            0,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .unwrap()
    }

    fn complete_small(session: &mut GraphConstructionSession) {
        if session.state() == GraphConstructionState::Staging {
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session
                .append(
                    ConstructionChunkKind::Edge,
                    "edges",
                    &edge_property_batch(100, 2),
                )
                .unwrap();
            session.seal().unwrap();
        }
        let encoded = session.prepare_canonical_encoding(1).unwrap();
        session
            .publish_canonical(&encoded, Uuid::from_u128(119_501), Uuid::from_u128(119_502))
            .unwrap();
    }

    #[test]
    fn supersession_crash_child() {
        let Ok(path) = std::env::var("GF_SUPERSESSION_CRASH_ROOT") else {
            return;
        };
        let mut session = small_session(Path::new(&path));
        complete_small(&mut session);
    }

    #[test]
    fn supersession_crashes_reconcile_removed_allocations_and_replay() {
        for boundary in [
            "shape.after_identity_retirement",
            "shape.after_endpoint_retirement",
            "shape.after_derived_unlink",
            "supersession.shape_authenticated",
            "supersession.encoded_authenticated",
            "supersession.before_unlink",
            "supersession.after_unlink",
            "supersession.after_sync",
            "supersession.after_checkpoint",
            "supersession.before_sync",
            "supersession.before_inputs_checkpoint",
            "supersession.before_shape_checkpoint",
            "supersession.shape_after_unlink",
            "supersession.shape_after_sync",
        ] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_construction::tests::lifecycle_budget::supersession_crash_child",
                ])
                .env("GF_SUPERSESSION_CRASH_ROOT", root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", boundary)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{boundary}");
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            let mut recovered = small_session(root.path());
            complete_small(&mut recovered);
            assert!(recovered.checkpoint.inputs_retired);
            assert!(recovered.checkpoint.shape_retired);
            assert_eq!(
                recovered.evidence().current_merge_temporary_allocated_bytes,
                0
            );
            let current = recovered.evidence().storage_current.clone();
            let peak = recovered
                .evidence()
                .storage_transient_peak_total_allocated_bytes;
            drop(recovered);
            assert_retirement_publication(root.path());
            let mut replay = small_session(root.path());
            complete_small(&mut replay);
            assert_eq!(replay.evidence().storage_current, current);
            assert_eq!(
                replay
                    .evidence()
                    .storage_transient_peak_total_allocated_bytes,
                peak
            );
        }
    }

    #[test]
    fn supersession_returned_errors_preserve_prior_authority_and_retry() {
        for boundary in [
            "supersession.shape_authenticated",
            "supersession.encoded_authenticated",
            "supersession.before_unlink",
            "supersession.after_unlink",
            "supersession.after_sync",
            "supersession.after_checkpoint",
            "supersession.before_sync",
            "supersession.before_inputs_checkpoint",
            "supersession.before_shape_checkpoint",
            "supersession.shape_after_unlink",
            "supersession.shape_after_sync",
        ] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let mut session = small_session(root.path());
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session.seal().unwrap();
            supersession::set_returned_failure(Some(boundary));
            let result = session.prepare_canonical_encoding(1);
            supersession::set_returned_failure(None);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected returned failure"),
                "{boundary}"
            );
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            // Retry on the SAME object first, then resume from durable controls.
            let encoded = session.prepare_canonical_encoding(1).unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(shape.node_count, 3);
            assert_eq!(
                session.encode_canonical(&shape, 1).unwrap().generation,
                encoded.generation
            );
            drop(session);
            let mut resumed = small_session(root.path());
            complete_small(&mut resumed);
            assert_eq!(
                resumed.evidence().current_merge_temporary_allocated_bytes,
                0
            );
        }
    }

    #[test]
    fn supersession_corrupt_successors_and_replaced_predecessors_fail_closed() {
        for case in ["shape", "encoding", "replacement", "receipt_chain"] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let mut session = small_session(root.path());
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session.seal().unwrap();
            let boundary = if case == "encoding" {
                "supersession.encoded_authenticated"
            } else {
                "supersession.shape_authenticated"
            };
            supersession::set_returned_failure(Some(boundary));
            let failed = session.prepare_canonical_encoding(1);
            supersession::set_returned_failure(None);
            assert!(failed.is_err());
            let input = session.read_receipt(0).unwrap();
            let path = match case {
                "shape" => session.root.path().join(
                    read_completed_shape_outputs(&session.root, &session.checkpoint).unwrap()[0]
                        .name
                        .clone(),
                ),
                "encoding" => {
                    let output = session
                        .root
                        .open_child_directory(OsStr::new("encoded-v1"))
                        .unwrap();
                    let inventory = crate::graph_construction_encoding::read_inventory(&output)
                        .unwrap()
                        .unwrap();
                    output
                        .path()
                        .join("graph")
                        .join(&inventory.artifacts[0].path)
                }
                "replacement" => session.root.path().join(&input.parquet.name),
                "receipt_chain" => session.root.path().join(receipt_name(0)),
                _ => unreachable!(),
            };
            let bytes = std::fs::read(&path).unwrap();
            if case == "replacement" {
                std::fs::rename(&path, root.path().join("preserved-original")).unwrap();
                std::fs::write(&path, &bytes).unwrap();
            } else if case == "receipt_chain" {
                let mut receipt: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                receipt["input_sha256"] = "0".repeat(64).into();
                std::fs::write(&path, serde_json::to_vec(&receipt).unwrap()).unwrap();
            } else {
                let mut corrupt = bytes.clone();
                corrupt[0] ^= 1;
                std::fs::write(&path, corrupt).unwrap();
            }
            let allocation = session.evidence().storage_current.clone();
            let error = session.prepare_canonical_encoding(1).unwrap_err();
            assert!(
                error.to_string().contains(match case {
                    "shape" => "payload digest changed",
                    "encoding" => "canonical artifact differs",
                    "replacement" => "predecessor identity changed",
                    "receipt_chain" => "receipt tail changed",
                    _ => unreachable!(),
                }),
                "{case}: {error}"
            );
            assert_eq!(session.evidence().storage_current, allocation);
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            assert!(path.exists());
        }
    }

    #[test]
    fn supersession_requires_every_committed_shape_writer_receipt() {
        for missing in [true, false] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let mut session = small_session(root.path());
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session.seal().unwrap();
            supersession::set_returned_failure(Some("supersession.encoded_authenticated"));
            let failed = session.prepare_canonical_encoding(1);
            supersession::set_returned_failure(None);
            assert!(failed.is_err());
            let outputs = read_completed_shape_outputs(&session.root, &session.checkpoint).unwrap();
            let path = session
                .root
                .path()
                .join(shape_receipt_name(&outputs[0].name));
            if missing {
                std::fs::remove_file(&path).unwrap();
            } else {
                let mut receipt = outputs[0].clone();
                receipt.sha256 = "0".repeat(64);
                std::fs::write(&path, serde_json::to_vec(&receipt).unwrap()).unwrap();
            }
            let current = session.evidence().storage_current.clone();
            assert!(session.prepare_canonical_encoding(1).is_err());
            assert!(!session.checkpoint.shape_retired);
            assert_eq!(session.evidence().storage_current, current);
            for output in outputs {
                assert!(session.root.path().join(output.name).exists());
            }
        }
    }

    #[test]
    fn supersession_all_published_replay_paths_refuse_corrupt_public_payload() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = small_session(root.path());
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 3),
            )
            .unwrap();
        session.seal().unwrap();
        let encoding = session.prepare_canonical_encoding(1).unwrap();
        let target = Uuid::from_u128(119_501);
        let transaction = Uuid::from_u128(119_502);
        session
            .publish_canonical(&encoding, target, transaction)
            .unwrap();
        assert!(session.checkpoint.shape_retired);
        let current = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
        let selected = crate::resolve_project_generation(root.path()).unwrap();
        let inventory = selected.graph_files_inventory().unwrap().unwrap();
        let entry = inventory
            .files
            .iter()
            .find(|entry| entry.relative_path.ends_with(".parquet"))
            .unwrap();
        let path = crate::graph_object_path(root.path(), &entry.content_sha256).unwrap();
        let permissions = std::fs::metadata(&path).unwrap().permissions();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::rename(&path, root.path().join("saved-public-payload")).unwrap();
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(&path, permissions).unwrap();
        for result in [
            session
                .replay_committed_publication(target, transaction, || false)
                .map(|_| ()),
            session
                .publish_canonical(&encoding, target, transaction)
                .map(|_| ()),
        ] {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("digest does not match its address")
            );
        }
        drop(session);
        let error = GraphConstructionSession::open_with_mode(
            root.path(),
            Uuid::from_u128(119_500),
            0,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .err()
        .unwrap();
        assert!(
            error
                .to_string()
                .contains("digest does not match its address")
        );
        assert_eq!(
            std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
            current
        );
    }

    #[test]
    fn supersession_cancellation_during_authentication_and_removal_is_recoverable() {
        for cancel_after in [2, 6, 12, 24, u64::MAX] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let mut session = small_session(root.path());
            for chunk in 0..8 {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{chunk}"),
                        &node_property_batch(1 + chunk * 256, 256),
                    )
                    .unwrap();
            }
            session.seal().unwrap();
            supersession::set_returned_failure(Some("supersession.shape_authenticated"));
            assert!(session.prepare_canonical_encoding(1).is_err());
            supersession::set_returned_failure(None);
            let predecessor = session
                .root
                .path()
                .join(session.read_receipt(0).unwrap().parquet.name);
            let mut polls = 0;
            let error = session
                .reclaim_superseded_payloads_cancellable(&mut || {
                    polls += 1;
                    polls >= cancel_after || (cancel_after == u64::MAX && !predecessor.exists())
                })
                .unwrap_err();
            assert!(error.to_string().contains("construction cancelled"));
            if cancel_after == u64::MAX {
                assert!(!predecessor.exists());
            } else {
                assert_eq!(polls, cancel_after);
            }
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            drop(session);
            let mut resumed = small_session(root.path());
            complete_small(&mut resumed);
            assert_eq!(
                resumed.evidence().current_merge_temporary_allocated_bytes,
                0
            );
            let error = resumed
                .replay_committed_publication(Uuid::from_u128(119_501), Uuid::from_u128(119_502), {
                    let mut polls = 0;
                    move || {
                        polls += 1;
                        polls >= 3
                    }
                })
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("construction ordinal authentication cancelled"),
                "{error}"
            );
            assert!(resumed.publication_committed());
        }
    }

    fn census(root: &Path) -> serde_json::Value {
        let mut pending = vec![root.to_path_buf()];
        let mut seen = BTreeSet::new();
        let mut groups = BTreeMap::<&str, (u64, u64, u64)>::new();
        while let Some(path) = pending.pop() {
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                pending.extend(std::fs::read_dir(path).unwrap().map(|e| e.unwrap().path()));
                continue;
            }
            let file = File::open(&path).unwrap();
            let identity = file_identity(&file).unwrap();
            let key = (identity.volume_serial, identity.file_id);
            if !seen.insert(key) {
                continue;
            }
            let name = path.file_name().unwrap().to_str().unwrap();
            let group = if path.components().any(|c| c.as_os_str() == "encoded-v1") {
                "encoded"
            } else if name.ends_with(".json") || name.ends_with(".lock") {
                "control"
            } else if name.starts_with("chunk-") {
                "accepted_inputs"
            } else {
                "shape_and_merge"
            };
            let usage = graphforge_filesystem::file_space_usage(&file).unwrap();
            let totals = groups.entry(group).or_default();
            totals.0 += usage.logical_bytes;
            totals.1 += usage.allocated_bytes;
            totals.2 += 1;
        }
        serde_json::to_value(groups).unwrap()
    }

    #[test]
    fn construction_lifecycle_multilevel_allocation_baseline() {
        for scale in [1_u64, 2, 4] {
            let mut before = None;
            for version in [8, 9] {
                let root = TempDir::new().unwrap();
                crate::open_or_initialize_project(root.path()).unwrap();
                let budgets = GraphConstructionBudgets {
                    merge_fan_in: 2,
                    max_batch_rows: 4096,
                    max_run_records: 16384,
                    ..Default::default()
                };
                let mut session = GraphConstructionSession::open_internal_with_format(
                    root.path(),
                    root.path(),
                    Uuid::new_v4(),
                    0,
                    graphforge_core::OntologyMode::Exploratory,
                    None,
                    budgets,
                    crate::filesystem_admission::ProjectLifecycleMode::Durable,
                    None,
                    version,
                )
                .unwrap();
                for chunk in 0..2 {
                    session
                        .append(
                            ConstructionChunkKind::Node,
                            &format!("nodes-{chunk}"),
                            &node_batch(1 + chunk * 4096, 4096),
                        )
                        .unwrap();
                }
                for chunk in 0..8 * scale {
                    session
                        .append(
                            ConstructionChunkKind::Edge,
                            &format!("edges-{chunk}"),
                            &edge_batch(1_000_000 + u128::from(chunk) * 4096, 4096),
                        )
                        .unwrap();
                }
                let append = census(session.root.path());
                session.seal().unwrap();
                let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
                let shape_peak = session
                    .evidence()
                    .storage_transient_peak_total_allocated_bytes;
                assert_eq!((shape.node_count, shape.edge_count), (8192, 32768 * scale));
                assert!(session.evidence().merge_passes >= 3);
                let shaped = census(session.root.path());
                let encoding = session.encode_canonical(&shape, 1).unwrap();
                let encoded = census(session.root.path());
                let publication = session
                    .publish_canonical(&encoding, Uuid::new_v4(), Uuid::new_v4())
                    .unwrap();
                let published = census(session.root.path());
                let selected = crate::resolve_project_generation(root.path()).unwrap();
                assert_eq!(selected.generation_uuid(), publication.generation_uuid);
                let admitted =
                    crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected)
                        .unwrap();
                let edges = crate::read_edges_from_inventory(
                    &admitted,
                    "*",
                    graphforge_core::OntologyMode::Exploratory,
                )
                .unwrap();
                assert_eq!(
                    edges.iter().map(RecordBatch::num_rows).sum::<usize>() as u64,
                    32768 * scale
                );
                let current = session.evidence().storage_current
                    [&crate::ArtifactCategory::ConstructionStaging]
                    .allocated_bytes;
                let peak = session
                    .evidence()
                    .storage_transient_peak_total_allocated_bytes;
                if version == 8 {
                    before = Some((current, peak, shape_peak));
                } else {
                    let (old_current, old_peak, old_shape_peak) = before.unwrap();
                    // Compression can move both variants' maximum into shaping,
                    // before retirement can reduce it. Keep the strict retention
                    // benefit, no peak regression against the compressed control,
                    // and a strict reduction against the source-bound uncompressed
                    // v8 baseline in construction-supersession.md (#1195).
                    assert!(current * 2 < old_current);
                    assert!(peak <= old_peak);
                    let uncompressed_peak = match scale {
                        1 => 23_425_024,
                        2 => 44_494_848,
                        4 => 86_634_496,
                        _ => unreachable!("fixed fixture scales"),
                    };
                    assert!(peak < uncompressed_peak);
                    if peak == old_peak {
                        assert_eq!(peak, shape_peak);
                        assert_eq!(old_peak, old_shape_peak);
                    }
                    assert_eq!(
                        session.evidence().current_merge_temporary_allocated_bytes,
                        0
                    );
                    assert!(published.get("accepted_inputs").is_none());
                    assert!(published.get("shape_and_merge").is_none());
                    // Explicit authentication/removal passes stay below eight input
                    // payload reads plus the small fixed manifest/control allowance.
                    assert!(
                        session.evidence().recovery_application_read_bytes
                            <= session.evidence().write_bytes * 8 + (16 << 20)
                    );
                }
                println!(
                    "LIFECYCLE_BASELINE {}",
                    serde_json::json!({
                        "version": version, "scale": scale, "nodes":8192, "edges":32768 * scale,
                        "append":append, "shape":shaped, "encode":encoded, "publication":published,
                        "payload_current":session.evidence().storage_current,
                        "payload_peak":session.evidence().storage_transient_peak_total_allocated_bytes,
                        "phase_reads": {
                            "shape":session.evidence().shape_application_read_bytes,
                            "encoding":session.evidence().encode_application_read_bytes,
                            "recovery_and_supersession":session.evidence().recovery_application_read_bytes,
                        },
                    })
                );
            }
        }
    }
    fn shaping_recovery_controls(root: &Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let name = entry.file_name().into_string().unwrap();
                (name, entry.path())
            })
            .filter(|(name, path)| name.ends_with(".json") && path.is_file())
            .map(|(name, path)| (name, std::fs::read(path).unwrap()))
            .collect()
    }

    fn shaping_recovery_fixture(root: &TempDir) -> GraphConstructionSession {
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = small_session(root.path());
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 3),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Edge,
                "edges",
                &edge_property_batch(100, 2),
            )
            .unwrap();
        session.seal().unwrap();
        session
    }

    #[test]
    fn shaping_recovery_refuses_same_inode_payload_corruption() {
        for completed in [false, true] {
            let root = TempDir::new().unwrap();
            let mut session = shaping_recovery_fixture(&root);
            let path = session.root.path().join("shaped-identities.run");
            if completed {
                session.shape_canonical_with_cancellation(|| false).unwrap();
            } else {
                assert!(
                    session
                        .shape_canonical_with_cancellation(|| path.exists())
                        .is_err()
                );
            }
            assert!(path.exists());
            let before = file_identity(&File::open(&path).unwrap()).unwrap();
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(file_identity(&File::open(&path).unwrap()).unwrap(), before);
            let current = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let session_path = session.root.path().to_path_buf();
            let before_recovery = shaping_recovery_controls(&session_path);
            drop(session);
            let result = GraphConstructionSession::open_with_mode(
                root.path(),
                Uuid::from_u128(119_500),
                0,
                graphforge_core::OntologyMode::Exploratory,
                GraphConstructionBudgets::default(),
            );
            assert!(
                result.is_err(),
                "completed={completed}: corrupted payload accepted"
            );
            assert_eq!(shaping_recovery_controls(&session_path), before_recovery);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                current
            );
        }
    }
    #[test]
    fn shaping_recovery_counts_surviving_payload_reads_and_retries() {
        let root = TempDir::new().unwrap();
        let session = shaping_recovery_fixture(&root);
        let initial = session.evidence().clone();
        drop(session);
        // An uninterrupted reopen measures the same unchanged parent authority.
        let mut session = small_session(root.path());
        let baseline = session.evidence().clone();
        let parent_bytes =
            baseline.recovery_application_read_bytes - initial.recovery_application_read_bytes;
        let parent_operations = baseline.recovery_application_read_operations
            - initial.recovery_application_read_operations;
        let path = session.root.path().join("shaped-identities.run");
        assert!(
            session
                .shape_canonical_with_cancellation(|| path.exists())
                .is_err()
        );
        let mut expected_bytes = parent_bytes;
        let mut expected_operations = parent_operations;
        for name in session.root.child_names().unwrap() {
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("shape-receipt-") {
                continue;
            }
            let mut file = session.root.open_child_file(OsStr::new(name)).unwrap();
            let receipt: ArtifactReceipt = decode_bounded(&mut file).unwrap();
            // Both control passes read receipts even when their payload is not
            // a derived shape artifact owned by this cleanup.
            expected_bytes += 2 * file.metadata().unwrap().len();
            expected_operations += 2;
            if !is_shape_artifact_name(&receipt.name) {
                continue;
            }
            if session.root.path().join(&receipt.name).exists() {
                expected_bytes += receipt.bytes;
                expected_operations += receipt.bytes.div_ceil(BLOCK_BYTES as u64);
            }
        }
        assert!(expected_bytes > 0);
        drop(session);
        let mut resumed = small_session(root.path());
        assert_eq!(
            resumed.evidence().recovery_application_read_bytes
                - baseline.recovery_application_read_bytes,
            expected_bytes
        );
        assert_eq!(
            resumed.evidence().recovery_application_read_operations
                - baseline.recovery_application_read_operations,
            expected_operations
        );
        complete_small(&mut resumed);
        let selected = crate::resolve_project_generation(root.path()).unwrap();
        let inventory =
            crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected).unwrap();
        let edges = crate::read_edges_from_inventory(
            &inventory,
            "*",
            graphforge_core::OntologyMode::Exploratory,
        )
        .unwrap();
        assert_eq!(edges.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        drop(resumed);
        complete_small(&mut small_session(root.path()));
    }
    #[test]
    fn shaping_recovery_refuses_replaced_or_linked_survivors() {
        for extra_link in [false, true] {
            let root = TempDir::new().unwrap();
            let mut session = shaping_recovery_fixture(&root);
            let path = session.root.path().join("shaped-identities.run");
            assert!(
                session
                    .shape_canonical_with_cancellation(|| path.exists())
                    .is_err()
            );
            let bytes = std::fs::read(&path).unwrap();
            let held = root.path().join("held-original.run");
            if extra_link {
                std::fs::hard_link(&path, &held).unwrap();
            } else {
                std::fs::rename(&path, &held).unwrap();
                std::fs::write(&path, &bytes).unwrap();
            }
            let session_path = session.root.path().to_path_buf();
            let before = shaping_recovery_controls(&session_path);
            drop(session);
            assert!(
                GraphConstructionSession::open_with_mode(
                    root.path(),
                    Uuid::from_u128(119_500),
                    0,
                    graphforge_core::OntologyMode::Exploratory,
                    GraphConstructionBudgets::default()
                )
                .is_err()
            );
            assert_eq!(shaping_recovery_controls(&session_path), before);
            assert_eq!(std::fs::read(path).unwrap(), bytes);
            assert_eq!(std::fs::read(held).unwrap(), bytes);
        }
    }
    #[test]
    fn consumed_shape_roots_have_bounded_multilevel_peak() {
        for scale in [1_u64, 2, 4] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let budgets = GraphConstructionBudgets {
                merge_fan_in: 2,
                max_batch_rows: 4096,
                max_run_records: 16384,
                ..Default::default()
            };
            let mut session = GraphConstructionSession::open_with_mode(
                root.path(),
                Uuid::from_u128(126_800 + u128::from(scale)),
                0,
                graphforge_core::OntologyMode::Exploratory,
                budgets,
            )
            .unwrap();
            for chunk in 0..2 {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{chunk}"),
                        &node_batch(1 + chunk * 4096, 4096),
                    )
                    .unwrap();
            }
            for chunk in 0..8 * scale {
                session
                    .append(
                        ConstructionChunkKind::Edge,
                        &format!("edges-{chunk}"),
                        &edge_batch(1_000_000 + u128::from(chunk) * 4096, 4096),
                    )
                    .unwrap();
            }
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!((shape.node_count, shape.edge_count), (8192, 32768 * scale));
            assert!(session.evidence().merge_passes >= 3);
            let baseline_peak = match scale {
                1 => 20_873_216,
                2 => 40_435_712,
                4 => 79_560_704,
                _ => unreachable!(),
            };
            let obsolete_identity_bytes = 32 * (8192 + 32768 * scale);
            // The preselected identity-root floor passed; retain the additional
            // measured benefit from deferring the final online carry insertion.
            let measured_carry_saving = 32 * 32768 * scale;
            assert!(
                session
                    .evidence()
                    .storage_transient_peak_total_allocated_bytes
                    <= baseline_peak - obsolete_identity_bytes - measured_carry_saving
            );

            println!(
                "CONSUMED_ROOTS {}",
                serde_json::json!({
                    "scale":scale,"nodes":shape.node_count,"edges":shape.edge_count,
                    "peak":session.evidence().storage_transient_peak_total_allocated_bytes,
                    "retained":session.evidence().storage_current,
                    "shape_reads":session.evidence().shape_application_read_bytes,
                    "merge_writes":session.evidence().merge_written_bytes,
                    "merge_passes":session.evidence().merge_passes,
                    "census":census(session.root.path()),
                })
            );
        }
    }

    fn sealed_retirement_fixture(root: &TempDir) -> GraphConstructionSession {
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = small_session(root.path());
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 3),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Edge,
                "edges",
                &edge_property_batch(100, 2),
            )
            .unwrap();
        session.seal().unwrap();
        session
    }

    fn assert_retirement_publication(root: &Path) {
        let selected = crate::resolve_project_generation(root).unwrap();
        let inventory =
            crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected).unwrap();
        let batches = crate::read_edges_from_inventory(
            &inventory,
            "*",
            graphforge_core::OntologyMode::Exploratory,
        )
        .unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            let ids = |name: &str| {
                let array = batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                assert_eq!(array.null_count(), 0);
                array
            };
            for row in 0..batch.num_rows() {
                actual.push((
                    u128::from_be_bytes(ids("edge_uuid").value(row).try_into().unwrap()),
                    u128::from_be_bytes(ids("src_uuid").value(row).try_into().unwrap()),
                    u128::from_be_bytes(ids("dst_uuid").value(row).try_into().unwrap()),
                ));
            }
        }
        actual.sort_unstable();
        assert_eq!(actual, [(100, 1, 2), (101, 2, 3)]);
        let mut properties = Vec::new();
        let routes = inventory
            .routes(crate::PropertyRouteKind::Edge)
            .collect::<Vec<_>>();
        assert_eq!(routes.len(), 1);
        for batch in
            crate::read_edge_properties_from_inventory(root, &inventory, routes[0]).unwrap()
        {
            let ids = batch
                .column_by_name("edge_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let weights = batch
                .column_by_name("weight")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ids.null_count(), 0);
            assert_eq!(weights.null_count(), 0);
            for row in 0..batch.num_rows() {
                properties.push((
                    u128::from_be_bytes(ids.value(row).try_into().unwrap()),
                    weights.value(row),
                ));
            }
        }
        properties.sort_unstable();
        assert_eq!(properties, [(100, 20), (101, 21)]);
    }

    #[test]
    fn consumed_shape_roots_returned_failures_recover_exact_properties() {
        for boundary in [
            "shape.before_identity_retirement",
            "shape.after_identity_retirement",
            "shape.before_endpoint_retirement",
            "shape.after_endpoint_retirement",
        ] {
            let root = TempDir::new().unwrap();
            let mut session = sealed_retirement_fixture(&root);
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            inject_shape_publication_failure(boundary);
            let error = session.prepare_canonical_encoding(1).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected shape publication failure")
            );
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            for sequence in 0..2 {
                let receipt = session.read_receipt(sequence).unwrap();
                assert!(session.root.path().join(receipt.parquet.name).exists());
            }
            // A live incomplete shape must refuse reuse until reopen reconciles it.
            let before_retry = session.evidence().clone();
            assert!(
                session
                    .prepare_canonical_encoding(1)
                    .unwrap_err()
                    .to_string()
                    .contains("incomplete construction shape was not recovered")
            );
            assert_eq!(session.evidence(), &before_retry);
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            drop(session);
            let mut resumed = small_session(root.path());
            complete_small(&mut resumed);
            assert_eq!(
                resumed.evidence().current_merge_temporary_allocated_bytes,
                0
            );
            drop(resumed);
            assert_retirement_publication(root.path());
        }
    }

    #[test]
    fn consumed_shape_roots_cancellation_reconstructs_exact_properties() {
        for endpoint_boundary in [false, true] {
            let root = TempDir::new().unwrap();
            let mut session = sealed_retirement_fixture(&root);
            let directory = session.root.path().to_path_buf();
            let mut observed_boundary = false;
            let result = session.shape_canonical_with_cancellation(|| {
                if !directory.join("shaped-identities.run").exists() {
                    return false;
                }
                let names = std::fs::read_dir(&directory)
                    .unwrap()
                    .map(|e| e.unwrap().file_name().into_string().unwrap())
                    .collect::<Vec<_>>();
                let obsolete_exists = names.iter().any(|name| {
                    name.ends_with(".run")
                        && if endpoint_boundary {
                            name.starts_with("merge-endpoint")
                        } else {
                            name.starts_with("merge-identities")
                                || name.starts_with("merge-unified")
                        }
                });
                let successor_exists = !endpoint_boundary
                    || names
                        .iter()
                        .any(|name| name.starts_with("merge-resolved") && name.ends_with(".run"));
                observed_boundary = !obsolete_exists && successor_exists;
                observed_boundary
            });
            assert!(observed_boundary);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("construction cancelled")
            );
            drop(session);
            let mut resumed = small_session(root.path());
            complete_small(&mut resumed);
            drop(resumed);
            assert_retirement_publication(root.path());
        }
    }

    #[test]
    fn consumed_shape_root_removal_refuses_replaced_and_linked_files() {
        for extra_link in [false, true] {
            let root = TempDir::new().unwrap();
            let mut session = sealed_retirement_fixture(&root);
            inject_shape_publication_failure("shape.before_identity_retirement");
            assert!(session.prepare_canonical_encoding(1).is_err());
            let name = session
                .root
                .child_names()
                .unwrap()
                .into_iter()
                .filter_map(|s| s.into_string().ok())
                .find(|n| n.starts_with("merge-identities") && n.ends_with(".run"))
                .unwrap();
            let path = session.root.path().join(&name);
            let held = root.path().join("held-original.run");
            if extra_link {
                std::fs::hard_link(&path, &held).unwrap();
            } else {
                let bytes = std::fs::read(&path).unwrap();
                std::fs::rename(&path, &held).unwrap();
                std::fs::write(&path, bytes).unwrap();
            }
            let before = session.evidence().clone();
            assert!(
                unlink_shape_artifact(&session.root, &name, &mut session.checkpoint.evidence)
                    .is_err()
            );
            assert!(path.exists());
            assert_eq!(session.evidence(), &before);
        }
    }

    #[test]
    fn consumed_shape_roots_preserve_successor_corruption_refusal() {
        for boundary in [
            "shape.after_identity_retirement",
            "shape.after_endpoint_retirement",
        ] {
            let root = TempDir::new().unwrap();
            let mut session = sealed_retirement_fixture(&root);
            inject_shape_publication_failure(boundary);
            assert!(session.prepare_canonical_encoding(1).is_err());
            let path = session.root.path().join("shaped-identities.run");
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(path, bytes).unwrap();
            drop(session);
            assert!(
                GraphConstructionSession::open_with_mode(
                    root.path(),
                    Uuid::from_u128(119_500),
                    0,
                    graphforge_core::OntologyMode::Exploratory,
                    GraphConstructionBudgets::default()
                )
                .is_err()
            );
        }
    }
    #[test]
    fn consumed_endpoint_lookahead_refuses_a_partial_record_before_retirement() {
        for window_rows in [1, 2] {
            let temporary = TempDir::new().unwrap();
            let root = StableDirectory::open(temporary.path()).unwrap();
            let mut identity = [0_u8; BASE_IDENTITY_WIDTH];
            identity[..16].copy_from_slice(&1_u128.to_be_bytes());
            identity[24..32].copy_from_slice(&1_u64.to_be_bytes());
            std::fs::write(temporary.path().join("identities.run"), identity).unwrap();
            let mut endpoint = [0_u8; ENDPOINT_WIDTH];
            endpoint[..16].copy_from_slice(&1_u128.to_be_bytes());
            endpoint[16..32].copy_from_slice(&100_u128.to_be_bytes());
            let mut malformed = endpoint.to_vec();
            malformed.push(0xff);
            let path = temporary.path().join("endpoints.run");
            std::fs::write(&path, &malformed).unwrap();
            let mut evidence = GraphConstructionEvidence::default();
            for category in crate::ArtifactCategory::ALL {
                evidence.storage_current.insert(category, Default::default());
                evidence
                    .storage_receipt_category_authorities
                    .insert(category, Default::default());
                evidence.storage_transient_peak_allocated_bytes.insert(category, 0);
                evidence.storage_receipt_transient_peak_authorities.insert(category, 0);
            }
            let error = resolve_endpoint_surrogates(
                &root,
                "identities.run",
                Some("endpoints.run"),
                None,
                window_rows,
                2,
                &mut || false,
                &mut evidence,
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("truncated fixed-width construction run")
            );
            assert_eq!(std::fs::read(path).unwrap(), malformed);
        }
    }
}
