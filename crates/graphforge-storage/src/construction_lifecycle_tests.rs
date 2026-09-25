// Real multi-level construction measurements, sharing the writer test fixtures.
mod lifecycle_budget {
    use super::*;

    /// Shaping produced its global order by range partitioning, and the
    /// partitioning was balanced. A collapsed one-partition run would pass
    /// every byte-equality check, so the balance is the load-bearing part.
    fn assert_partitioned_shaping(evidence: &GraphConstructionEvidence) {
        assert!(evidence.shape_partitions > 1, "{}", evidence.shape_partitions);
        assert_eq!(
            evidence.shape_partition_count,
            u64::from(GraphConstructionBudgets::default().partition_count)
        );
        assert!(evidence.partition_outputs >= 3, "{}", evidence.partition_outputs);
        assert!(evidence.splitter_sample_records > 0);
        assert!(
            evidence.max_partition_identity_rows * evidence.shape_partitions
                <= evidence.partitioned_identity_rows * 4,
            "max={} partitions={} total={}",
            evidence.max_partition_identity_rows,
            evidence.shape_partitions,
            evidence.partitioned_identity_rows
        );
        assert_eq!(evidence.merge_passes, 0, "the external merge tree is gone");
    }

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
            let error = if case == "encoding" {
                // A same-inode, same-length encoded payload mutation is not
                // the reclaim sweep's to refuse: it checks identity, link
                // count and length only (the #1392 pattern applied to the
                // encoded branch). Preparation succeeds; the CAS install at
                // publication hashes the artifact and refuses it there.
                let prepared = session.prepare_canonical_encoding(1).unwrap();
                session
                    .publish_canonical(
                        &prepared,
                        Uuid::from_u128(119_601),
                        Uuid::from_u128(119_602),
                    )
                    .unwrap_err()
            } else {
                session.prepare_canonical_encoding(1).unwrap_err()
            };
            assert!(
                error.to_string().contains(match case {
                    // #1392: the completed-shape trust boundary refuses this
                    // deliberately now, instead of incidentally at retirement.
                    "shape" => "shape manifest output payload changed",
                    "encoding" => "graph object source digest or length changed during install",
                    "replacement" => "predecessor identity changed",
                    "receipt_chain" => "receipt tail changed",
                    _ => unreachable!(),
                }),
                "{case}: {error}"
            );
            if case == "encoding" {
                // The accepted trade: the sweep retired the staged predecessors
                // before publication refused, so the session cannot be
                // re-encoded from them and the import is re-run from source.
                let staging = crate::ArtifactCategory::ConstructionStaging;
                assert!(
                    session.evidence().storage_current[&staging].logical_references
                        < allocation[&staging].logical_references,
                    "{case}: predecessors were not retired"
                );
                assert_ne!(
                    session.checkpoint.publication_state,
                    Some(ConstructionPublicationState::Published)
                );
            } else {
                assert_eq!(session.evidence().storage_current, allocation);
            }
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
            {
                let version = FORMAT_VERSION;
                let root = TempDir::new().unwrap();
                crate::open_or_initialize_project(root.path()).unwrap();
                let budgets = GraphConstructionBudgets {
                    merge_fan_in: 2,
                    max_batch_rows: 4096,
                    max_run_records: 16384,
                    ..Default::default()
                };
                let mut session = GraphConstructionSession::open_internal_with_allocation(
                    root.path(),
                    root.path(),
                    Uuid::new_v4(),
                    0,
                    graphforge_core::OntologyMode::Exploratory,
                    None,
                    budgets,
                    crate::filesystem_admission::ProjectLifecycleMode::Durable,
                    None,
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
                            &edge_batch(1_000_000 + u128::from(chunk) * 4096, 1 + (u128::from(chunk) * 4096) % 8192, 8192, 4096),
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
                assert_partitioned_shaping(session.evidence());
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
                // Frozen historical source-bound budgets remain evidence; current
                // execution never recreates retired on-disk formats.
                let (retained_ceiling, peak_ceiling) = match scale {
                    1 => (11_407_360, 23_425_024),
                    2 => (21_958_656, 44_494_848),
                    4 => (43_061_248, 86_634_496),
                    _ => unreachable!("fixed fixture scales"),
                };
                assert!(current < retained_ceiling);
                assert!(peak < peak_ceiling);
                assert!(shape_peak <= peak);
                {
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

    fn retained_row_roots_fixture(root: &TempDir) -> GraphConstructionSession {
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = small_session(root.path());
        // Two independently retained exact-schema roots and no edge family.
        for input in 0..32 {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("properties-{input}"),
                    &node_property_batch(input + 1, 1),
                )
                .unwrap();
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("plain-{input}"),
                    &node_batch(input + 33, 1),
                )
                .unwrap();
        }
        session.seal().unwrap();
        session
    }

    fn assert_retained_row_roots(session: &mut GraphConstructionSession) -> Vec<ArtifactReceipt> {
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.node_count, 64);
        assert_eq!(shape.edge_count, 0);
        assert_eq!(shape.node_rows.len(), 2);
        assert!(shape.edge_rows.is_empty());
        let mut ids = Vec::new();
        let mut receipts = Vec::new();
        for name in &shape.node_rows {
            assert!(name.starts_with("shaped-rows-"), "{name}");
            receipts.push(receipt_for_existing(&session.root, name).unwrap());
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                session.root.open_child_file(OsStr::new(name)).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            let mut previous = None;
            for batch in reader {
                let batch = batch.unwrap();
                let values = uuid_column(&batch, "node_uuid").unwrap();
                for row in 0..batch.num_rows() {
                    let value = uuid_value(values, row).unwrap();
                    assert!(previous.is_none_or(|prior| prior < value));
                    previous = Some(value);
                    ids.push(value);
                }
            }
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            (1_u128..=64).map(u128::to_be_bytes).collect::<Vec<_>>()
        );
        receipts
    }

    #[test]
    fn retained_row_roots_crash_child() {
        let Ok(path) = std::env::var("GF_RETAINED_ROW_ROOTS_CRASH_ROOT") else {
            return;
        };
        small_session(Path::new(&path))
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
    }

    #[test]
    fn retained_row_roots_recover_crashes_and_reopen_without_reinstallation() {
        for failpoint in [
            "shape.row_partition.after_install",
            "shape.after_complete_inventory",
            "shape.after_evidence_checkpoint",
        ] {
            let root = TempDir::new().unwrap();
            drop(retained_row_roots_fixture(&root));
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::lifecycle_budget::retained_row_roots_crash_child")
                .arg("--nocapture")
                .env("GF_RETAINED_ROW_ROOTS_CRASH_ROOT", root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            let mut session = small_session(root.path());
            let receipts = assert_retained_row_roots(&mut session);
            let retained = session.evidence().storage_current.clone();
            let allocations = session
                .evidence()
                .storage_active_identity_allocated_bytes
                .clone();
            let groups = session.evidence().merge_groups;
            drop(session);
            for _ in 0..2 {
                let mut session = small_session(root.path());
                assert_eq!(assert_retained_row_roots(&mut session), receipts);
                assert_eq!(session.evidence().storage_current, retained);
                // Transition history is live-only. The durable identity union
                // must survive repeated recovery without installation/removal.
                assert_eq!(
                    session.evidence().storage_active_identity_allocated_bytes,
                    allocations
                );
                assert_eq!(session.evidence().merge_groups, groups);
            }
            let mut resumed = small_session(root.path());
            complete_small(&mut resumed);
            assert_eq!(
                resumed.evidence().current_merge_temporary_allocated_bytes,
                0
            );
        }
    }

    #[test]
    fn retained_row_roots_cancellation_recovers_and_corruption_fails_closed() {
        // A completed root is authoritative both in an interrupted shape and
        // after it has become the final inventory entry.
        for complete in [false, true] {
            for damage in ["none", "payload", "replacement", "extra-link"] {
                let root = TempDir::new().unwrap();
                let mut session = retained_row_roots_fixture(&root);
                let directory = session.root.path().to_path_buf();
                let name = if complete {
                    assert_retained_row_roots(&mut session)[0].name.clone()
                } else {
                    let error = session
                        .shape_canonical_with_cancellation(|| {
                            std::fs::read_dir(&directory).unwrap().any(|entry| {
                                let name = entry.unwrap().file_name();
                                let name = name.to_string_lossy();
                                name.starts_with("part-rows-") && name.ends_with(".arrow")
                            })
                        })
                        .unwrap_err();
                    assert!(error.to_string().contains("construction cancelled"));
                    session
                        .root
                        .child_names()
                        .unwrap()
                        .into_iter()
                        .filter_map(|name| name.into_string().ok())
                        .find(|name| name.starts_with("part-rows-") && name.ends_with(".arrow"))
                        .unwrap()
                };
                let path = directory.join(name);
                let mut bytes = std::fs::read(&path).unwrap();
                let held = root.path().join("held-root.parquet");
                match damage {
                    "payload" => {
                        bytes[0] ^= 1;
                        std::fs::write(&path, &bytes).unwrap();
                    }
                    "replacement" => {
                        std::fs::rename(&path, &held).unwrap();
                        std::fs::write(&path, &bytes).unwrap();
                    }
                    "extra-link" => std::fs::hard_link(&path, held).unwrap(),
                    _ => {}
                }
                let controls = shaping_recovery_controls(&directory);
                let current = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
                drop(session);
                if damage == "none" {
                    let mut resumed = small_session(root.path());
                    assert_retained_row_roots(&mut resumed);
                    complete_small(&mut resumed);
                    assert_eq!(
                        resumed.evidence().current_merge_temporary_allocated_bytes,
                        0
                    );
                    continue;
                }
                for _ in 0..2 {
                    assert!(
                        GraphConstructionSession::open_with_mode(
                            root.path(),
                            Uuid::from_u128(119_500),
                            0,
                            graphforge_core::OntologyMode::Exploratory,
                            GraphConstructionBudgets::default(),
                        )
                        .is_err(),
                        "complete={complete}, damage={damage}"
                    );
                    assert_eq!(shaping_recovery_controls(&directory), controls);
                    assert_eq!(std::fs::read(&path).unwrap(), bytes);
                    assert_eq!(
                        std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                        current
                    );
                }
            }
        }
    }

    /// #1392. The completed-shape trust boundary must refuse a same-inode,
    /// same-length payload mutation **by itself**.
    ///
    /// Before this test existed the refusal arrived incidentally, from the full
    /// SHA-256 that `retire_payload` performed just before unlinking a
    /// superseded payload. That pass is removed under #1384, so a test that
    /// only observes `open_with_mode` failing cannot distinguish the boundary
    /// working from the retirement pass masking a hole. This test names the
    /// boundary directly, and asserts in passing that the identity-only check
    /// the boundary used to perform still accepts the corruption.
    #[test]
    fn completed_shape_boundary_refuses_same_inode_payload_corruption() {
        let root = TempDir::new().unwrap();
        let mut session = shaping_recovery_fixture(&root);
        session.shape_canonical_with_cancellation(|| false).unwrap();
        let outputs = read_completed_shape_outputs(&session.root, &session.checkpoint).unwrap();
        let expected = outputs
            .iter()
            .find(|output| output.name == "shaped-identities.run")
            .expect("completed shape retains its identity run")
            .clone();

        // The untouched payload authenticates.
        let work = authenticate_shaped_output(&session.root, &expected).unwrap();
        assert_eq!(work.bytes, expected.bytes);

        let path = session.root.path().join(&expected.name);
        let before = file_identity(&File::open(&path).unwrap()).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(&path, &bytes).unwrap();

        // Same inode, same link count, same length: precisely what a writer
        // receipt cannot see.
        assert_eq!(file_identity(&File::open(&path).unwrap()).unwrap(), before);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expected.bytes);
        authenticate_shaped_output_identity(&session.root, &expected)
            .expect("the identity-only check accepts the corruption; that is the gap");

        let error = authenticate_shaped_output(&session.root, &expected).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shape manifest output payload changed"),
            "{error}"
        );
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
                        &edge_batch(1_000_000 + u128::from(chunk) * 4096, 1 + (u128::from(chunk) * 4096) % 8192, 8192, 4096),
                    )
                    .unwrap();
            }
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!((shape.node_count, shape.edge_count), (8192, 32768 * scale));
            assert_partitioned_shaping(session.evidence());
            let baseline_peak = match scale {
                1 => 20_873_216,
                2 => 40_435_712,
                4 => 79_560_704,
                _ => unreachable!(),
            };
            let retirement_peak = match scale {
                1 => 18_513_920,
                2 => 35_979_264,
                4 => 70_909_952,
                _ => unreachable!(),
            };
            assert!(
                session
                    .evidence()
                    .storage_transient_peak_total_allocated_bytes
                    <= retirement_peak - 14 * 32768 * scale
            );
            let (baseline_reads, baseline_writes) = match scale {
                1 => (54_544_436, 37_339_136),
                2 => (124_245_316, 89_112_576),
                4 => (282_652_710, 208_519_168),
                _ => unreachable!("fixed fixture scales"),
            };
            assert!(session.evidence().shape_application_read_bytes < baseline_reads);
            assert!(session.evidence().merge_written_bytes < baseline_writes);

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
                    "partition_outputs":session.evidence().partition_outputs,
                    "census":census(session.root.path()),
                })
            );
        }
    }

    #[test]
    fn range_partition_work_is_linear_across_production_chunk_counts() {
        use crate::graph_construction::partition::{IdentitySampler, PartitionBalance};
        use crate::graph_construction::partition_shaping::{
            FixedRangePartitioner, PartitionFamily,
        };

        // The external merge tree charged `records * ceil(log_32(inputs))`
        // reads and writes: 31 inputs cost 31 records of work, 1025 cost 3073.
        // Range partitioning charges exactly two passes at every size — one to
        // route, one to sort and concatenate — so the work is linear in records
        // and independent of how many chunks were staged. 272 and 1088 are the
        // accepted S20/S22 construction chunk counts.
        //
        // Each staged chunk contributes `RECORDS_PER_CHUNK` records, enough
        // that the recorded cut (identities/16) yields the full requested 16
        // partitions at every chunk count. Measuring linearity at one partition
        // would measure nothing.
        const RECORDS_PER_CHUNK: u64 = 16;
        const REQUESTED_PARTITIONS: u32 = 16;
        for chunks in [31_u64, 32, 33, 272, 1023, 1024, 1025, 1088] {
            let records = chunks * RECORDS_PER_CHUNK;
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let session = small_session(root.path());
            let mut evidence = session.evidence().clone();
            let keys = (0..records)
                .map(|index| u128::from(index + 1).to_be_bytes())
                .collect::<Vec<_>>();
            let mut sampler = IdentitySampler::new(16, records).unwrap();
            let positions = sampler.positions().collect::<Vec<_>>();
            for position in positions {
                sampler.admit(keys[usize::try_from(position).unwrap()]).unwrap();
            }
            let plan = sampler.into_plan(16).unwrap();
            assert!(plan.partitions() > 1, "{}", plan.partitions());
            let mut partitioner = FixedRangePartitioner::<16>::new(
                &session.root,
                PartitionFamily::Identities,
                plan.partitions(),
                None,
                true,
            )
            .unwrap();
            for key in &keys {
                partitioner.route(&plan, key, key, &mut evidence).unwrap();
            }
            let balance: PartitionBalance = partitioner.balance().clone();
            balance.assert_balanced("linear work").unwrap();
            let output = partitioner
                .finish_optional(
            "staged-identities.run",
            0,
            false,
            &mut || false,
            &mut evidence,
        )
                .unwrap()
                .unwrap();
            let mut file = session.root.open_child_file(OsStr::new(&output)).unwrap();
            for expected in 1..=records {
                assert_eq!(
                    read_fixed::<16>(&mut file).unwrap(),
                    Some(u128::from(expected).to_be_bytes())
                );
            }
            assert_eq!(read_fixed::<16>(&mut file).unwrap(), None);
            // Two passes: route-write plus concatenate-write, and the same on
            // the read side. Never logarithmic in the chunk count.
            assert_eq!(evidence.merge_read_records, records * 2, "chunks={chunks} records={records}");
            assert_eq!(
                evidence.merge_written_records,
                records * 2,
                "chunks={chunks} records={records}"
            );
            assert_eq!(evidence.merge_written_bytes, records * 16 * 2);
            assert_eq!(evidence.partition_rows, records, "chunks={chunks} records={records}");
            assert_eq!(evidence.merge_passes, 0, "chunks={chunks} records={records}");
            assert_eq!(evidence.partition_outputs, 1, "chunks={chunks} records={records}");
            assert!(
                evidence.peak_partition_records * u64::try_from(plan.partitions()).unwrap()
                    <= records * 4,
                "chunks={chunks} peak={}",
                evidence.peak_partition_records
            );
            // Every spill is retired once its partition has been concatenated.
            assert!(
                shape_temporary_names(&session.root).is_empty(),
                "chunks={chunks} records={records}"
            );
            println!(
                "RUNTIME_PARTITION {}",
                serde_json::json!({"chunks":chunks,"records":records,
                    "partitions":plan.partitions(),
                    "records_read":evidence.merge_read_records,
                    "records_written":evidence.merge_written_records,
                    "max_partition_rows":balance.max_rows(),
                    "outputs":evidence.partition_outputs})
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
                            name == "staged-endpoints.run"
                        } else {
                            name == "staged-identities.run"
                        }
                });
                let successor_exists = !endpoint_boundary
                    || names
                        .iter()
                        .any(|name| name.starts_with("part-resolved-") && name.ends_with(".run"));
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
                .find(|n| n == "staged-identities.run")
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
            identity[IDENTITY_SURROGATE_OFFSET..BASE_IDENTITY_WIDTH]
                .copy_from_slice(&1_u64.to_be_bytes());
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
                evidence
                    .storage_current
                    .insert(category, Default::default());
                evidence
                    .storage_receipt_category_authorities
                    .insert(category, Default::default());
                evidence
                    .storage_transient_peak_allocated_bytes
                    .insert(category, 0);
                evidence
                    .storage_receipt_transient_peak_authorities
                    .insert(category, 0);
            }
            let plan = crate::graph_construction::partition::PartitionPlan::single(1);
            let error = resolve_endpoint_surrogates(
                &root,
                &plan,
                "identities.run",
                Some("endpoints.run"),
                None,
                window_rows,
                partition::default_materialization_bytes(),
                0,
                0,
                false,
                None,
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

mod group_boundary {
    use super::*;

    /// The staged-input byte count that crosses one sealing boundary in this
    /// fixture: `partition_count: 1` puts one spill behind each family, so
    /// the cadence threshold is four spills times 255 KiB, and an 8192-row
    /// edge chunk stages well above it.
    fn boundary_budgets() -> GraphConstructionBudgets {
        GraphConstructionBudgets {
            partition_count: 1,
            ..Default::default()
        }
    }

    fn boundary_session(path: &Path, operation: u128) -> GraphConstructionSession {
        GraphConstructionSession::open_with_mode(
            path,
            Uuid::from_u128(operation),
            0,
            graphforge_core::OntologyMode::Exploratory,
            boundary_budgets(),
        )
        .unwrap()
    }

    /// Two property-free node chunks, six property-free edge chunks and two
    /// property-bearing edge chunks: every chunk crosses a sealing boundary,
    /// and the property-bearing edges exercise the row-partition resume path.
    fn stage_boundary_chunks(session: &mut GraphConstructionSession) {
        stage_boundary_groups(session, 1);
    }

    /// `groups` repetitions of the ten-chunk fixture above, over disjoint
    /// identity ranges. Every chunk still crosses a sealing boundary, so the
    /// boundary count — and with it the sealed-segment count — scales with
    /// `groups` while the shape's families and outputs do not (#1526).
    fn stage_boundary_groups(session: &mut GraphConstructionSession, groups: u128) {
        for index in 0..groups * 2 {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{index}"),
                    &node_batch(1 + index * 8192, 8192),
                )
                .unwrap();
        }
        for index in 0..groups * 6 {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{index}"),
                    &edge_batch(1_000_000 + index * 8192, 1, 8192, 8192),
                )
                .unwrap();
        }
        for index in 0..groups * 2 {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("weighted-edges-{index}"),
                    &edge_property_batch(2_000_000 + index * 8192, 8192),
                )
                .unwrap();
        }
        session.seal().unwrap();
    }

    fn complete_boundary(session: &mut GraphConstructionSession) {
        if session.state() == GraphConstructionState::Staging {
            stage_boundary_chunks(session);
        }
        let encoded = session.prepare_canonical_encoding(1).unwrap();
        session
            .publish_canonical(&encoded, Uuid::from_u128(2_119_501), Uuid::from_u128(2_119_502))
            .unwrap();
    }

    fn published_edge_count(path: &Path) -> usize {
        let selected = crate::resolve_project_generation(path).unwrap();
        let inventory =
            crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected).unwrap();
        crate::read_edges_from_inventory(
            &inventory,
            "*",
            graphforge_core::OntologyMode::Exploratory,
        )
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
    }

    fn progress_control_names(session_path: &Path) -> Vec<String> {
        std::fs::read_dir(session_path)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                name.starts_with("shape-progress-").then_some(name)
            })
            .collect()
    }

    #[test]
    fn group_boundary_crash_child() {
        let Ok(path) = std::env::var("GF_SUPERSESSION_CRASH_ROOT") else {
            return;
        };
        let mut session = boundary_session(Path::new(&path), 141_800);
        complete_boundary(&mut session);
    }

    #[test]
    fn group_boundary_crashes_resume_with_retired_inputs() {
        for boundary in ["shape.after_group_seal", "shape.after_group_retire"] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_construction::tests::group_boundary::group_boundary_crash_child",
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
            // The crash happened behind at least one durable boundary: the
            // progress chain exists. after_group_retire additionally proves
            // the retirement ran: the first chunk's staged input is gone
            // while the chain still records it as sealed.
            let session_path = root
                .path()
                .join(".graphforge-construction")
                .join(format!("{:032x}", 141_800));
            let controls = progress_control_names(&session_path);
            assert!(!controls.is_empty(), "{boundary}");
            if boundary == "shape.after_group_retire" {
                assert!(
                    !session_path
                        .join("chunk-00000000000000000000-node.parquet")
                        .exists(),
                    "{boundary}: retired staged input survived"
                );
            }
            // A clean reference run of the same fixture for ledger equality.
            let clean_root = TempDir::new().unwrap();
            crate::open_or_initialize_project(clean_root.path()).unwrap();
            let mut clean = boundary_session(clean_root.path(), 141_800);
            complete_boundary(&mut clean);
            let clean_current = clean.evidence().storage_current.clone();
            drop(clean);

            let mut recovered = boundary_session(root.path(), 141_800);
            complete_boundary(&mut recovered);
            assert!(recovered.checkpoint.inputs_retired, "{boundary}");
            assert!(recovered.checkpoint.shape_retired, "{boundary}");
            assert_eq!(
                recovered.evidence().current_merge_temporary_allocated_bytes,
                0,
                "{boundary}"
            );
            assert_eq!(
                recovered.evidence().storage_current,
                clean_current,
                "{boundary}: resumed run must reconcile every allocation"
            );
            assert_eq!(
                published_edge_count(root.path()),
                8 * 8192,
                "{boundary}"
            );
            drop(recovered);
            // The boundary controls were dead weight once supersession ran.
            let reopened = boundary_session(root.path(), 141_800);
            assert!(
                progress_control_names(&session_path).is_empty(),
                "{boundary}: progress controls outlived supersession"
            );
            assert_eq!(
                reopened.evidence().storage_current,
                clean_current,
                "{boundary}"
            );
        }
    }

    /// Files in the session directory keyed by the allocation-ledger identity
    /// each one would occupy, so a ledger entry can be named.
    fn session_identity_names(session_path: &Path) -> BTreeMap<String, String> {
        let mut names = BTreeMap::new();
        for entry in std::fs::read_dir(session_path).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file = std::fs::File::open(entry.path()).unwrap();
            let identity = file_identity(&file).unwrap();
            names.insert(
                format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id)),
                entry.file_name().to_string_lossy().into_owned(),
            );
        }
        names
    }

    fn segment_names(session_path: &Path) -> Vec<String> {
        std::fs::read_dir(session_path)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                crate::graph_construction::partition_shaping::is_partition_artifact_name(&name).then_some(name)
            })
            .collect()
    }

    fn session_directory(root: &Path, operation: u128) -> std::path::PathBuf {
        root.join(".graphforge-construction")
            .join(format!("{operation:032x}"))
    }

    /// Shape `groups` repetitions of the boundary fixture and report the
    /// shape-end durable state: boundaries sealed, the allocation ledger the
    /// shape-end checkpoint persisted, and the two control sizes.
    fn shape_end_state(
        root: &Path,
        operation: u128,
        groups: u128,
    ) -> (usize, BTreeMap<String, String>, u64, u64) {
        crate::open_or_initialize_project(root).unwrap();
        let mut session = boundary_session(root, operation);
        stage_boundary_groups(&mut session, groups);
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.edge_count, groups as u64 * 8 * 8192);
        let session_path = session_directory(root, operation);
        // Sealing happened and retention was in force: every chunk below this
        // sequence was retired behind a progress boundary, which is exactly
        // the condition under which segments are retained (#1418). The
        // progress controls themselves are already unlinked by the shape-end
        // supersession, so this is the surviving record of the boundaries.
        let boundaries = session.shape_boundary_retired_through as usize;
        assert!(boundaries >= 2, "groups={groups} boundaries={boundaries}");
        // Every segment is gone by the shape end.
        assert_eq!(
            segment_names(&session_path),
            Vec::<String>::new(),
            "groups={groups}: sealed segment survived the shape end"
        );
        let intent: ShapeIntent =
            serde_json::from_slice(&std::fs::read(session_path.join(SHAPE_INTENT)).unwrap())
                .unwrap();
        assert!(intent.complete);
        let final_evidence = intent.final_evidence.as_ref().unwrap();
        // Transition history is live operation evidence; a durable control
        // that serializes it grows with every artifact the shape installed.
        assert!(
            final_evidence.storage_allocation_transitions.is_empty(),
            "groups={groups}"
        );
        let checkpoint: Checkpoint =
            serde_json::from_slice(&std::fs::read(session_path.join(CHECKPOINT)).unwrap()).unwrap();
        // The shape-end checkpoint is the record under the 1 MiB bound, and it
        // carries exactly the intent's ledger.
        assert_eq!(
            checkpoint.evidence.storage_active_identity_allocated_bytes,
            final_evidence.storage_active_identity_allocated_bytes,
            "groups={groups}"
        );
        let names = session_identity_names(&session_path);
        let mut ledger = BTreeMap::new();
        for key in final_evidence
            .storage_active_identity_allocated_bytes
            .keys()
        {
            // No entry for a file that is gone, and none for a segment.
            let name = names
                .get(key)
                .unwrap_or_else(|| panic!("groups={groups}: ledger entry {key} names no file"));
            assert!(
                !crate::graph_construction::partition_shaping::is_partition_artifact_name(name),
                "groups={groups}: ledger retained segment {name}"
            );
            ledger.insert(key.clone(), name.clone());
        }
        let intent_bytes = std::fs::metadata(session_path.join(SHAPE_INTENT))
            .unwrap()
            .len();
        let checkpoint_bytes = std::fs::metadata(session_path.join(CHECKPOINT)).unwrap().len();
        assert!(checkpoint_bytes < MAX_CONTROL_BYTES, "groups={groups}");
        assert!(intent_bytes < MAX_SHAPE_CONTROL_BYTES, "groups={groups}");
        (boundaries, ledger, checkpoint_bytes, intent_bytes)
    }

    /// #1526. The durable shape-end controls must not grow with the number of
    /// sealed segments.
    ///
    /// This is asserted as a slope rather than a threshold: doubling the
    /// routed chunks doubles the sealing boundaries, and so the segments each
    /// boundary seals, while the shape's families and outputs stay fixed. The
    /// allocation ledger the shape-end checkpoint persists must name exactly
    /// the same files in both runs. A threshold assertion ("under 1 MiB at
    /// this size") would pass on a tree that merely still had margin, which
    /// is how #1519 reached S22 before failing.
    #[test]
    fn shape_end_controls_are_independent_of_sealed_segment_count() {
        let single = TempDir::new().unwrap();
        let (single_boundaries, single_ledger, ..) =
            shape_end_state(single.path(), 141_802, 1);
        let double = TempDir::new().unwrap();
        let (double_boundaries, double_ledger, ..) =
            shape_end_state(double.path(), 141_803, 2);
        assert!(
            double_boundaries > single_boundaries,
            "the fixture must scale its boundaries: {single_boundaries} -> {double_boundaries}"
        );
        // Same named files, and therefore the same entry count, under twice
        // the sealed segments. Keys are inode-ordered, so compare the names.
        let names = |ledger: &BTreeMap<String, String>| {
            let mut names: Vec<String> = ledger.values().cloned().collect();
            names.sort();
            names
        };
        assert_eq!(
            names(&single_ledger),
            names(&double_ledger),
            "the shape-end ledger grew with the sealed-segment count"
        );
        // The shape's own outputs are the whole ledger: nothing the routing
        // installed per boundary survives into the durable control.
        assert_eq!(names(&single_ledger).len(), 7);
    }

    #[test]
    fn shape_end_segment_discard_crash_child() {
        let Ok(path) = std::env::var("GF_SUPERSESSION_CRASH_ROOT") else {
            return;
        };
        let mut session = boundary_session(Path::new(&path), 141_804);
        stage_boundary_groups(&mut session, 1);
        session.shape_canonical_with_cancellation(|| false).unwrap();
    }

    /// #1526. The two crash windows the split retirement introduces: after the
    /// complete inventory is durable but before the segments are unlinked, and
    /// after they are unlinked but before the shape-end checkpoint. Recovery
    /// must complete with the same outputs and an exact ledger in both.
    #[test]
    fn shape_end_crashes_discard_segments_and_complete() {
        for (failpoint, segments_survive) in [
            ("shape.after_complete_inventory", true),
            ("shape.after_segment_discard", false),
        ] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_construction::tests::group_boundary::shape_end_segment_discard_crash_child",
                ])
                .env("GF_SUPERSESSION_CRASH_ROOT", root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior
            );
            let session_path = session_directory(root.path(), 141_804);
            // The positive control for the window: the complete inventory is
            // durable, and the segments it excluded from the ledger are still
            // on disk at the first failpoint and gone at the second.
            let intent: ShapeIntent =
                serde_json::from_slice(&std::fs::read(session_path.join(SHAPE_INTENT)).unwrap())
                    .unwrap();
            assert!(intent.complete, "{failpoint}");
            assert_eq!(
                !segment_names(&session_path).is_empty(),
                segments_survive,
                "{failpoint}: segment survival"
            );
            assert!(
                !progress_control_names(&session_path).is_empty(),
                "{failpoint}"
            );

            // A clean reference run of the same fixture for ledger equality.
            let clean_root = TempDir::new().unwrap();
            crate::open_or_initialize_project(clean_root.path()).unwrap();
            let mut clean = boundary_session(clean_root.path(), 141_804);
            complete_boundary(&mut clean);
            let clean_current = clean.evidence().storage_current.clone();
            drop(clean);

            let mut recovered = boundary_session(root.path(), 141_804);
            // Recovery closed the window: nothing is left to collect, and the
            // restored ledger is the reconciled one.
            assert_eq!(
                segment_names(&session_path),
                Vec::<String>::new(),
                "{failpoint}: recovery left a segment behind"
            );
            complete_boundary(&mut recovered);
            assert!(recovered.checkpoint.inputs_retired, "{failpoint}");
            assert!(recovered.checkpoint.shape_retired, "{failpoint}");
            assert_eq!(
                recovered.evidence().current_merge_temporary_allocated_bytes,
                0,
                "{failpoint}"
            );
            assert_eq!(
                recovered.evidence().storage_current,
                clean_current,
                "{failpoint}: recovered run must reconcile every allocation"
            );
            assert_eq!(published_edge_count(root.path()), 8 * 8192, "{failpoint}");
        }
    }

    #[test]
    fn group_boundary_retirement_returned_errors_retry_on_same_facade() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
        let mut session = boundary_session(root.path(), 141_801);
        stage_boundary_chunks(&mut session);
        supersession::set_returned_failure(Some("supersession.before_unlink"));
        let result = session.prepare_canonical_encoding(1);
        supersession::set_returned_failure(None);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("injected returned failure")
        );
        assert_eq!(
            std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
            prior
        );
        // The retry resumes behind the already-installed boundaries instead of
        // replaying retired inputs, and completes on the same session object.
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.edge_count, 8 * 8192);
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        session
            .publish_canonical(&encoded, Uuid::from_u128(2_119_503), Uuid::from_u128(2_119_504))
            .unwrap();
        assert_eq!(session.evidence().current_merge_temporary_allocated_bytes, 0);
        assert_eq!(published_edge_count(root.path()), 8 * 8192);
    }

    /// Content of every completed shape output, by name: identical bytes are
    /// the same graph, whatever inode or write history produced them.
    fn shape_output_content(session: &GraphConstructionSession) -> BTreeMap<String, (u64, String)> {
        read_completed_shape_outputs(&session.root, &session.checkpoint)
            .unwrap()
            .into_iter()
            .map(|receipt| (receipt.name, (receipt.bytes, receipt.sha256)))
            .collect()
    }

    fn stage_control_names(session_path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(session_path)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                name.starts_with("shape-stage-").then_some(name)
            })
            .collect();
        names.sort();
        names
    }

    #[test]
    fn finish_stage_crash_child() {
        let Ok(path) = std::env::var("GF_SUPERSESSION_CRASH_ROOT") else {
            return;
        };
        let mut session = boundary_session(Path::new(&path), 156_200);
        complete_boundary(&mut session);
    }

    fn crash_at_finish_stage(failpoint: &str) -> TempDir {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "graph_construction::tests::group_boundary::finish_stage_crash_child",
            ])
            .env("GF_SUPERSESSION_CRASH_ROOT", root.path())
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{failpoint}");
        assert_eq!(
            std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
            prior,
            "{failpoint}"
        );
        root
    }

    /// #1562. Every crash window the finish stages introduce resumes to the
    /// same graph: before a stage is durable the family re-finishes from its
    /// segments, after it the recorded successor is adopted, and in between
    /// a stranded copy on either side is discarded. The families a stage
    /// retired are really gone from disk while later stages run.
    #[test]
    #[allow(clippy::too_many_lines)] // One crash matrix; each row is a window.
    fn finish_stage_crashes_resume_with_the_same_graph() {
        let clean_root = TempDir::new().unwrap();
        crate::open_or_initialize_project(clean_root.path()).unwrap();
        let mut clean = boundary_session(clean_root.path(), 156_200);
        stage_boundary_chunks(&mut clean);
        clean.shape_canonical_with_cancellation(|| false).unwrap();
        let clean_outputs = shape_output_content(&clean);
        complete_boundary(&mut clean);
        let clean_current = clean.evidence().storage_current.clone();
        drop(clean);

        // (failpoint, stage controls durable at the crash, segment families
        // that must already be gone from disk at the crash).
        for (failpoint, stages, retired) in [
            ("shape.partition_output.after_install", 0, &[][..]),
            ("shape.stage.identities.after_install", 1, &[][..]),
            ("shape.after_derived_unlink", 1, &[][..]),
            ("shape.stage.identities.after_retire", 1, &["identities"][..]),
            ("shape.stage.node-details.after_install", 2, &["identities"][..]),
            (
                "shape.stage.node-details.after_retire",
                2,
                &["identities", "node-details"][..],
            ),
            (
                "shape.stage.edge-details.after_retire",
                3,
                &["identities", "node-details", "edge-details"][..],
            ),
            (
                "shape.stage.endpoints.after_install",
                4,
                &["identities", "node-details", "edge-details"][..],
            ),
            (
                "shape.stage.endpoints.after_retire",
                4,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.stage.assigned.after_install",
                5,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.after_identity_retirement",
                5,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.stage.resolved-routed.after_install",
                6,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.after_endpoint_retirement",
                6,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.stage.resolved.after_install",
                7,
                &["identities", "node-details", "edge-details", "endpoints"][..],
            ),
            (
                "shape.stage.resolved.after_retire",
                7,
                &[
                    "identities",
                    "node-details",
                    "edge-details",
                    "endpoints",
                    "resolved",
                ][..],
            ),
        ] {
            let root = crash_at_finish_stage(failpoint);
            let session_path = session_directory(root.path(), 156_200);
            assert_eq!(
                stage_control_names(&session_path).len(),
                stages,
                "{failpoint}: durable stages"
            );
            let segments = segment_names(&session_path);
            for family in retired {
                let prefix = format!("part-{family}-g");
                assert!(
                    !segments.iter().any(|name| name.starts_with(&prefix)),
                    "{failpoint}: retired {family} segments survived: {segments:?}"
                );
            }
            // At the resolution instant — the #1393 peak — only the resolved
            // family and the row groups may hold segments.
            if failpoint == "shape.stage.resolved-routed.after_install" {
                assert!(
                    segments
                        .iter()
                        .all(|name| name.starts_with("part-resolved-g")
                            || name.starts_with("part-rows-")),
                    "{failpoint}: {segments:?}"
                );
                assert!(
                    segments.iter().any(|name| name.starts_with("part-resolved-g")),
                    "{failpoint}: resolved segments are the stage's successor"
                );
            }

            let mut recovered = boundary_session(root.path(), 156_200);
            recovered.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(
                shape_output_content(&recovered),
                clean_outputs,
                "{failpoint}: resumed shape differs from the uninterrupted one"
            );
            complete_boundary(&mut recovered);
            assert_eq!(
                recovered.evidence().current_merge_temporary_allocated_bytes,
                0,
                "{failpoint}"
            );
            assert_eq!(
                recovered.evidence().storage_current,
                clean_current,
                "{failpoint}: resumed run must reconcile every allocation"
            );
            assert_eq!(published_edge_count(root.path()), 8 * 8192, "{failpoint}");
            drop(recovered);
            assert!(
                stage_control_names(&session_path).is_empty(),
                "{failpoint}: stage controls outlived supersession"
            );
        }
    }

    /// #1562, the #1269 class. Once a stage has retired the inputs its
    /// successor replaces, nothing can reproduce the successor's rows, so a
    /// successor or stage control mutated in place is refused rather than
    /// consumed, and the project's current generation never moves.
    #[test]
    fn finish_stage_resume_refuses_mutated_successors() {
        fn flip_last_byte(path: &Path) {
            use std::io::{Seek, SeekFrom};
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let length = file.metadata().unwrap().len();
            file.seek(SeekFrom::Start(length - 1)).unwrap();
            let mut byte = [0_u8];
            std::io::Read::read_exact(&mut file, &mut byte).unwrap();
            file.seek(SeekFrom::Start(length - 1)).unwrap();
            file.write_all(&[byte[0] ^ 0x5a]).unwrap();
            file.sync_all().unwrap();
        }
        /// Change the first digit of `key`'s numeric value, in place.
        fn bump_number(path: &Path, key: &str) {
            let body = std::fs::read_to_string(path).unwrap();
            let at = body.find(key).unwrap() + key.len();
            let digit = &body[at..=at];
            let replacement = if digit == "1" { "2" } else { "1" };
            rewrite(
                path,
                &format!("{key}{digit}"),
                &format!("{key}{replacement}"),
            );
        }
        fn rewrite(path: &Path, from: &str, to: &str) {
            let body = std::fs::read_to_string(path).unwrap();
            assert!(body.contains(from), "{from} not in {}", path.display());
            let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            // Same inode, same length: an in-place rewrite.
            assert_eq!(from.len(), to.len());
            file.write_all(body.replacen(from, to, 1).as_bytes()).unwrap();
            file.sync_all().unwrap();
        }
        type Mutation<'a> = (&'a str, &'a str, &'a dyn Fn(&Path));
        let cases: [Mutation; 4] = [
            // A family output adopted in place of its retired segments.
            ("shape.stage.edge-details.after_retire",
                "supersession payload digest changed",
                &|session: &Path| {
                flip_last_byte(&session.join("shaped-edge-details.run"));
            }),
            // Resolved segments adopted in place of the retired staged
            // endpoint domain.
            ("shape.after_endpoint_retirement",
                "supersession payload digest changed",
                &|session: &Path| {
                let segment = segment_names(session)
                    .into_iter()
                    .find(|name| name.starts_with("part-resolved-g"))
                    .unwrap();
                flip_last_byte(&session.join(segment));
            }),
            // A stage inside the chain: its successor's prior digest breaks.
            ("shape.stage.node-details.after_retire",
                "construction shape stage chain changed",
                &|session: &Path| {
                bump_number(&session.join("shape-stage-00.json"), "\"write_operations\":");
            }),
            // The head stage: its recorded receipt no longer names the bytes
            // its writer installed.
            ("shape.stage.edge-details.after_retire",
                "construction shape stage output receipt changed",
                &|session: &Path| {
                let path = session.join("shape-stage-02.json");
                let body = std::fs::read_to_string(&path).unwrap();
                let at = body.find("\"sha256\":\"").unwrap() + "\"sha256\":\"".len();
                let digit = &body[at..=at];
                let replacement = if digit == "0" { "1" } else { "0" };
                rewrite(
                    &path,
                    &format!("\"sha256\":\"{digit}"),
                    &format!("\"sha256\":\"{replacement}"),
                );
            }),
        ];
        for (failpoint, refusal, mutate) in cases {
            let root = crash_at_finish_stage(failpoint);
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let session_path = session_directory(root.path(), 156_200);
            mutate(&session_path);
            let refused = GraphConstructionSession::open_with_mode(
                root.path(),
                Uuid::from_u128(156_200),
                0,
                graphforge_core::OntologyMode::Exploratory,
                boundary_budgets(),
            )
            .and_then(|mut session| session.prepare_canonical_encoding(1).map(|_| ()));
            let error = refused
                .err()
                .unwrap_or_else(|| panic!("{failpoint}: mutated successor was consumed"));
            assert!(
                error.to_string().contains(refusal),
                "{failpoint}: refused for the wrong reason: {error}"
            );
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior,
                "{failpoint}"
            );
        }
    }

    /// #1562. A returned error inside a staged finish is recovered the way
    /// every interrupted shape is: the live facade refuses reuse without
    /// touching its evidence, and a reopen resumes to the same graph.
    #[test]
    fn finish_stage_returned_errors_refuse_reuse_until_reopen() {
        let clean_root = TempDir::new().unwrap();
        crate::open_or_initialize_project(clean_root.path()).unwrap();
        let mut clean = boundary_session(clean_root.path(), 156_201);
        stage_boundary_chunks(&mut clean);
        clean.shape_canonical_with_cancellation(|| false).unwrap();
        let clean_outputs = shape_output_content(&clean);
        drop(clean);
        for point in [
            "shape.before_identity_retirement",
            "shape.after_identity_retirement",
            "shape.before_endpoint_retirement",
            "shape.after_endpoint_retirement",
        ] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let prior = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let mut session = boundary_session(root.path(), 156_201);
            stage_boundary_chunks(&mut session);
            inject_shape_publication_failure(point);
            let error = session.prepare_canonical_encoding(1).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected shape publication failure"),
                "{point}: {error}"
            );
            // The failure landed inside the staged finish, not before it.
            assert!(
                !stage_control_names(session.root.path()).is_empty(),
                "{point}"
            );
            let before_retry = session.evidence().clone();
            assert!(
                session
                    .prepare_canonical_encoding(1)
                    .unwrap_err()
                    .to_string()
                    .contains("incomplete construction shape was not recovered"),
                "{point}"
            );
            assert_eq!(session.evidence(), &before_retry, "{point}");
            drop(session);
            let mut resumed = boundary_session(root.path(), 156_201);
            resumed.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(shape_output_content(&resumed), clean_outputs, "{point}");
            complete_boundary(&mut resumed);
            assert_eq!(
                resumed.evidence().current_merge_temporary_allocated_bytes,
                0,
                "{point}"
            );
            assert_ne!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                prior,
                "{point}: the resumed run did not publish"
            );
            assert_eq!(published_edge_count(root.path()), 8 * 8192, "{point}");
        }
    }
}
