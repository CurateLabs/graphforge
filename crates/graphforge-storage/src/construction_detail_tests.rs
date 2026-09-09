// Included within the construction tests to reuse the real writer fixtures.
mod compact_details {
    use super::*;

    fn create(
        root: &TempDir,
        operation: Uuid,
        version: u32,
        budgets: GraphConstructionBudgets,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> GraphConstructionSession {
        GraphConstructionSession::open_internal_with_format(
            root.path(),
            root.path(),
            operation,
            0,
            graphforge_core::OntologyMode::Exploratory,
            None,
            budgets,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            allocation,
            version,
        )
        .unwrap()
    }

    fn raw_inventory(paths: &[PathBuf]) -> (BTreeMap<String, u64>, u64) {
        let mut pending = paths.to_vec();
        let mut identities = BTreeMap::new();
        let mut references = 0_u64;
        let mut visited = 0;
        while let Some(path) = pending.pop() {
            visited += 1;
            assert!(visited < 100_000);
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                pending.extend(
                    std::fs::read_dir(path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            } else {
                assert!(metadata.is_file());
                let file = File::open(&path).unwrap();
                let identity = file_identity(&file).unwrap();
                let key = crate::storage_attribution::native_identity_key(
                    identity.volume_serial,
                    &identity.file_id,
                );
                let allocated = graphforge_filesystem::file_space_usage(&file)
                    .unwrap()
                    .allocated_bytes;
                if let Some(previous) = identities.insert(key, allocated) {
                    assert_eq!(previous, allocated);
                }
                references += 1;
            }
        }
        (identities, references)
    }

    #[test]
    fn mapped_encoding_versions_resume_with_explicit_layout_authority() {
        for version in [6, 7, 8] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let operation = Uuid::new_v4();
            let budgets = GraphConstructionBudgets::default();
            let mut session = create(&root, operation, version, budgets, None);
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session
                .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
                .unwrap();
            session.seal().unwrap();
            drop(session);
            let mut resumed =
                GraphConstructionSession::open(root.path(), operation, 0, budgets).unwrap();
            assert_eq!(resumed.checkpoint.format_version, version);
            let encoded = resumed.prepare_canonical_encoding(1).unwrap();
            assert_eq!(
                encoded
                    .artifacts
                    .iter()
                    .any(|entry| entry.path == crate::route_component::TABLE_FILE),
                version == 8
            );
            resumed
                .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
                .unwrap();
            drop(resumed);
            let selected = crate::resolve_project_generation(root.path()).unwrap();
            let crate::GraphFilesParticipant::V2(compact) = selected
                .declared_graph_files_participant()
                .unwrap()
                .unwrap()
            else {
                panic!("compact publication required")
            };
            assert_eq!(compact.format_version, if version == 8 { 4 } else { 2 });
            let inventory = selected.graph_files_inventory().unwrap().unwrap();
            assert_eq!(inventory.format_version, if version == 8 { 3 } else { 1 });
            let admitted =
                crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected).unwrap();
            let edges = crate::read_edges_from_inventory(
                &admitted,
                "*",
                graphforge_core::OntologyMode::Exploratory,
            )
            .unwrap();
            assert_eq!(edges.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            let lease = crate::begin_graph_object_publication(root.path()).unwrap();
            let (_, evidence) = crate::GraphManifestState::open(
                &lease,
                compact,
                crate::GraphManifestLimits::default(),
            )
            .unwrap();
            let expected = inventory
                .files
                .iter()
                .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
                .map_or(0, |entry| entry.byte_length);
            assert_eq!(evidence.authority_read_bytes, expected);
            assert!(evidence.decoded_bytes > 0);
        }
        assert!(DetailCodec::from_version(9).is_err());
        assert_eq!(
            DetailCodec::from_version(7).unwrap(),
            DetailCodec::from_version(8).unwrap()
        );
    }

    #[test]
    fn mapped_encoding_appends_preserve_parent_routes_and_refuse_changed_table() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut initial = create(
            &root,
            Uuid::new_v4(),
            7,
            GraphConstructionBudgets::default(),
            None,
        );
        initial
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 3),
            )
            .unwrap();
        initial
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
            .unwrap();
        initial.seal().unwrap();
        let encoded = initial.prepare_canonical_encoding(1).unwrap();
        initial
            .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
            .unwrap();
        drop(initial);
        let mut prior = crate::resolve_project_generation(root.path())
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap();
        for generation in [2, 3] {
            let (_source, mut session, shape) =
                ordinal_append_session(&root, generation, u128::from(generation + 2), 1);
            assert_eq!(session.checkpoint.format_version, 8);
            let encoding = session.encode_canonical(&shape, generation).unwrap();
            let path = session
                .root
                .path()
                .join(&encoding.root)
                .join("graph")
                .join(crate::route_component::TABLE_FILE);
            let bytes = std::fs::read(&path).unwrap();
            let table =
                crate::route_component::RouteTable::decode(&bytes, 64 * 1024 * 1024, 100_000)
                    .unwrap();
            let before = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
            let mut corrupted = bytes.clone();
            corrupted[0] ^= 1;
            std::fs::write(&path, &corrupted).unwrap();
            let target = Uuid::new_v4();
            let transaction = Uuid::new_v4();
            assert!(
                session
                    .publish_canonical(&encoding, target, transaction)
                    .is_err()
            );
            assert_eq!(
                std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
                before
            );
            std::fs::write(&path, &bytes).unwrap();
            session
                .publish_canonical(&encoding, target, transaction)
                .unwrap();
            drop(session);
            let selected = crate::resolve_project_generation(root.path()).unwrap();
            let current = selected.graph_files_inventory().unwrap().unwrap();
            assert_eq!(current.format_version, 3);
            table
                .validate_paths(
                    current
                        .files
                        .iter()
                        .map(|entry| entry.relative_path.as_str()),
                )
                .unwrap();
            for old in &prior.files {
                if crate::route_component::route_position(&old.relative_path)
                    .unwrap()
                    .is_none()
                {
                    continue;
                }
                let path = if prior.format_version == 1 {
                    crate::route_component::encode_relative_route(
                        &old.relative_path,
                        &mut crate::route_component::RouteTable::default(),
                        64 * 1024 * 1024,
                        100_000,
                    )
                    .unwrap()
                } else {
                    old.relative_path.clone()
                };
                let retained = current
                    .files
                    .iter()
                    .find(|entry| entry.relative_path == path)
                    .unwrap();
                assert_eq!(retained.content_sha256, old.content_sha256);
                assert_eq!(retained.byte_length, old.byte_length);
            }
            let admitted =
                crate::AuthenticatedPropertyInventory::from_resolved_generation(&selected).unwrap();
            assert_eq!(
                crate::read_edges_from_inventory(
                    &admitted,
                    "*",
                    graphforge_core::OntologyMode::Exploratory
                )
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
                2
            );
            prior = current;
        }
    }

    #[test]
    fn detail_codec_legacy_and_compact_resume_cross_multiple_merge_levels() {
        let mut measurements = Vec::new();
        for version in [6, 7] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let operation = Uuid::new_v4();
            let budgets = GraphConstructionBudgets {
                merge_fan_in: 2,
                max_batch_rows: 128,
                max_run_records: 512,
                ..GraphConstructionBudgets::default()
            };
            // Match first-party pre-operation setup: persistent lock files are
            // real baseline owners even though they allocate zero data blocks.
            drop(crate::begin_graph_object_publication(root.path()).unwrap());
            drop(crate::project_publication::wait_for_writer_lock(root.path()).unwrap());
            let paths = crate::StorageAllocationOperation::project_paths(root.path()).unwrap();
            let allocation = crate::StorageAllocationOperation::from_paths(&paths).unwrap();
            let mut session = create(&root, operation, version, budgets, Some(&allocation));
            // Genuine legacy output: selected before the initial checkpoint exists.
            for chunk in 0..8 {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{chunk}"),
                        &node_batch(1 + chunk * 128, 128),
                    )
                    .unwrap();
            }
            drop(session);
            let mut session = GraphConstructionSession::open_with_allocation(
                root.path(),
                root.path(),
                operation,
                0,
                graphforge_core::OntologyMode::Exploratory,
                None,
                budgets,
                crate::filesystem_admission::ProjectLifecycleMode::Durable,
                true,
                &allocation,
            )
            .unwrap();
            assert_eq!(session.checkpoint.format_version, version);
            for chunk in 0..8 {
                session
                    .append(
                        ConstructionChunkKind::Edge,
                        &format!("edges-{chunk}"),
                        &edge_batch(10_000 + chunk * 128, 128),
                    )
                    .unwrap();
            }
            let mut detail_bytes = 0;
            let mut detail_allocated = 0;
            for sequence in 0..16 {
                let mut file = session
                    .root
                    .open_child_file(OsStr::new(&receipt_name(sequence)))
                    .unwrap();
                let receipt: ConstructionChunkReceipt = decode_bounded(&mut file).unwrap();
                let expected_width = match (version, receipt.kind) {
                    (6, ConstructionChunkKind::Node) => NODE_DETAIL_WIDTH,
                    (6, ConstructionChunkKind::Edge) => EDGE_DETAIL_WIDTH,
                    (7, ConstructionChunkKind::Node) => 16 + 1 + "Person".len(),
                    (7, ConstructionChunkKind::Edge) => 48 + 1 + "R".len(),
                    _ => unreachable!(),
                };
                assert_eq!(receipt.details.bytes, receipt.rows * expected_width as u64);
                let physical = session
                    .root
                    .open_child_file(OsStr::new(&receipt.details.name))
                    .unwrap();
                let usage = graphforge_filesystem::file_space_usage(&physical).unwrap();
                assert_eq!(physical.metadata().unwrap().len(), receipt.details.bytes);
                assert_eq!(usage.allocated_bytes, receipt.details.allocated_bytes);
                detail_bytes += receipt.details.bytes;
                detail_allocated += usage.allocated_bytes;
            }
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert!(session.evidence().merge_passes >= 3);
            assert_eq!((shape.node_count, shape.edge_count), (1024, 1024));
            let outputs = [&shape.node_details, &shape.edge_details];
            let mut canonical = Vec::new();
            for (index, name) in outputs.into_iter().enumerate() {
                let mut file = session
                    .root
                    .open_child_file(OsStr::new(name.as_ref().unwrap()))
                    .unwrap();
                let codec = DetailCodec::from_version(version).unwrap();
                if index == 0 {
                    while let Some(record) = codec.read::<NODE_DETAIL_WIDTH>(&mut file).unwrap() {
                        canonical.push(record.to_vec());
                    }
                } else {
                    while let Some(record) = codec.read::<EDGE_DETAIL_WIDTH>(&mut file).unwrap() {
                        canonical.push(record.to_vec());
                    }
                }
            }
            let peak = session
                .evidence()
                .storage_transient_peak_total_allocated_bytes;
            assert!(peak > detail_allocated);
            drop(session);
            let mut session = GraphConstructionSession::open_with_allocation(
                root.path(),
                root.path(),
                operation,
                0,
                graphforge_core::OntologyMode::Exploratory,
                None,
                budgets,
                crate::filesystem_admission::ProjectLifecycleMode::Durable,
                true,
                &allocation,
            )
            .unwrap();
            assert_eq!(session.checkpoint.format_version, version);
            let replay_shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(replay_shape, shape);
            let encoded = session.encode_canonical(&shape, 1).unwrap();
            let publication = session
                .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
                .unwrap();
            let peak = session
                .evidence()
                .storage_transient_peak_total_allocated_bytes;
            let (raw, references) = raw_inventory(&paths);
            assert!(
                allocation
                    .snapshot()
                    .unwrap()
                    .matches_file_inventory(&raw, references)
            );
            assert_eq!(allocation.totals().unwrap().0, raw.values().sum::<u64>());
            drop(session);
            let mut replay = GraphConstructionSession::resume_with_mode_and_lifecycle(
                root.path(),
                operation,
                graphforge_core::OntologyMode::Exploratory,
                budgets,
                crate::filesystem_admission::ProjectLifecycleMode::Durable,
            )
            .unwrap();
            assert_eq!(replay.checkpoint.format_version, version);
            let repeated = replay
                .publish_canonical(
                    &encoded,
                    publication.generation_uuid,
                    publication.transaction_uuid,
                )
                .unwrap();
            assert_eq!(repeated.generation_uuid, publication.generation_uuid);
            measurements.push((detail_bytes, detail_allocated, peak, canonical));
        }
        assert_eq!(measurements[0].3, measurements[1].3);
        assert!(measurements[1].0 < measurements[0].0);
        assert!(measurements[1].1 < measurements[0].1);
        assert!(measurements[1].2 < measurements[0].2);
        eprintln!(
            "legacy/compact detail EOF, allocation, construction peak: {:?} {:?}",
            (measurements[0].0, measurements[0].1, measurements[0].2),
            (measurements[1].0, measurements[1].1, measurements[1].2)
        );
    }
    #[test]
    fn detail_codec_partial_control_temp_preserved_and_mixed_version_refused() {
        for version in [6, 7] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let operation = Uuid::new_v4();
            let budgets = GraphConstructionBudgets::default();
            let mut session = create(&root, operation, version, budgets, None);
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
                .unwrap();
            let private = session.root.path().to_path_buf();
            let partial = private.join(control_temp(CHECKPOINT));
            std::fs::write(&partial, b"{\"format_version\":").unwrap();
            drop(session);
            let session =
                GraphConstructionSession::open(root.path(), operation, 0, budgets).unwrap();
            assert_eq!(session.checkpoint.format_version, version);
            assert_eq!(std::fs::read(&partial).unwrap(), b"{\"format_version\":");
            let mut wrong = session.checkpoint.clone();
            wrong.format_version = if version == 6 { 7 } else { 6 };
            let mixed = private.join(control_temp(CHECKPOINT));
            let bytes = serde_json::to_vec(&wrong).unwrap();
            std::fs::write(&mixed, &bytes).unwrap();
            let current = std::fs::read(root.path().join("CURRENT")).unwrap();
            let checkpoint = std::fs::read(private.join(CHECKPOINT)).unwrap();
            drop(session);
            let error = GraphConstructionSession::open(root.path(), operation, 0, budgets)
                .err()
                .expect("mixed control rejected");
            assert!(
                error
                    .to_string()
                    .contains("temporary control version differs"),
                "{error}"
            );
            assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
            assert_eq!(std::fs::read(private.join(CHECKPOINT)).unwrap(), checkpoint);
            assert_eq!(std::fs::read(&mixed).unwrap(), bytes);
            std::fs::remove_file(mixed).unwrap();
            let mut invalid: serde_json::Value = serde_json::from_slice(&checkpoint).unwrap();
            invalid["format_version"] = 99.into();
            let invalid = serde_json::to_vec(&invalid).unwrap();
            std::fs::write(private.join(CHECKPOINT), &invalid).unwrap();
            assert!(GraphConstructionSession::open(root.path(), operation, 0, budgets).is_err());
            assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
            assert_eq!(std::fs::read(private.join(CHECKPOINT)).unwrap(), invalid);
        }
    }
    #[test]
    fn detail_codec_crash_child() {
        let Ok(path) = std::env::var("GF_DETAIL_CODEC_CRASH_ROOT") else {
            return;
        };
        let version: u32 = std::env::var("GF_DETAIL_CODEC_VERSION")
            .unwrap()
            .parse()
            .unwrap();
        let root = Path::new(&path);
        let mut session = GraphConstructionSession::open_internal_with_format(
            root,
            root,
            Uuid::from_u128(117_200),
            0,
            graphforge_core::OntologyMode::Exploratory,
            None,
            GraphConstructionBudgets::default(),
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            None,
            version,
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1))
            .unwrap();
        session.seal().unwrap();
        let encoded = session.prepare_canonical_encoding(1).unwrap();
        session
            .publish_canonical(&encoded, Uuid::from_u128(117_201), Uuid::from_u128(117_202))
            .unwrap();
    }

    #[test]
    fn detail_codec_both_versions_crash_replay_preserves_authority() {
        let boundaries = [
            "control.install.after_temp_fsync.checkpoint.json",
            "control.install.after_install.intent.json",
            "artifact.after_install.chunk-00000000000000000000-node.node-details.run",
            "shape.after_complete_inventory",
            "publication.after_current_before_receipt",
        ];
        let mut failed = Vec::new();
        for version in [6, 7] {
            for boundary in boundaries {
                let root = TempDir::new().unwrap();
                crate::open_or_initialize_project(root.path()).unwrap();
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "graph_construction::tests::compact_details::detail_codec_crash_child",
                        "--nocapture",
                    ])
                    .env("GF_DETAIL_CODEC_CRASH_ROOT", root.path())
                    .env("GF_DETAIL_CODEC_VERSION", version.to_string())
                    .env(
                        "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                        "graphforge-construction-test-v1",
                    )
                    .env("GF_CONSTRUCTION_FAILPOINT", boundary)
                    .status()
                    .unwrap();
                if status.code() != Some(86) {
                    failed.push(format!("v{version} {boundary}: {status}"));
                    continue;
                }
                let mut resumed = if boundary == "control.install.after_temp_fsync.checkpoint.json"
                {
                    // No installed checkpoint exists yet: use the ordinary create-or-reopen entry.
                    GraphConstructionSession::open(
                        root.path(),
                        Uuid::from_u128(117_200),
                        0,
                        GraphConstructionBudgets::default(),
                    )
                    .unwrap()
                } else {
                    GraphConstructionSession::resume_with_mode_and_lifecycle(
                        root.path(),
                        Uuid::from_u128(117_200),
                        graphforge_core::OntologyMode::Exploratory,
                        GraphConstructionBudgets::default(),
                        crate::filesystem_admission::ProjectLifecycleMode::Durable,
                    )
                    .unwrap()
                };
                assert_eq!(resumed.checkpoint.format_version, version);
                if resumed.state() == GraphConstructionState::Staging {
                    if resumed.accepted_chunks() == 0 {
                        resumed
                            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
                            .unwrap();
                    }
                    if resumed.accepted_chunks() == 1 {
                        resumed
                            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1))
                            .unwrap();
                    }
                    resumed.seal().unwrap();
                }
                let encoded = resumed.prepare_canonical_encoding(1).unwrap();
                let receipt = resumed
                    .publish_canonical(&encoded, Uuid::from_u128(117_201), Uuid::from_u128(117_202))
                    .unwrap();
                assert_eq!(receipt.generation_uuid, Uuid::from_u128(117_201));
                let repeated = resumed
                    .publish_canonical(&encoded, Uuid::from_u128(117_201), Uuid::from_u128(117_202))
                    .unwrap();
                assert_eq!(repeated.generation_uuid, receipt.generation_uuid);
                assert_eq!(
                    crate::resolve_project_generation(root.path())
                        .unwrap()
                        .generation_uuid(),
                    receipt.generation_uuid
                );
            }
        }
        assert!(failed.is_empty(), "{failed:?}");
    }
    #[test]
    fn detail_codec_both_versions_cancel_corrupt_copy_and_retry() {
        for version in [6, 7] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let mut session = create(
                &root,
                Uuid::new_v4(),
                version,
                GraphConstructionBudgets::default(),
                None,
            );
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
                .unwrap();
            let receipt = session.read_receipt(0).unwrap().details;
            let path = session.root.path().join(&receipt.name);
            let original = std::fs::read(&path).unwrap();
            let current = std::fs::read(root.path().join("CURRENT")).unwrap();
            let codec = DetailCodec::from_version(version).unwrap();
            let output = "merge-node-codec-retry.run";
            let cancelled = copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
                &session.root,
                &receipt,
                output,
                &mut || true,
                &mut session.checkpoint.evidence,
                Some(codec),
            )
            .unwrap_err();
            assert!(cancelled.to_string().contains("construction cancelled"));
            assert!(shape_temporary_names(&session.root).is_empty());
            assert!(!session.root.path().join(output).exists());
            assert_eq!(std::fs::read(&path).unwrap(), original);
            let mut corrupt = original.clone();
            corrupt[17] = b'X'; // Still valid UTF-8 and ordering: digest must detect it.
            std::fs::write(&path, &corrupt).unwrap();
            let corrupted = copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
                &session.root,
                &receipt,
                output,
                &mut || false,
                &mut session.checkpoint.evidence,
                Some(codec),
            )
            .unwrap_err();
            assert!(
                corrupted.to_string().contains("source content changed"),
                "{corrupted}"
            );
            assert!(shape_temporary_names(&session.root).is_empty());
            assert!(!session.root.path().join(output).exists());
            assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
            std::fs::write(&path, &original).unwrap();
            copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
                &session.root,
                &receipt,
                output,
                &mut || false,
                &mut session.checkpoint.evidence,
                Some(codec),
            )
            .unwrap();
            assert_eq!(
                std::fs::read(session.root.path().join(output)).unwrap(),
                original
            );
            assert!(shape_temporary_names(&session.root).is_empty());
        }
    }
    #[test]
    fn detail_codec_initial_temporary_rejects_unknown_conflicting_and_changed_binding() {
        for case in ["unknown", "conflict", "budgets", "noninitial"] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_construction::tests::compact_details::detail_codec_crash_child",
                    "--nocapture",
                ])
                .env("GF_DETAIL_CODEC_CRASH_ROOT", root.path())
                .env("GF_DETAIL_CODEC_VERSION", "6")
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env(
                    "GF_CONSTRUCTION_FAILPOINT",
                    "control.install.after_temp_fsync.checkpoint.json",
                )
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86));
            let private = root
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(117_200).simple().to_string());
            let temporary = std::fs::read_dir(&private)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with(".checkpoint.json-")
                })
                .unwrap();
            let mut candidate: Checkpoint =
                serde_json::from_slice(&std::fs::read(&temporary).unwrap()).unwrap();
            match case {
                "unknown" => candidate.format_version = 99,
                "conflict" => candidate.format_version = 7,
                "budgets" => candidate.budgets.max_batch_rows /= 2,
                "noninitial" => candidate.saw_edge = true,
                _ => unreachable!(),
            }
            let destination = if case == "conflict" {
                private.join(control_temp(CHECKPOINT))
            } else {
                temporary
            };
            let bytes = serde_json::to_vec(&candidate).unwrap();
            std::fs::write(&destination, &bytes).unwrap();
            let current = std::fs::read(root.path().join("CURRENT")).unwrap();
            let error = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(117_200),
                0,
                GraphConstructionBudgets::default(),
            )
            .err()
            .expect("invalid initial authority rejected");
            assert!(
                error.to_string().contains("checkpoint"),
                "case={case}: {error}"
            );
            assert!(!private.join(CHECKPOINT).exists());
            assert_eq!(std::fs::read(destination).unwrap(), bytes);
            assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
        }
    }
}
