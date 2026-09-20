// Included within the construction tests to reuse the real writer fixtures.
mod compact_details {
    use super::*;

    fn create(
        root: &TempDir,
        operation: Uuid,
        budgets: GraphConstructionBudgets,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> GraphConstructionSession {
        GraphConstructionSession::open_internal_with_allocation(
            root.path(),
            root.path(),
            operation,
            0,
            graphforge_core::OntologyMode::Exploratory,
            None,
            budgets,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            allocation,
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
    fn mapped_encoding_current_format_resumes_with_explicit_layout_authority() {
        {
            let version = FORMAT_VERSION;
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let operation = Uuid::new_v4();
            let budgets = GraphConstructionBudgets::default();
            let mut session = create(&root, operation, budgets, None);
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 3),
                )
                .unwrap();
            session
                .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1, 3, 2))
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
                true
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
            assert_eq!(compact.format_version, 4);
            let inventory = selected.graph_files_inventory().unwrap().unwrap();
            assert_eq!(inventory.format_version, 3);
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
        assert!(DetailCodec::from_version(FORMAT_VERSION).is_ok());
        for old in 6..FORMAT_VERSION {
            assert!(DetailCodec::from_version(old).is_err());
        }
    }

    #[test]
    fn mapped_encoding_appends_preserve_parent_routes_and_refuse_changed_table() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut initial = create(
            &root,
            Uuid::new_v4(),
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
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1, 3, 2))
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
            assert_eq!(session.checkpoint.format_version, FORMAT_VERSION);
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
                let path = old.relative_path.clone();
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
    fn detail_codec_current_format_resumes_cross_multiple_merge_levels() {
        {
            let version = FORMAT_VERSION;
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
            let mut session = create(&root, operation, budgets, Some(&allocation));
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
                        &edge_batch(10_000 + chunk * 128, 1 + (chunk * 128) % 1024, 1024, 128),
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
                let expected_width = match receipt.kind {
                    ConstructionChunkKind::Node => 16 + 1 + "Person".len(),
                    ConstructionChunkKind::Edge => 48 + 1 + "R".len(),
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
            assert!(session.evidence().shape_partitions > 1);
            assert!(session.evidence().partition_outputs >= 3);
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
            assert_eq!(detail_bytes, 1024 * (23 + 50));
            assert!(detail_allocated > 0);
            assert!(peak >= detail_allocated);
            let mut expected = Vec::new();
            for uuid in 1_u128..=1024 {
                let mut record = vec![0; NODE_DETAIL_WIDTH];
                record[..16].copy_from_slice(&uuid.to_be_bytes());
                record[16] = 6;
                record[17..23].copy_from_slice(b"Person");
                expected.push(record);
            }
            // src/dst walk the full 1024-node range across the 8 chunks and
            // wrap modulo 1024 (#1439), rather than restarting at node 1 on
            // every chunk: `edge_batch`'s `node_start` argument varies per
            // chunk to match.
            for chunk in 0_u128..8 {
                for row in 0_u128..128 {
                    let mut record = vec![0; EDGE_DETAIL_WIDTH];
                    record[..16].copy_from_slice(&(10_000 + chunk * 128 + row).to_be_bytes());
                    let src = 1 + chunk * 128 + row;
                    let dst = 1 + (chunk * 128 + row + 1) % 1024;
                    record[16..32].copy_from_slice(&src.to_be_bytes());
                    record[32..48].copy_from_slice(&dst.to_be_bytes());
                    record[48] = 1;
                    record[49] = b'R';
                    expected.push(record);
                }
            }
            assert_eq!(canonical, expected);
        }
    }
    #[test]
    fn detail_codec_partial_control_temp_preserved_and_mixed_version_refused() {
        {
            let version = FORMAT_VERSION;
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let operation = Uuid::new_v4();
            let budgets = GraphConstructionBudgets::default();
            let mut session = create(&root, operation, budgets, None);
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
            wrong.format_version = FORMAT_VERSION - 1;
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
        let root = Path::new(&path);
        let mut session = GraphConstructionSession::open_internal_with_allocation(
            root,
            root,
            Uuid::from_u128(117_200),
            0,
            graphforge_core::OntologyMode::Exploratory,
            None,
            GraphConstructionBudgets::default(),
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            None,
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1, 2, 1))
            .unwrap();
        session.seal().unwrap();
        let encoded = session.prepare_canonical_encoding(1).unwrap();
        session
            .publish_canonical(&encoded, Uuid::from_u128(117_201), Uuid::from_u128(117_202))
            .unwrap();
    }

    #[test]
    fn detail_codec_current_format_crash_replay_preserves_authority() {
        let boundaries = [
            "control.install.after_temp_fsync.checkpoint.json",
            "control.install.after_install.intent.json",
            "artifact.after_install.chunk-00000000000000000000-node.node-details.run",
            "shape.after_complete_inventory",
            "publication.after_current_before_receipt",
        ];
        let mut failed = Vec::new();
        {
            let version = FORMAT_VERSION;
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
                            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1, 2, 1))
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
    fn detail_codec_current_format_cancel_corrupt_route_and_retry() {
        use crate::graph_construction::partition::PartitionPlan;
        use crate::graph_construction::shape::route_fixed_run;
        use crate::graph_construction::partition_shaping::{
            FixedRangePartitioner, PartitionFamily,
        };
        {
            let version = FORMAT_VERSION;
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let mut session = create(
                &root,
                Uuid::new_v4(),
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
            let output = "shaped-node-details.run";
            let plan = PartitionPlan::single(1);
            fn new_partitioner<'a>(
                root: &'a crate::construction_directory::ConstructionDirectory,
                codec: DetailCodec,
            ) -> FixedRangePartitioner<'a, NODE_DETAIL_WIDTH> {
                FixedRangePartitioner::<NODE_DETAIL_WIDTH>::new(
                    root,
                    PartitionFamily::NodeDetails,
                    1,
                    Some(codec),
                    false,
                )
                .unwrap()
            }
            // Routing authenticates the staged run against its writer receipt
            // while it streams, which is the check the deleted copy step used
            // to perform. Cancellation must leave the source untouched, and an
            // abandoned partitioner must leave no owned temporary behind.
            {
                let mut partitioner = new_partitioner(&session.root, codec);
                let cancelled = route_fixed_run::<NODE_DETAIL_WIDTH>(
                    &session.root,
                    &plan,
                    &receipt,
                    Some(codec),
                    &mut partitioner,
                    &mut || true,
                    &mut session.checkpoint.evidence,
                )
                .unwrap_err();
                assert!(cancelled.to_string().contains("construction cancelled"));
            }
            assert!(shape_temporary_names(&session.root).is_empty());
            assert_eq!(std::fs::read(&path).unwrap(), original);

            let mut corrupt = original.clone();
            corrupt[17] = b'X'; // Still valid UTF-8 and ordering: digest must detect it.
            std::fs::write(&path, &corrupt).unwrap();
            {
                let mut partitioner = new_partitioner(&session.root, codec);
                let corrupted = route_fixed_run::<NODE_DETAIL_WIDTH>(
                    &session.root,
                    &plan,
                    &receipt,
                    Some(codec),
                    &mut partitioner,
                    &mut || false,
                    &mut session.checkpoint.evidence,
                )
                .unwrap_err();
                assert!(
                    corrupted.to_string().contains("source content changed"),
                    "{corrupted}"
                );
            }
            assert!(shape_temporary_names(&session.root).is_empty());
            assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);

            std::fs::write(&path, &original).unwrap();
            let mut partitioner = new_partitioner(&session.root, codec);
            route_fixed_run::<NODE_DETAIL_WIDTH>(
                &session.root,
                &plan,
                &receipt,
                Some(codec),
                &mut partitioner,
                &mut || false,
                &mut session.checkpoint.evidence,
            )
            .unwrap();
            partitioner
                .finish_optional(output, 0, false, &mut || false, &mut session.checkpoint.evidence)
                .unwrap()
                .unwrap();
            // A single partition over an already sorted run reproduces the
            // staged bytes exactly.
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
