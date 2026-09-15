use super::*;
use arrow::array::Int64Array;

fn publish_compact_graph_workspace(project: &Path, workspace: &Path) {
    use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome,
    };

    let lease = graphforge_storage::begin_graph_object_publication(project).unwrap();
    let mut state = graphforge_storage::GraphManifestState::empty();
    let (inventory, _) = graphforge_storage::capture_graph_files(workspace).unwrap();
    let mapped = inventory.format_version == graphforge_storage::GRAPH_FILES_MAPPED_RECORD_VERSION;
    let paths = inventory
        .files
        .into_iter()
        .map(|entry| PathBuf::from(entry.relative_path))
        .collect::<Vec<_>>();
    let (mut root, _) =
        graphforge_storage::append_graph_files_v2(&lease, workspace, &mut state, &paths, &[])
            .unwrap();
    if mapped {
        root.format_version = graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION;
    }
    let participant = ProjectParticipant {
        capability_id: graphforge_storage::GRAPH_CAPABILITY_ID.into(),
        capability_version: graphforge_storage::GRAPH_CAPABILITY_VERSION,
        record_family_id: graphforge_storage::GRAPH_FILES_FAMILY.into(),
        record_version: root.format_version,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            if mapped { b"graphforge-graph-files-root/4|root_node_sha256|logical_file_count|logical_byte_length|semantic-routes/1" } else { b"graphforge-graph-files-root/2|root_node_sha256|logical_file_count|logical_byte_length" },
        )
        .unwrap(),
        row_count: root.logical_file_count,
        bytes: graphforge_storage::encode_graph_files_root_v2(&root).unwrap(),
    };
    let current = graphforge_storage::resolve_project_generation(project).unwrap();
    let capabilities = current
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect();
    let mut participants = current
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|snapshot| {
            snapshot.capability_id != graphforge_storage::GRAPH_CAPABILITY_ID
                || snapshot.record_family_id != graphforge_storage::GRAPH_FILES_FAMILY
        })
        .map(|snapshot| ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "parquet" => ProjectParticipantEncoding::Parquet,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                other => panic!("unsupported fixture participant encoding {other}"),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    participants.push(participant);
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::new_v4(),
        generation_uuid: uuid::Uuid::new_v4(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        graphforge_storage::stage_project_generation(project, &request).unwrap()
    else {
        panic!("compact graph publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&lease)
        .unwrap();
}

#[test]
fn compact_graph_root_reopens_through_ordinary_api_and_rematerializes() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph
        .execute("CREATE (:Person {name: 'Ada'})")
        .expect("create compact-root fixture");
    publish_compact_graph_workspace(project.path(), &graph.dir());
    drop(graph);

    let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let result = reopened
        .execute("MATCH (n:Person) RETURN n.name AS name")
        .expect("ordinary query over compact-root generation");
    assert_eq!(result.stats.rows_produced, 1);
    // Mutable route and UUID controls are private; immutable payloads stay shared.
    let controls = [
        "semantic-routes.json",
        "topology/uuid-membership/manifest.json",
        "topology/uuid-membership/topology-receipt.json",
    ];
    assert_eq!(reopened.graph_open_evidence().files_copied, 3);
    let mut control_bytes = 0;
    for relative in controls {
        let file = std::fs::File::open(reopened.dir().join(relative)).unwrap();
        assert_eq!(graphforge_filesystem::file_link_count(&file).unwrap(), 1);
        control_bytes += file.metadata().unwrap().len();
    }
    assert_eq!(reopened.graph_open_evidence().bytes_copied, control_bytes);
    assert!(reopened.graph_open_evidence().files_reused > 0);
    assert!(!reopened.dir().join("files").exists());
    assert!(reopened.dir().join("topology").is_dir());

    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let (read_only_dir, read_only_guard, read_only_evidence) =
        hydrate_graph_workspace(&resolved, true).unwrap();
    assert!(read_only_dir.join("topology").is_dir());
    assert!(read_only_evidence.files_reused > 0);
    assert_eq!(read_only_evidence.files_opened_in_place, 0);

    let rematerialized_owner = tempfile::tempdir().unwrap();
    let rematerialized = rematerialized_owner.path().join("workspace");
    std::fs::create_dir(&rematerialized).unwrap();
    rematerialize_graph_workspace(&resolved, &rematerialized).unwrap();
    let (expected, _) = graphforge_storage::capture_graph_files(&reopened.dir()).unwrap();
    let (actual, _) = graphforge_storage::capture_graph_files(&rematerialized).unwrap();
    assert_eq!(actual, expected);

    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let victim_entry = inventory
        .files
        .iter()
        .find(|entry| entry.byte_length > 0)
        .expect("compact fixture contains a nonempty graph object");
    let victim =
        graphforge_storage::graph_object_path(project.path(), &victim_entry.content_sha256)
            .unwrap();
    drop(read_only_guard);
    drop(reopened);
    let mut permissions = std::fs::metadata(&victim).unwrap().permissions();
    permissions.set_readonly(false);
    std::fs::set_permissions(&victim, permissions).unwrap();
    std::fs::write(&victim, vec![0_u8; victim_entry.byte_length as usize]).unwrap();
    assert!(GraphForge::new(Some(project.path().to_str().unwrap())).is_err());
}

#[test]
fn compact_graph_root_replays_authoritative_deltas_into_distinct_workspace() {
    use graphforge_storage::{
        GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
        GraphDeltaPublishRequest,
    };

    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();
    let created = graph
        .execute("CREATE (n:Person) RETURN n.node_uuid")
        .unwrap();
    let ids = created.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let node_uuid = uuid::Uuid::from_slice(ids.value(0)).unwrap().to_string();
    drop(graph);
    graphforge_storage::publish_graph_delta(
        project.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: uuid::Uuid::new_v4(),
            generation_uuid: uuid::Uuid::new_v4(),
            run_uuid: uuid::Uuid::new_v4(),
            operations: vec![GraphDeltaOp {
                operation_uuid: uuid::Uuid::new_v4(),
                kind: GraphDeltaOpKind::SetNodeProperty,
                payload: GraphDeltaPayload::SetNodeProperty {
                    node_uuid,
                    property_stem: "_untyped".into(),
                    key: "rank".into(),
                    value: graphforge_storage::encode_graph_delta_value(
                        &graphforge_ir::IrLiteral::Int(7),
                    )
                    .unwrap(),
                },
            }],
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let delta_generation = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    assert!(delta_generation.graph_tree_root().join("deltas").is_dir());
    publish_compact_graph_workspace(project.path(), &delta_generation.graph_tree_root());
    drop(delta_generation);

    let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let result = reopened
        .execute("MATCH (n) RETURN count(n) AS total")
        .unwrap();
    let total = result.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 2);
    let ranks = reopened
        .execute("MATCH (n:Person) WHERE n.rank = 7 RETURN n.rank AS rank")
        .unwrap();
    assert_eq!(
        ranks
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        ranks.batches[0]
            .column_by_name("rank")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert!(!reopened.dir().join("deltas").exists());
    assert!(reopened.graph_open_evidence().files_reused > 0);
    assert!(reopened.graph_open_evidence().files_copied > 0);
}

#[test]
fn persistent_open_resolves_and_reuses_one_committed_generation() {
    let root = tempfile::tempdir().unwrap();
    let first = GraphForge::new(root.path().to_str()).expect("create v1 project");
    let generation_uuid = first.resolved_generation.generation_uuid();
    assert_eq!(first.path(), Some(root.path()));
    assert_eq!(
        std::fs::read(root.path().join(graphforge_storage::FORMAT_FILE)).unwrap(),
        graphforge_storage::PROJECT_FORMAT_BYTES
    );
    drop(first);

    let reopened = GraphForge::new(root.path().to_str()).expect("reopen v1 project");
    assert_eq!(
        reopened.resolved_generation.generation_uuid(),
        generation_uuid
    );
}

#[test]
fn property_sessions_pin_old_inventory_and_publication_installs_new_snapshot() {
    let graph = GraphForge::new(None).expect("open ephemeral project");
    graph
        .execute("CREATE (:Person {name: 'old'})")
        .expect("publish initial property generation");
    let old = graph.property_inventory_for_session();
    let old_generation = old.generation_uuid().expect("generation-backed inventory");

    graph
        .execute("MATCH (n:Person) SET n.name = 'new' RETURN n.name")
        .expect("publish replacement property generation");
    let new = graph.property_inventory_for_session();
    let new_generation = new.generation_uuid().expect("generation-backed inventory");

    assert_ne!(old_generation, new_generation);
    assert_eq!(old.generation_uuid(), Some(old_generation));
    assert_eq!(
        *graph.current_generation_uuid.lock().unwrap(),
        new_generation
    );
    assert!(!Arc::ptr_eq(&old, &new));
}

#[test]
fn concurrent_property_authority_read_never_observes_a_split_generation_pair() {
    let graph = GraphForge::new(None).expect("open ephemeral project");
    graph.execute("CREATE (:Person {name: 'old'})").unwrap();
    let old = graph.property_inventory_for_session();
    let old_uuid = old.generation_uuid().unwrap();
    graph
        .execute("MATCH (n:Person) SET n.name = 'new'")
        .unwrap();
    let new = graph.property_inventory_for_session();
    let new_uuid = new.generation_uuid().unwrap();

    for _ in 0..64 {
        *graph.property_authority.lock().unwrap() = GenerationPropertyAuthority {
            generation_uuid: old_uuid,
            inventory: Arc::clone(&old),
        };
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let authority_for_install = Arc::clone(&graph.property_authority);
        let install_barrier = Arc::clone(&barrier);
        let new_inventory = Arc::clone(&new);
        let installer = std::thread::spawn(move || {
            install_barrier.wait();
            *authority_for_install.lock().unwrap() = GenerationPropertyAuthority {
                generation_uuid: new_uuid,
                inventory: new_inventory,
            };
        });
        let authority_for_read = Arc::clone(&graph.property_authority);
        let read_barrier = Arc::clone(&barrier);
        let reader = std::thread::spawn(move || {
            read_barrier.wait();
            let authority = authority_for_read.lock().unwrap();
            (
                authority.generation_uuid,
                authority.inventory.generation_uuid(),
            )
        });
        barrier.wait();
        installer.join().unwrap();
        let observed = reader.join().unwrap();
        assert!(
            observed == (old_uuid, Some(old_uuid)) || observed == (new_uuid, Some(new_uuid)),
            "authority snapshot was split: {observed:?}"
        );
    }
}

#[test]
fn persistent_open_does_not_cleanup_generation_files() {
    let dir = tempfile::TempDir::new().unwrap();
    let first = GraphForge::new(dir.path().to_str()).unwrap();
    let topology = first.dir().join("topology");
    std::fs::create_dir_all(&topology).unwrap();
    let stale = topology.join("nodes.parquet.Abc123.tmp");
    let unrelated = topology.join("notes.tmp");
    std::fs::write(&stale, b"stale").unwrap();
    std::fs::write(&unrelated, b"keep").unwrap();
    first.publish_workspace_update().unwrap();
    drop(first);

    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();

    assert!(
        graph
            .dir()
            .join("topology/nodes.parquet.Abc123.tmp")
            .exists()
    );
    assert!(graph.dir().join("topology/notes.tmp").exists());
    assert_eq!(graph.path(), Some(dir.path()));
}
