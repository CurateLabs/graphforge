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

fn assert_same_inode_graph_object_corruption_is_refused(
    project: &Path,
    entry: &graphforge_storage::GraphFileEntry,
    ordinary_open: bool,
) {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    let object = graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
    let original_metadata = std::fs::metadata(&object).unwrap();
    let identity = graphforge_filesystem::path_identity(&object).unwrap();
    let mut writable = original_metadata.permissions();
    writable.set_readonly(false);
    std::fs::set_permissions(&object, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&object)
        .unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
    file.sync_all().unwrap();
    assert_eq!(std::fs::metadata(&object).unwrap().len(), entry.byte_length);
    assert_eq!(
        graphforge_filesystem::path_identity(&object).unwrap(),
        identity
    );

    // Resolve afresh: a previous ResolvedProjectGeneration intentionally
    // memoizes its admitted inventory for one open.
    let fresh = graphforge_storage::resolve_project_generation(project).unwrap();
    assert!(
        fresh.graph_files_inventory().is_err(),
        "{:?} object {} was accepted after in-place corruption",
        entry.role,
        entry.relative_path
    );
    drop(fresh);
    if ordinary_open {
        assert!(
            GraphForge::new(Some(project.to_str().unwrap())).is_err(),
            "ordinary open accepted corrupted {}",
            entry.relative_path
        );
    }
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    drop(file);
    std::fs::set_permissions(&object, original_metadata.permissions()).unwrap();
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

/// Retained paths under `root` that this process still holds an OS handle on.
///
/// `/proc/self/fd` is the only portable-enough way to observe the retention
/// directly; on other platforms the removal assertion below carries the
/// contract instead.
#[cfg(target_os = "linux")]
fn retained_handles_under(root: &Path) -> Vec<(bool, PathBuf)> {
    let mut retained = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fd").expect("read /proc/self/fd") {
        let entry = entry.expect("read /proc/self/fd entry");
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue;
        };
        if !target.starts_with(root) {
            continue;
        }
        let is_dir = std::fs::metadata(&target).is_ok_and(|metadata| metadata.is_dir());
        retained.push((is_dir, target));
    }
    retained.sort();
    retained
}

/// #1363 — an open persistent facade retains OS handles inside the committed
/// generations it reads from. The authenticated property inventory
/// (`AuthenticatedPropertyInventory::root`, admitted at the generation's
/// `graph_tree_root`) holds a *directory* handle on `generations/<uuid>/graph`,
/// and the resolved generations hold their `lease.lock` files.
///
/// POSIX unlinks around all three, so only Windows reports them: it refuses
/// `RemoveDirectoryW` on a directory that still has a live handle
/// (`ERROR_SHARING_VIOLATION`), and `graph` sorts before `lease.lock`, which is
/// why cleanup surfaced the directory first. Every one of these must be
/// released when the facade is released — not before it, so reads of the
/// active snapshot stay authenticated for the facade's whole life.
#[test]
fn releasing_the_facade_releases_every_committed_generation_handle() {
    let root = tempfile::TempDir::new().unwrap();
    let project = root.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let graph = GraphForge::new(project.to_str()).expect("open persistent project");
    graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();

    let generation_graph_trees = std::fs::read_dir(project.join("generations"))
        .expect("read generations")
        .map(|entry| entry.expect("generation entry").path().join("graph"))
        .filter(|tree| tree.is_dir())
        .collect::<Vec<_>>();
    assert!(
        !generation_graph_trees.is_empty(),
        "the write published a file-backed generation graph tree"
    );

    #[cfg(target_os = "linux")]
    {
        let open = retained_handles_under(&project);
        assert!(
            open.iter()
                .any(|(is_dir, path)| *is_dir
                    && generation_graph_trees.iter().any(|tree| tree == path)),
            "the open facade retains a directory handle on a generation graph tree: {open:?}"
        );
        assert!(
            open.iter()
                .any(|(_, path)| path.file_name() == Some("lease.lock".as_ref())),
            "the open facade retains its generation lease: {open:?}"
        );
    }

    drop(graph);

    #[cfg(target_os = "linux")]
    assert_eq!(
        retained_handles_under(&project),
        Vec::new(),
        "releasing the facade releases every handle inside the project"
    );

    // The load-bearing assertion on Windows: a released facade leaves nothing
    // pinned, so the project directory can be removed by its owner.
    std::fs::remove_dir_all(&project).expect("released project directory is removable");
    assert!(!project.exists());
}

/// Releasing is a *close-time* obligation, never an early one: the facade must
/// keep serving authenticated reads from the pinned generation for its whole
/// life, and the committed data must survive the release and reopen.
#[test]
fn retained_generation_handles_outlive_reads_and_survive_reopen() {
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).expect("open persistent project");
    graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    for _ in 0..3 {
        assert_eq!(
            graph
                .execute("MATCH (n:Person) RETURN n.name")
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }
    drop(graph);

    let reopened = GraphForge::new(Some(path)).expect("reopen persistent project");
    assert_eq!(
        reopened
            .execute("MATCH (n:Person) RETURN n.name")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
}

/// Regression proof for the #1425/#1388 open-path gap: a byte flipped in
/// place (same inode, same declared length) inside a *hardlink-materialized*
/// Topology-role payload object must still be refused, either at open or at
/// the latest by the first ordinary query that reads it. `topology/nodes.parquet`
/// is deliberately NOT the object
/// `compact_graph_root_reopens_through_ordinary_api_and_rematerializes`
/// corrupts: that test's `find(|entry| entry.byte_length > 0)` lands on the
/// first Properties/Catalog-role entry in file order, which is either
/// copy-and-hash materialized (control files) or independently re-hashed by
/// `property_overlay::inventory` -- neither exercises the hardlink path this
/// test targets, and neither would have caught #1425/cd964b69 removing the
/// only content check that ever covered Topology-role objects.
#[test]
fn hardlinked_topology_payload_corruption_is_refused() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    // Enough real edges that topology route files are nonempty and go
    // through the ordinary hardlink materialization path (not the
    // small-control-file copy path).
    graph
        .execute(
            "UNWIND range(1, 200) AS i \
             CREATE (a:Person {name: 'p' + toString(i), rank: i}) \
             CREATE (b:Person {name: 'q' + toString(i), rank: i}) \
             CREATE (a)-[:KNOWS {since: i}]->(b)",
        )
        .expect("seed payload data");
    publish_compact_graph_workspace(project.path(), &graph.dir());
    drop(graph);

    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let victim_entry = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/nodes.parquet")
        .expect("compact fixture contains topology/nodes.parquet");
    assert_eq!(
        victim_entry.role,
        graphforge_storage::GraphFileRole::Topology,
        "victim must exercise the hardlink path, not a control/property file"
    );
    let victim =
        graphforge_storage::graph_object_path(project.path(), &victim_entry.content_sha256)
            .unwrap();
    let before_meta = std::fs::metadata(&victim).unwrap();
    #[cfg(unix)]
    let before_ino = {
        use std::os::unix::fs::MetadataExt;
        before_meta.ino()
    };
    drop(resolved);

    // Flip exactly one byte in place: open read-write, no truncate, no
    // rename -- same inode, same length, one bit different.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut permissions = before_meta.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            permissions.set_mode(permissions.mode() | 0o200);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        std::fs::set_permissions(&victim, permissions).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&victim)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut byte = [0_u8; 1];
        std::io::Read::read_exact(&mut std::fs::File::open(&victim).unwrap(), &mut byte).unwrap();
        file.write_all(&[byte[0] ^ 0xFF]).unwrap();
        file.sync_all().unwrap();
    }
    let after_meta = std::fs::metadata(&victim).unwrap();
    assert_eq!(
        after_meta.len(),
        victim_entry.byte_length,
        "mutation must preserve declared length"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(after_meta.ino(), before_ino, "mutation must preserve inode");
    }

    // Fresh open, exactly the ordinary API surface a session uses. Either
    // the open itself refuses the corruption, or -- if it did not -- the
    // first ordinary query that reads the corrupted object must. Silently
    // returning a result computed over corrupted data is the one outcome
    // that is never acceptable, regardless of which layer catches it.
    match GraphForge::new(Some(project.path().to_str().unwrap())) {
        Err(_) => {} // Refused at open: correct, nothing more to check.
        Ok(reopened) => {
            let query_result = reopened.execute("MATCH (n:Person) RETURN count(n) AS total");
            assert!(
                query_result.is_err(),
                "corrupted hardlinked Topology payload object was accepted at open \
                 AND an ordinary query over it succeeded (rows: {:?}) -- corruption \
                 that determines a query answer was never caught",
                query_result.map(|r| r.stats.rows_produced)
            );
        }
    }
}

/// Every declared role passes through the same V2 payload admission. The
/// adjacency and search entries come from their real public build paths;
/// Delta and Other use small opaque files to exercise the role classifier
/// without requiring a journal replay or a consumer for an unknown file.
#[test]
fn compact_graph_root_refuses_same_inode_corruption_for_every_role() {
    use graphforge_storage::GraphFileRole;

    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph
        .execute("CREATE (a:Person {name:'Ada'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .unwrap();
    graph.index_adjacency().unwrap();
    graph
        .index_search(
            "Person",
            crate::SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    let workspace = graph.dir();
    publish_compact_graph_workspace(project.path(), &workspace);
    drop(graph);

    // These are real, published index artifacts, and the ordinary API must
    // refuse them at open. Keep the later opaque role fixtures out of this
    // phase so they cannot cause an unrelated journal/consumer refusal.
    let clean_open = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    drop(clean_open);
    let real = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let real_inventory = real.graph_files_inventory().unwrap().unwrap();
    for path in [
        "indexes/adjacency/index_manifest.parquet",
        "indexes/search/",
    ] {
        let entry = real_inventory
            .files
            .iter()
            .find(|entry| {
                entry.role == GraphFileRole::Index
                    && if path.ends_with('/') {
                        entry.relative_path.starts_with(path)
                    } else {
                        entry.relative_path == path
                    }
                    && entry.byte_length > 0
            })
            .unwrap_or_else(|| panic!("real Index publication lacks {path}"));
        assert_same_inode_graph_object_corruption_is_refused(project.path(), entry, true);
    }

    for (relative, bytes) in [
        ("deltas/role-proof.bin", b"delta".as_slice()),
        ("misc/role-proof.bin", b"other".as_slice()),
    ] {
        let path = workspace.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    publish_compact_graph_workspace(project.path(), &workspace);
    drop(workspace);

    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    assert!(
        inventory
            .files
            .iter()
            .any(|entry| entry.relative_path.starts_with("indexes/search/")
                && entry.role == GraphFileRole::Index),
        "a real search index must reach the compact inventory"
    );
    let selected = [
        (GraphFileRole::Topology, "topology/nodes.parquet"),
        (GraphFileRole::Properties, "properties/"),
        (
            GraphFileRole::Index,
            "indexes/adjacency/index_manifest.parquet",
        ),
        (GraphFileRole::Index, "indexes/search/"),
        (GraphFileRole::Delta, "deltas/role-proof.bin"),
        (GraphFileRole::Catalog, "semantic-routes.json"),
        (GraphFileRole::Other, "misc/role-proof.bin"),
    ];
    for (role, path) in selected {
        let entry = inventory
            .files
            .iter()
            .find(|entry| {
                entry.role == role
                    && if path.ends_with('/') {
                        entry.relative_path.starts_with(path)
                    } else {
                        entry.relative_path == path
                    }
                    && entry.byte_length > 0
            })
            .unwrap_or_else(|| panic!("missing nonempty {role:?} entry at {path}"));
        assert_same_inode_graph_object_corruption_is_refused(project.path(), entry, false);
    }
    let fresh = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    assert_eq!(fresh.graph_files_inventory().unwrap().unwrap(), inventory);
}
