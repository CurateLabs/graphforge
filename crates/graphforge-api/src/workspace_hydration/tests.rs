use super::*;
use arrow::array::Int64Array;

mod placement;

fn publish_compact_graph_workspace(project: &Path, workspace: &Path) {
    use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome,
    };

    let lease = graphforge_storage::begin_graph_object_publication(project).unwrap();
    let mut state = graphforge_storage::GraphManifestState::empty();
    let (inventory, _) = graphforge_storage::capture_graph_files(workspace).unwrap();
    let mapped =
        inventory.format_version == graphforge_storage::GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION;
    let paths = inventory
        .files
        .into_iter()
        .map(|entry| PathBuf::from(entry.relative_path))
        .collect::<Vec<_>>();
    let (mut root, _) =
        graphforge_storage::append_graph_files_v2(&lease, workspace, &mut state, &paths, &[])
            .unwrap();
    if mapped {
        root.format_version = graphforge_storage::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION;
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
            if mapped { b"graphforge-graph-files-root/8|root_node_sha256|logical_file_count|logical_byte_length|xxh64/1|semantic-routes/1" } else { b"graphforge-graph-files-root/6|root_node_sha256|logical_file_count|logical_byte_length|xxh64/1" },
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

/// A same-inode, same-length byte mutation of one content-store object,
/// reverted on drop so one project can exercise several payloads in turn.
struct InPlaceCorruption {
    object: PathBuf,
    file: std::fs::File,
    offset: u64,
    original: u8,
    permissions: std::fs::Permissions,
}

impl InPlaceCorruption {
    /// XOR `mask` into the byte at `offset`.
    fn apply_at(
        project: &Path,
        entry: &graphforge_storage::GraphFileEntry,
        offset: u64,
        mask: u8,
    ) -> Self {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        let object = graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
        let permissions = std::fs::metadata(&object).unwrap().permissions();
        let identity = graphforge_filesystem::path_identity(&object).unwrap();
        let mut writable = permissions.clone();
        writable.set_readonly(false);
        std::fs::set_permissions(&object, writable).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&object)
            .unwrap();
        let mut byte = [0_u8; 1];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[byte[0] ^ mask]).unwrap();
        file.sync_all().unwrap();
        assert_eq!(std::fs::metadata(&object).unwrap().len(), entry.byte_length);
        assert_eq!(
            graphforge_filesystem::path_identity(&object).unwrap(),
            identity,
            "the mutation must keep the inode"
        );
        Self {
            object,
            file,
            offset,
            original: byte[0],
            permissions,
        }
    }

    fn apply(project: &Path, entry: &graphforge_storage::GraphFileEntry) -> Self {
        Self::apply_at(project, entry, 0, 0xff)
    }
}

impl Drop for InPlaceCorruption {
    fn drop(&mut self) {
        use std::io::{Seek as _, SeekFrom, Write as _};

        self.file.seek(SeekFrom::Start(self.offset)).unwrap();
        self.file.write_all(&[self.original]).unwrap();
        self.file.sync_all().unwrap();
        std::fs::set_permissions(&self.object, self.permissions.clone()).unwrap();
    }
}

/// A read that touches one payload for the first time, reporting whether the
/// corrupted bytes were refused rather than served.
type FirstTouch<'a> = &'a dyn Fn(&GraphForge) -> bool;

/// Corruption must be refused before any result is returned: by the open when
/// something at open reads the payload, otherwise by the first read that
/// touches it. Silently answering over corrupted data is the one outcome that
/// is never acceptable (#1425), so a successful open must be followed by a
/// refusing touch. Returns whether the open itself refused.
fn assert_refused_by_open_or_first_touch(
    project: &Path,
    entry: &graphforge_storage::GraphFileEntry,
    touch: FirstTouch<'_>,
) -> bool {
    match GraphForge::new(Some(project.to_str().unwrap())) {
        Err(_) => true,
        Ok(reopened) => {
            assert!(
                touch(&reopened),
                "{:?} object {} was accepted at open and then read after in-place corruption",
                entry.role,
                entry.relative_path
            );
            false
        }
    }
}

/// The full-admission API refuses the corrupted object, while the inventory an
/// open uses decodes it by presence and exact length only (#1388).
fn assert_open_inventory_defers_content_to_first_touch(
    project: &Path,
    entry: &graphforge_storage::GraphFileEntry,
) {
    // Resolve afresh: a previous ResolvedProjectGeneration intentionally
    // memoizes its admitted inventory for one open.
    let fresh = graphforge_storage::resolve_project_generation(project).unwrap();
    assert!(
        fresh.graph_files_inventory().is_err(),
        "{:?} object {} was accepted after in-place corruption",
        entry.role,
        entry.relative_path
    );
    let fresh = graphforge_storage::resolve_project_generation(project).unwrap();
    assert!(
        fresh.unadmitted_graph_files_inventory().is_ok(),
        "{:?} object {}: the open inventory must not read payload content",
        entry.role,
        entry.relative_path
    );
    assert!(fresh.admit_all_payloads().is_err());
}

#[test]
fn persisted_graph_snapshot_is_rejected_before_publication() {
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, stage_project_generation,
    };

    let project = tempfile::tempdir().unwrap();
    let parent = graphforge_storage::open_or_initialize_project(project.path()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    std::fs::write(
        workspace.path().join("graph-content.bin"),
        b"legacy payload",
    )
    .unwrap();
    let snapshot = crate::graph_snapshot::capture(workspace.path()).unwrap();
    let mut participants = graphforge_storage::empty_workspace_participants().unwrap();
    participants.insert(0, snapshot);
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::new_v4(),
        generation_uuid: uuid::Uuid::new_v4(),
        capabilities: vec![
            ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let error = match stage_project_generation(project.path(), &request) {
        Ok(_) => panic!("legacy persisted graph snapshots must not stage"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("unsupported"), "{message}");
    assert!(message.contains("snapshot"), "{message}");
    assert_eq!(
        graphforge_storage::resolve_project_generation(project.path())
            .unwrap()
            .generation_uuid(),
        parent.generation_uuid(),
        "legacy refusal must retain the prior committed generation"
    );
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
    // The mutable route control is private; immutable payloads stay shared.
    let controls = ["semantic-routes.json"];
    assert_eq!(reopened.graph_open_evidence().files_copied, 1);
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
    // An eager sidecar: property fragments, like every bulk payload, are checked
    // on first touch and so no longer fail the open (#1388).
    let victim_entry = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/generation.json" && entry.byte_length > 0)
        .expect("compact fixture contains a nonempty generation counter");
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
    let project = tempfile::tempdir().unwrap();
    exercise_compact_replay(project.path());
}

/// A compact generation that carries an authoritative journal run over its base.
fn compact_delta_project(project: &Path) {
    use graphforge_storage::{
        GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
        GraphDeltaPublishRequest,
    };

    let graph = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
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
    // No commit publishes delta runs any more, but generations published before
    // that carry them: build one over an expanded base, as the journal API does.
    drop(crate::expanded_generation_test_support::into_expanded(
        graph,
    ));
    graphforge_storage::publish_graph_delta(
        project,
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
    let delta_generation = graphforge_storage::resolve_project_generation(project).unwrap();
    assert!(delta_generation.graph_tree_root().join("deltas").is_dir());
    publish_compact_graph_workspace(project, &delta_generation.graph_tree_root());
}

fn exercise_compact_replay(project: &Path) {
    compact_delta_project(project);
    let reopened = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
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

/// Replaying a journal reads the whole base into a private tree, so a
/// delta-bearing generation verifies its base while it materializes (the replay
/// preflight, the run loader, the tree copy and the path resolver each check it
/// again) and does not depend on first-touch admission: corruption in the base
/// fails the open. Those layers are independent, so no one removal isolates this
/// test; it guards the property that dropping the open-time sweep did not open
/// a path for them to be skipped.
#[test]
fn compact_delta_generation_refuses_base_corruption_at_open() {
    let project = tempfile::tempdir().unwrap();
    compact_delta_project(project.path());
    drop(GraphForge::new(Some(project.path().to_str().unwrap())).unwrap());
    let entry = compact_entry(
        project.path(),
        graphforge_storage::GraphFileRole::Topology,
        "topology/nodes.parquet",
        "",
    );
    // One letter of the footer's `created_by` string: no Parquet decoder reads
    // it as data, so only a checksum can refuse it.
    let object =
        graphforge_storage::graph_object_path(project.path(), &entry.content_sha256).unwrap();
    let marker = b"graphforge permanent parquet";
    let offset = std::fs::read(&object)
        .unwrap()
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("the writer stamps created_by into the footer");
    let _corruption = InPlaceCorruption::apply_at(project.path(), &entry, offset as u64 + 3, 0x01);
    assert!(
        GraphForge::new(Some(project.path().to_str().unwrap())).is_err(),
        "a delta-bearing generation was opened over a corrupted base payload"
    );
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
    // A compact root has no generation tree to hold a handle on; only an
    // expanded generation does.
    let graph = crate::expanded_generation_test_support::into_expanded(graph);

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

const COUNT_NODES: &str = "MATCH (n:Person) RETURN count(n) AS total";
const COUNT_EDGES: &str = "MATCH ()-[r]->() RETURN count(*) AS total";

/// The query is refused outright.
fn touch(query: &'static str) -> impl Fn(&GraphForge) -> bool {
    move |graph| graph.execute(query).is_err()
}

/// A compact (V2) project holding enough people and edges that every topology
/// payload is nonempty and hard-linked into the workspace.
fn compact_person_project(project: &Path) {
    let graph = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    graph
        .execute(
            "UNWIND range(1, 200) AS i \
             CREATE (a:Person {name: 'p' + toString(i), rank: i}) \
             CREATE (b:Person {name: 'q' + toString(i), rank: i}) \
             CREATE (a)-[:KNOWS {since: i}]->(b)",
        )
        .expect("seed payload data");
    // Mutating commits publish compact roots, so no conversion is needed.
    assert_compact_graph_root(project);
}

/// The project's current graph participant is a compact (V2) root.
fn assert_compact_graph_root(project: &Path) {
    let record_version = graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .participant_snapshot(
            graphforge_storage::GRAPH_CAPABILITY_ID,
            graphforge_storage::GRAPH_FILES_FAMILY,
        )
        .unwrap()
        .expect("the project records a graph participant")
        .record_version;
    assert!(
        matches!(
            record_version,
            graphforge_storage::GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
        ),
        "record version {record_version} is not a compact root"
    );
}

fn entry_at(project: &Path, prefix: &str, suffix: &str) -> graphforge_storage::GraphFileEntry {
    let resolved = graphforge_storage::resolve_project_generation(project).unwrap();
    resolved
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .find(|entry| {
            entry.relative_path.starts_with(prefix)
                && entry.relative_path.ends_with(suffix)
                && entry.byte_length > 0
        })
        .unwrap_or_else(|| panic!("compact fixture lacks a nonempty {prefix}*{suffix}"))
}

fn compact_entry(
    project: &Path,
    role: graphforge_storage::GraphFileRole,
    prefix: &str,
    suffix: &str,
) -> graphforge_storage::GraphFileEntry {
    let entry = entry_at(project, prefix, suffix);
    assert_eq!(
        entry.role, role,
        "{} has an unexpected role",
        entry.relative_path
    );
    entry
}

/// A compact project that ships both derived adjacency artifacts.
fn compact_indexed_project(project: &Path) {
    let graph = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    graph
        .execute(
            "UNWIND range(1, 50) AS i \
             CREATE (a:Person {name: 'p' + toString(i)})-[:KNOWS]->(b:Person {name: 'q' + toString(i)})",
        )
        .unwrap();
    graph.index_adjacency().unwrap();
    assert_compact_graph_root(project);
}

/// A mutation the parser cannot notice: only a checksum can see it. JSON gets
/// one whitespace byte swapped for another (or, in compact JSON, one digit
/// changed to its neighbour); Parquet gets its leading magic flipped, which no
/// reader decodes.
fn semantically_inert_corruption(
    project: &Path,
    entry: &graphforge_storage::GraphFileEntry,
) -> InPlaceCorruption {
    let object = graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
    if !entry.relative_path.ends_with(".json") {
        return InPlaceCorruption::apply(project, entry);
    }
    let bytes = std::fs::read(&object).unwrap();
    if let Some(offset) = bytes.iter().position(|byte| matches!(byte, b' ' | b'\n')) {
        // space -> tab, newline -> carriage return: both JSON whitespace.
        let mask = if bytes[offset] == b' ' { 0x29 } else { 0x07 };
        return InPlaceCorruption::apply_at(project, entry, offset as u64, mask);
    }
    match bytes.iter().position(u8::is_ascii_digit) {
        Some(offset) => InPlaceCorruption::apply_at(project, entry, offset as u64, 0x01),
        // No inert byte exists (a pointer holding one name): the parser may
        // refuse this too, so callers assert the refusal names the checksum.
        None => InPlaceCorruption::apply(project, entry),
    }
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
    // Enough real edges that topology route files are nonempty and go
    // through the ordinary hardlink materialization path (not the
    // small-control-file copy path).
    compact_person_project(project.path());
    let entry = compact_entry(
        project.path(),
        graphforge_storage::GraphFileRole::Topology,
        "topology/nodes.parquet",
        "",
    );
    let _corruption = InPlaceCorruption::apply(project.path(), &entry);
    assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
    assert_refused_by_open_or_first_touch(project.path(), &entry, &touch(COUNT_NODES));
}

/// Edge topology is read by the lazy edge scan, not by anything at open, so the
/// first recount is its first touch (#1388).
#[test]
fn hardlinked_edge_route_corruption_is_refused_on_recount() {
    let project = tempfile::tempdir().unwrap();
    compact_person_project(project.path());
    let entry = compact_entry(
        project.path(),
        graphforge_storage::GraphFileRole::Topology,
        "topology/edges/",
        ".parquet",
    );
    let _corruption = InPlaceCorruption::apply(project.path(), &entry);
    assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
    let open_refused =
        assert_refused_by_open_or_first_touch(project.path(), &entry, &touch(COUNT_EDGES));
    assert!(
        !open_refused,
        "edge topology is first touched by the recount, not at open"
    );
}

/// A mutating commit captures every file of the workspace, and capture admits a
/// hydrated payload (`capture_payload_identity`, before any digest is reused or
/// minted) that nothing has read yet. A corrupted payload is therefore refused
/// by the commit, whether the commit leaves it untouched (it would otherwise be
/// referenced again) or appends to it, rather than being carried into a new
/// generation.
#[test]
fn a_mutating_commit_does_not_launder_a_corrupted_payload() {
    for statement in [
        "CREATE (:Person {name: 'late'})",
        "MATCH (a:Person {name: 'p1'}), (b:Person {name: 'q1'}) CREATE (a)-[:KNOWS {since: 0}]->(b)",
    ] {
        let project = tempfile::tempdir().unwrap();
        compact_person_project(project.path());
        let entry = compact_entry(
            project.path(),
            graphforge_storage::GraphFileRole::Topology,
            "topology/edges/",
            ".parquet",
        );
        let _corruption = InPlaceCorruption::apply(project.path(), &entry);
        let before = graphforge_storage::resolve_project_generation(project.path())
            .unwrap()
            .generation_uuid();
        if let Ok(reopened) = GraphForge::new(Some(project.path().to_str().unwrap())) {
            assert!(
                reopened.execute(statement).is_err(),
                "{statement}: a commit over a corrupted edge payload was accepted"
            );
        }
        assert_eq!(
            graphforge_storage::resolve_project_generation(project.path())
                .unwrap()
                .generation_uuid(),
            before,
            "{statement}: the corrupted payload must not have been published into a new generation"
        );
    }
}

/// Small sidecars are read by name from many places, so hydration checks them
/// while linking the workspace and reports corruption at open, rather than
/// leaving it to be decoded later by whichever reader happens to touch it
/// first. These are the files the old open-time sweep protected that nothing at
/// open reads by content: the topology generation counters, the runtime entity
/// label marker, the surrogate tails and the runtime catalog. The adjacency
/// index manifest and the CSR shard manifests are checked on first touch
/// instead (`corrupted_adjacency_manifests_are_refused_on_first_touch`).
#[test]
fn corrupted_sidecars_are_refused_at_open() {
    let project = tempfile::tempdir().unwrap();
    compact_indexed_project(project.path());
    let clean = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    drop(clean);
    for (prefix, suffix) in [
        ("topology/generation.json", ""),
        ("topology/runtime_entity_label_encoding.json", ""),
        ("topology/surrogate_tails.parquet", ""),
        ("topology/runtime_catalog.parquet", ""),
    ] {
        let entry = entry_at(project.path(), prefix, suffix);
        let _corruption = semantically_inert_corruption(project.path(), &entry);
        assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
        assert!(
            GraphForge::new(Some(project.path().to_str().unwrap())).is_err(),
            "{} was accepted at open after in-place corruption",
            entry.relative_path
        );
    }
}

/// Whether reading the adjacency index manifest and every CSR shard manifest of
/// the open project is refused.
fn adjacency_manifests_refused(graph: &GraphForge) -> bool {
    let directory = graph.dir().join("indexes/adjacency");
    let manifest_refused = matches!(
        graphforge_storage::adjacency::read_manifest(&graph.dir()),
        Err(GfError::Validation(_))
    );
    let shard_manifest_refused = std::fs::read_dir(directory)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.to_string_lossy().ends_with(".csr.json"))
        .any(|path| {
            matches!(
                graphforge_storage::adjacency::ShardedCsrIndex::open(&path.with_extension("")),
                Err(GfError::Validation(_))
            )
        });
    manifest_refused || shard_manifest_refused
}

/// The derived adjacency index's manifests authenticate its shards, and only the
/// adjacency provider reads them, so they are checked when it first does
/// rather than at open: a project whose index is corrupt still opens and
/// answers queries that never touch the index, and the query that does is
/// refused with `GF_VALIDATION` (#1388).
#[test]
fn corrupted_adjacency_manifests_are_refused_on_first_touch() {
    let project = tempfile::tempdir().unwrap();
    compact_indexed_project(project.path());
    let clean = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    drop(clean);
    for (prefix, suffix) in [
        ("indexes/adjacency/index_manifest.parquet", ""),
        ("indexes/adjacency/", ".csr.json"),
    ] {
        let entry = entry_at(project.path(), prefix, suffix);
        let _corruption = semantically_inert_corruption(project.path(), &entry);
        assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
        assert!(
            !assert_refused_by_open_or_first_touch(
                project.path(),
                &entry,
                &adjacency_manifests_refused
            ),
            "{} must open and be refused on first touch",
            entry.relative_path
        );
    }
}

/// Every search reader begins at `current_search_artifact`, so the artifact's
/// manifest and segments are checksummed there on first touch, before any of
/// them is trusted or mapped. The pointer is the exception: the search
/// lifecycle replaces it by name, so it is copied to a private single-link
/// file and verified at open.
#[test]
#[cfg(feature = "search")]
fn corrupted_search_artifact_is_refused_on_first_touch() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph
        .execute("CREATE (:Person {name:'Ada'}), (:Person {name:'Bob'})")
        .unwrap();
    graph
        .index_search(
            "Person",
            crate::SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    publish_compact_graph_workspace(project.path(), &graph.dir());
    drop(graph);
    let key = graphforge_storage::SearchArtifactKey::text("Person", ["name"]).unwrap();
    let read = |graph: &GraphForge| graphforge_storage::current_search_artifact(&graph.dir(), &key);
    let clean = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    assert!(read(&clean).unwrap().is_some());
    drop(clean);
    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let segments = resolved
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.relative_path.starts_with("indexes/search/") && entry.byte_length > 0)
        .collect::<Vec<_>>();
    drop(resolved);
    assert!(
        segments.len() >= 4,
        "the published artifact owns a pointer, a manifest and segments"
    );
    for entry in &segments {
        let _corruption = semantically_inert_corruption(project.path(), entry);
        if entry.relative_path.ends_with("/current.json") {
            // The pointer is a single-link control, verified as it is copied,
            // so its corruption is refused at open.
            assert!(
                GraphForge::new(Some(project.path().to_str().unwrap())).is_err(),
                "{} was accepted at open after in-place corruption",
                entry.relative_path
            );
            continue;
        }
        // Every other payload under the artifact, including the manifest, is
        // refused on first touch, before it is parsed.
        let reopened = GraphForge::new(Some(project.path().to_str().unwrap()))
            .expect("opening reads no search payload");
        let error = read(&reopened).unwrap_err();
        assert!(
            error.to_string().contains("XXH64 checksum"),
            "{}: {error}",
            entry.relative_path
        );
    }
}

/// Every declared role passes through the same V2 payload admission. The
/// adjacency and search entries come from their real public build paths;
/// Delta and Other use small opaque files to exercise the role classifier
/// without requiring a journal replay or a consumer for an unknown file.
///
/// Each payload is refused before any result is returned. Bulk data (nodes,
/// edges, search segments, the adjacency manifests) is refused on its first
/// touch; the route table is authenticated by SHA-256 with the manifest;
/// everything else is checked while
/// hydrating, so even a payload no reader opens (`Other`) fails the open.
#[test]
#[cfg(feature = "search")]
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
    let other = workspace.join("misc/role-proof.bin");
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::write(other, b"other").unwrap();
    publish_compact_graph_workspace(project.path(), &workspace);

    let clean_open = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    drop(clean_open);
    let key = graphforge_storage::SearchArtifactKey::text("Person", ["name"]).unwrap();
    let find = |graph: &GraphForge| {
        graphforge_storage::current_search_artifact(&graph.dir(), &key).is_err()
    };
    let selected: [(GraphFileRole, &str, Option<FirstTouch<'_>>); 7] = [
        (
            GraphFileRole::Topology,
            "topology/nodes.parquet",
            Some(&touch(COUNT_NODES)),
        ),
        (
            GraphFileRole::Properties,
            "properties/",
            Some(&touch(PROJECT_NAMES)),
        ),
        (
            GraphFileRole::Index,
            "indexes/adjacency/index_manifest.parquet",
            Some(&adjacency_manifests_refused),
        ),
        (GraphFileRole::Index, "indexes/search/", Some(&find)),
        (GraphFileRole::Catalog, "semantic-routes.json", None),
        (GraphFileRole::Other, "misc/role-proof.bin", None),
        (GraphFileRole::Topology, "topology/generation.json", None),
    ];
    for (role, path, first_touch) in selected {
        let entry = compact_entry(project.path(), role, path, "");
        let _corruption = semantically_inert_corruption(project.path(), &entry);
        if path == "semantic-routes.json" {
            // The route table is a control object: the open inventory
            // authenticates it by SHA-256 before any path is trusted, so it is
            // refused at open and never deferred.
            let fresh = graphforge_storage::resolve_project_generation(project.path()).unwrap();
            assert!(fresh.unadmitted_graph_files_inventory().is_err());
            assert!(GraphForge::new(Some(project.path().to_str().unwrap())).is_err());
            continue;
        }
        assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
        match first_touch {
            Some(first_touch) => {
                assert_refused_by_open_or_first_touch(project.path(), &entry, first_touch);
            }
            // Checked while hydrating (small sidecars, derived adjacency
            // records) or by the property inventory at open: the open refuses.
            None => assert!(
                GraphForge::new(Some(project.path().to_str().unwrap())).is_err(),
                "{:?} object {} was accepted at open after in-place corruption",
                entry.role,
                entry.relative_path
            ),
        }
    }
    let fresh = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    assert!(fresh.graph_files_inventory().is_ok());
    drop(fresh);

    // A journal run is verified when it is replayed, so hydration does not read
    // it. An opaque fixture is not a replayable run and would fail any open for
    // an unrelated reason, so only the full-admission API is asserted here.
    let delta = workspace.join("deltas/role-proof.bin");
    std::fs::create_dir_all(delta.parent().unwrap()).unwrap();
    std::fs::write(delta, b"delta").unwrap();
    publish_compact_graph_workspace(project.path(), &workspace);
    drop(graph);
    let entry = compact_entry(
        project.path(),
        GraphFileRole::Delta,
        "deltas/role-proof.bin",
        "",
    );
    let _corruption = semantically_inert_corruption(project.path(), &entry);
    assert_open_inventory_defers_content_to_first_touch(project.path(), &entry);
}

/// `find` on a text index published in a compact project must answer, and a
/// same-inode, same-length corruption of any search payload must be refused by
/// `find` itself rather than answered or rebuilt over.
///
/// One layer refuses it: `current_search_artifact` admits the artifact
/// directory before any pointer, manifest or segment is trusted, and reports a
/// checksum refusal as a hard validation error, not as a rebuildable derived
/// index (maintainer decision 4, #1388). `find` no longer captures a
/// whole-project inventory, so nothing else stands in front of it: removing the
/// admission makes `find` answer over the corrupted segment, and mapping the
/// refusal back to `CorruptDerivedIndex` makes `find` heal it by rebuilding,
/// either of which fails this test.
#[test]
#[cfg(feature = "search")]
fn find_serves_a_compact_text_index_and_refuses_segment_corruption() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph
        .execute("CREATE (:Person {name:'Ada'}), (:Person {name:'Bob'})")
        .unwrap();
    graph
        .index_search(
            "Person",
            crate::SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    publish_compact_graph_workspace(project.path(), &graph.dir());
    drop(graph);
    let find = |graph: &GraphForge| {
        graph.find(crate::FindOptions {
            label: Some("Person".into()),
            query: Some("Ada".into()),
            limit: 3,
            ..crate::FindOptions::default()
        })
    };

    let clean = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let found = find(&clean).expect("find on a compact text index");
    assert_eq!(found.num_rows(), 1);
    drop(clean);

    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let segments = resolved
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.relative_path.starts_with("indexes/search/") && entry.byte_length > 0)
        .collect::<Vec<_>>();
    drop(resolved);
    assert!(segments.len() >= 4, "pointer, manifest and segments");
    for entry in &segments {
        let _corruption = semantically_inert_corruption(project.path(), entry);
        if entry.relative_path.ends_with("/current.json") {
            // The pointer is a single-link control, verified as it is copied.
            assert!(
                GraphForge::new(Some(project.path().to_str().unwrap())).is_err(),
                "{} was accepted at open after in-place corruption",
                entry.relative_path
            );
            continue;
        }
        let reopened = GraphForge::new(Some(project.path().to_str().unwrap()))
            .expect("opening reads no search segment");
        let pointer_before = search_pointer(&reopened);
        let error = find(&reopened).expect_err(&entry.relative_path);
        assert!(
            matches!(error, GfError::Validation(_)),
            "{}: a checksum refusal is a hard validation error, got {error:?}",
            entry.relative_path
        );
        assert!(
            error.to_string().contains("XXH64 checksum"),
            "{}: {error}",
            entry.relative_path
        );
        assert_eq!(
            search_pointer(&reopened),
            pointer_before,
            "{}: find must not rebuild over a refused index",
            entry.relative_path
        );
    }
}

/// A vector index published in a compact project must accept an upsert and
/// answer `find`: its writer lock, pointer and mutation journal are replaced
/// or opened for writing by name, so they cannot share a content-store inode.
#[test]
#[cfg(feature = "search")]
fn vector_upsert_and_find_work_on_a_compact_project() {
    use crate::{FindOptions, NodeSelector, PropValue, SearchIndexOptions};
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let alpha = graph
        .add_node(
            "Paper",
            &std::collections::HashMap::from([("title".into(), PropValue::Str("alpha".into()))]),
        )
        .unwrap();
    let beta = graph
        .add_node(
            "Paper",
            &std::collections::HashMap::from([("title".into(), PropValue::Str("beta".into()))]),
        )
        .unwrap();
    graph
        .index_search(
            "Paper",
            SearchIndexOptions::Vector {
                node: NodeSelector::Handle(alpha.clone()),
                vector: vec![1.0, 0.0],
                space: "sbert".into(),
            },
        )
        .unwrap();
    publish_compact_graph_workspace(project.path(), &graph.dir());
    drop(graph);

    let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    graph
        .index_search(
            "Paper",
            SearchIndexOptions::Vector {
                node: NodeSelector::Uuid(beta.uuid),
                vector: vec![0.0, 1.0],
                space: "sbert".into(),
            },
        )
        .expect("vector upsert on a compact project");
    let found = graph
        .find(FindOptions {
            label: Some("Paper".into()),
            vector: Some(vec![0.0, 1.0]),
            space: Some("sbert".into()),
            limit: 2,
            ..FindOptions::default()
        })
        .expect("find on a compact vector index");
    assert_eq!(found.num_rows(), 2);
    let ids = found
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), beta.uuid.as_bytes());
}

/// The current-version pointer of the `Person`/`name` text index, which a
/// rebuild replaces.
#[cfg(feature = "search")]
fn search_pointer(graph: &GraphForge) -> Vec<u8> {
    let key = graphforge_storage::SearchArtifactKey::text("Person", ["name"]).unwrap();
    std::fs::read(key.artifact_root(&graph.dir()).join("current.json")).expect("published pointer")
}

const PROJECT_NAMES: &str = "MATCH (n:Person) RETURN n.name AS v";
const FILTER_NAMES: &str = "MATCH (n:Person) WHERE n.rank = 5 RETURN n.name AS v";
const SET_RANK: &str = "MATCH (n:Person {name: 'p1'}) SET n.rank = 0";
const PROJECT_SINCE: &str = "MATCH ()-[r:KNOWS]->() RETURN r.since AS v";
const FILTER_SINCE: &str = "MATCH ()-[r:KNOWS]->() WHERE r.since = 5 RETURN r.since AS v";
const SET_SINCE: &str = "MATCH ()-[r:KNOWS {since: 1}]->() SET r.since = 0";

/// A successful first read cannot authorize later reads of changed bytes.
/// The decoder ignores the leading magic; only authentication can refuse it.
#[test]
fn property_reads_refuse_same_inode_corruption_after_a_successful_read() {
    let project = tempfile::tempdir().unwrap();
    compact_person_project(project.path());
    for (prefix, queries) in [
        ("properties/", [PROJECT_NAMES, FILTER_NAMES]),
        ("edge_properties/", [PROJECT_SINCE, FILTER_SINCE]),
    ] {
        let entry = compact_entry(
            project.path(),
            graphforge_storage::GraphFileRole::Properties,
            prefix,
            ".parquet",
        );
        for query in queries {
            let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
            let rows = |result: graphforge_exec::ExecutionResult| {
                result
                    .batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
            };
            let original_rows = rows(graph.execute(query).unwrap());
            assert!(original_rows > 0, "{query}: fixture must read properties");
            {
                let _corruption = InPlaceCorruption::apply(project.path(), &entry);
                let error = graph
                    .execute(query)
                    .expect_err("a prior admission cannot authorize changed bytes");
                assert_eq!(error.code(), "GF_PROJECT_CORRUPT", "{query}: {error}");
                assert!(
                    error.to_string().to_lowercase().contains("checksum"),
                    "{query}: {error}"
                );
            }
            assert_eq!(rows(graph.execute(query).unwrap()), original_rows);
        }
    }
}

/// A failed footer load must remain an integrity error through schema discovery.
/// It must not become a base-only schema that plans the property as NULL.
#[test]
fn property_schema_refuses_corrupted_parquet_magic_before_planning() {
    let project = tempfile::tempdir().unwrap();
    compact_person_project(project.path());
    let entry = compact_entry(
        project.path(),
        graphforge_storage::GraphFileRole::Properties,
        "properties/",
        ".parquet",
    );
    for offset in [0, entry.byte_length - 1] {
        let _corruption = InPlaceCorruption::apply_at(project.path(), &entry, offset, 0x01);
        for query in [PROJECT_NAMES, "MATCH (n:Person) RETURN n.name AS v LIMIT 0"] {
            let reopened = GraphForge::new(Some(project.path().to_str().unwrap()))
                .expect("project open must defer property payload admission");
            let error = reopened
                .execute(query)
                .expect_err("corrupt property schema must not plan NULL rows");
            assert_eq!(
                error.code(),
                "GF_PROJECT_CORRUPT",
                "offset {offset}, {query}: {error}"
            );
            let message = error.to_string();
            assert!(
                message.to_lowercase().contains("checksum"),
                "offset {offset}, {query}: {message}"
            );
        }
    }
}

/// [`compact_person_project`] plus a published text index over `name`.
#[cfg(feature = "search")]
fn compact_person_project_with_text_index(project: &Path) {
    let graph = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    graph
        .execute(
            "UNWIND range(1, 200) AS i \
             CREATE (a:Person {name: 'p' + toString(i), rank: i}) \
             CREATE (b:Person {name: 'q' + toString(i), rank: i}) \
             CREATE (a)-[:KNOWS {since: i}]->(b)",
        )
        .expect("seed payload data");
    graph
        .index_search(
            "Person",
            crate::SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    publish_compact_graph_workspace(project, &graph.dir());
}

#[cfg(feature = "search")]
fn find_person(graph: &GraphForge) -> Result<arrow::record_batch::RecordBatch, GfError> {
    graph.find(crate::FindOptions {
        label: Some("Person".into()),
        query: Some("p1".into()),
        limit: 3,
        ..crate::FindOptions::default()
    })
}

/// Opening reads no property fragment, so a fragment corrupted in place (same
/// inode, same length, the leading magic that no decoder reads) is first met by
/// a read. Every read that touches it must refuse: a Cypher projection, a
/// Cypher filter, `find` (node properties), a portable export, and a `SET`
/// that must not publish over it.
///
/// Which layer refuses which row, found by removing the first-touch admission
/// (`admit_fragment` in `property_overlay/inventory.rs`): the projection and
/// filter rows are refused only by it, since a decoder never reads the flipped
/// byte; `find` and the portable export and the `SET` have an independent layer
/// (the node/edge payload checks in front of them) and so keep refusing.
#[test]
#[cfg(feature = "search")]
fn property_fragment_corruption_is_refused_by_every_touching_read() {
    use graphforge_storage::GraphFileRole;

    let project = tempfile::tempdir().unwrap();
    compact_person_project_with_text_index(project.path());
    let clean = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    assert!(find_person(&clean).unwrap().num_rows() > 0);
    drop(clean);

    let export = |graph: &GraphForge| {
        let out = tempfile::tempdir().unwrap();
        graph
            .export_portable(crate::PortableExportRequest {
                selection: crate::PortableSelection::Current,
                output: out.path().join("export.gfportable"),
            })
            .is_err()
    };
    let find = |graph: &GraphForge| find_person(graph).is_err();
    let generation = || {
        graphforge_storage::resolve_project_generation(project.path())
            .unwrap()
            .generation_uuid()
    };
    let before = generation();
    let node_reads: [(&str, FirstTouch<'_>); 5] = [
        ("projection", &touch(PROJECT_NAMES)),
        ("filter", &touch(FILTER_NAMES)),
        ("find", &find),
        ("export", &export),
        ("set", &touch(SET_RANK)),
    ];
    let edge_reads: [(&str, FirstTouch<'_>); 4] = [
        ("projection", &touch(PROJECT_SINCE)),
        ("filter", &touch(FILTER_SINCE)),
        ("export", &export),
        ("set", &touch(SET_SINCE)),
    ];
    for (prefix, reads) in [
        ("properties/", node_reads.as_slice()),
        ("edge_properties/", edge_reads.as_slice()),
    ] {
        let entry = compact_entry(
            project.path(),
            GraphFileRole::Properties,
            prefix,
            ".parquet",
        );
        eprintln!("corrupting {}", entry.relative_path);
        let _corruption = InPlaceCorruption::apply(project.path(), &entry);
        for (name, read) in reads {
            let reopened = GraphForge::new(Some(project.path().to_str().unwrap()))
                .unwrap_or_else(|error| panic!("{prefix} {name}: open reads no property: {error}"));
            assert!(
                read(&reopened),
                "{} {name} answered over a corrupted property fragment",
                entry.relative_path
            );
        }
        let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        let error = reopened
            .execute(if prefix == "properties/" {
                PROJECT_NAMES
            } else {
                PROJECT_SINCE
            })
            .unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("checksum"),
            "{prefix}: {error}"
        );
    }
    assert_eq!(generation(), before, "a refused SET must publish nothing");
}

/// A corrupted property fragment is not a stale index. `find` reads node
/// properties to discover and bind its source, and must refuse the corruption
/// rather than treat the index as rebuildable and publish a new one over it.
#[test]
#[cfg(feature = "search")]
fn find_refuses_a_corrupted_property_fragment_instead_of_rebuilding() {
    use graphforge_storage::GraphFileRole;

    let project = tempfile::tempdir().unwrap();
    compact_person_project_with_text_index(project.path());
    let entry = compact_entry(
        project.path(),
        GraphFileRole::Properties,
        "properties/",
        ".parquet",
    );
    let _corruption = InPlaceCorruption::apply(project.path(), &entry);
    let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let pointer = search_pointer(&reopened);
    let error = find_person(&reopened).expect_err("find over a corrupted property fragment");
    assert!(
        error.to_string().to_lowercase().contains("checksum"),
        "{error}"
    );
    assert_eq!(
        search_pointer(&reopened),
        pointer,
        "find must not republish the index over a refused source"
    );
}

/// `find` must not read the graph. With every edge topology object corrupted in
/// place (so any read of one is refused), a text `find` still answers, while a
/// query that does read edges refuses: the corruption is live, and `find` never
/// touches it. Before `find` used the session's admitted inventory, its source
/// capture checksummed every payload in the project and refused here.
#[test]
#[cfg(feature = "search")]
fn find_does_not_read_edge_payloads() {
    use graphforge_storage::GraphFileRole;

    let project = tempfile::tempdir().unwrap();
    compact_person_project_with_text_index(project.path());
    let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let edges = resolved
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| {
            entry.role == GraphFileRole::Topology
                && entry.relative_path.starts_with("topology/edges/")
                && entry.byte_length > 0
        })
        .collect::<Vec<_>>();
    drop(resolved);
    assert!(!edges.is_empty(), "the fixture must publish edge objects");
    let _corruption = edges
        .iter()
        .map(|entry| InPlaceCorruption::apply(project.path(), entry))
        .collect::<Vec<_>>();
    let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    assert!(
        reopened.execute(COUNT_EDGES).is_err(),
        "the corrupted edge objects must be live"
    );
    for _ in 0..2 {
        assert!(find_person(&reopened).unwrap().num_rows() > 0);
    }
}
