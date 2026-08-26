//! Deterministic CI fixture for file-backed graph generations (#338).
//!
//! Proves the public Rust facade can publish and reopen a multi-file graph
//! without assembling the workspace into one Arrow snapshot payload.
//!
//! Large-class evidence (> legacy 2 GiB snapshot envelope) is an ignored manual
//! test in this file. It does not download external 8M/128M fixtures in CI.
//! Reproduce with:
//! `GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT=build/file-backed-oversize-evidence.json \
//!  cargo test -p graphforge-api --test file_backed_graph_generation \
//!  oversize_file_backed_generation_exceeds_legacy_snapshot_envelope -- --ignored --nocapture`

#[path = "support/project_fixture.rs"]
mod project_fixture;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{FixedSizeBinaryArray, Int64Array};
use graphforge_api::{
    CheckpointRequest, GraphForge, OperationId, PortableExportRequest, PortableSelection,
    PortableV2ExportRequest, PortableV2ImportRequest, PortableVerifyRequest, verify_portable_v2,
};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GRAPH_FILES_FAMILY, GraphFileRole,
    GraphFilesOpenStrategy, GraphWriter, GraphWriterLimits, PortableV2GraphSelector,
    PortableV2Limits, PortableV2Mode, PortableV2Output, PortableV2PackageClass,
    PortableV2PropertyProjection, PortableV2SelectionProfile, PortableV2SubsetClosure,
    PortableV2SubsetRequest, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
    UuidIndexKind, UuidMembershipIndex, capture_graph_files, empty_workspace_participants,
    resolve_project_generation, stage_project_generation_with_graph_tree,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Legacy graph snapshot envelope total (must remain exceeded by oversize evidence).
const LEGACY_SNAPSHOT_ENVELOPE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Deterministic oversize padding used for the manual large-class proof.
const OVERSIZE_PADDING_BYTES: u64 = LEGACY_SNAPSHOT_ENVELOPE_BYTES + 64 * 1024 * 1024;

/// Serializes the process-global storage counters used by the acceptance proof.
static IO_STATS_LOCK: Mutex<()> = Mutex::new(());

fn result_fingerprint(result: &graphforge_api::ExecutionResult) -> String {
    let mut hasher = Sha256::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            for column in batch.columns() {
                let value = arrow::util::display::array_value_to_string(column, row)
                    .expect("canonical Arrow display value");
                hasher.update(value.len().to_le_bytes());
                hasher.update(value.as_bytes());
            }
        }
    }
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!("sha256:{encoded}")
}

fn scalar_i64(result: &graphforge_api::ExecutionResult) -> i64 {
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("scalar is Int64")
        .value(0)
}

fn scalar_uuid(result: &graphforge_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("UUID scalar is FixedSizeBinary(16)");
    Uuid::from_slice(values.value(0)).expect("valid UUID bytes")
}

fn inventory_digests(root: &Path) -> BTreeMap<String, String> {
    resolve_project_generation(root)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .map(|entry| (entry.relative_path, entry.content_sha256))
        .collect()
}

fn property_digests(root: &Path) -> BTreeMap<String, String> {
    resolve_project_generation(root)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.role == GraphFileRole::Properties)
        .map(|entry| (entry.relative_path, entry.content_sha256))
        .collect()
}

#[test]
fn sharded_graph_uses_ordinary_reopen_query_and_portable_round_trip() {
    let _io_guard = IO_STATS_LOCK.lock().expect("I/O stats lock");
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let source_path = source.to_str().unwrap();
    let graph = GraphForge::new(Some(source_path)).unwrap();
    graph
        .execute("CREATE (:Person {name:'Ada'})-[:KNOWS]->(:Person {name:'Bob'})")
        .unwrap();
    graph
        .execute("CREATE (:Person {name:'Cy'})-[:KNOWS]->(:Person {name:'Di'})")
        .unwrap();
    drop(graph);

    let generation = resolve_project_generation(&source).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let node_fragments = inventory
        .files
        .iter()
        .filter(|entry| {
            entry.relative_path == "topology/nodes.parquet"
                || entry.relative_path.starts_with("topology/nodes/")
        })
        .count();
    let edge_fragments = inventory
        .files
        .iter()
        .filter(|entry| {
            entry.relative_path.starts_with("topology/edges/")
                && entry.relative_path.ends_with(".parquet")
        })
        .count();
    assert_eq!(node_fragments, 2, "both immutable node shards must publish");
    assert_eq!(edge_fragments, 2, "both immutable edge shards must publish");

    let reopened = GraphForge::new(Some(source_path)).unwrap();
    assert_eq!(reopened.node_count("Person").unwrap(), 4);
    let properties_before = property_digests(&source);
    assert_eq!(
        properties_before.len(),
        2,
        "each append owns a property shard"
    );
    reopened
        .execute("MATCH (p:Person {name:'Ada'}) SET p.name = 'Ada Lovelace'")
        .unwrap();
    let properties_after = property_digests(&source);
    let unchanged_properties = properties_before
        .iter()
        .filter(|(path, digest)| properties_after.get(*path) == Some(*digest))
        .count();
    assert_eq!(
        properties_after.len(),
        properties_before.len() + 1,
        "the property update appends one authoritative immutable fragment"
    );
    assert_eq!(
        unchanged_properties,
        properties_before.len(),
        "property updates must not rewrite any prior immutable fragment"
    );

    reopened.rebuild_adjacency(None).unwrap();
    drop(reopened);
    let reopened = GraphForge::new(Some(source_path)).unwrap();
    let traversal = "MATCH (a:Person)-[:KNOWS]->(b:Person) \
                     RETURN a.name AS from, b.name AS to ORDER BY from, to";
    assert!(
        reopened
            .explain(traversal)
            .unwrap()
            .contains("adjacency=hit"),
        "reopened traversal must use the rebuilt adjacency index"
    );
    let queried = reopened.execute(traversal).unwrap();
    assert_eq!(queried.stats.rows_produced, 2);
    let source_query_fingerprint = result_fingerprint(&queried);
    let later_edge_uuid = scalar_uuid(
        &reopened
            .execute("MATCH (a:Person {name:'Cy'})-[r:KNOWS]->(:Person) RETURN r.edge_uuid")
            .unwrap(),
    );
    let open = reopened.graph_open_evidence();
    let reopened_inventory = resolve_project_generation(&source)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    assert_eq!(open.strategy, GraphFilesOpenStrategy::PrivateMaterialize);
    assert_eq!(open.files_validated, reopened_inventory.file_count);
    assert_eq!(open.files_copied, open.files_validated);

    let limits = PortableV2Limits::default();
    let subset_package = root.path().join("later-edge-subset.gfpb");
    reopened
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: subset_package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: Some(PortableV2SubsetRequest {
                    selector: PortableV2GraphSelector {
                        node_uuids: Vec::new(),
                        edge_uuids: vec![later_edge_uuid.hyphenated().to_string()],
                    },
                    closure: PortableV2SubsetClosure::Referential,
                    projection: PortableV2PropertyProjection::default(),
                }),
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    let subset_verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: subset_package,
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        subset_verified.package_class,
        PortableV2PackageClass::GraphDataSubset
    );

    let package = root.path().join("sharded.gfpb");
    let exported = reopened
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    drop(reopened);
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(verified.package_digest, exported.package_digest);

    let imported_path = root.path().join("clean-import");
    assert!(!imported_path.exists(), "import target must start clean");
    GraphForge::import_portable_v2(
        &imported_path,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::from_u128(931)),
            limits,
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(Some(imported_path.to_str().unwrap())).unwrap();
    assert_eq!(imported.node_count("Person").unwrap(), 4);
    let imported_edges = imported
        .execute("MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) AS edges")
        .unwrap();
    assert_eq!(scalar_i64(&imported_edges), 2);
    let imported_query = imported.execute(traversal).unwrap();
    assert_eq!(imported_query.stats.rows_produced, 2);
    assert_eq!(
        result_fingerprint(&imported_query),
        source_query_fingerprint
    );

    // The writer's direct evidence proves the second shard encodes only new
    // input and never decodes or rewrites the first shard.
    let direct = root.path().join("direct-work-evidence");
    fs::create_dir(&direct).unwrap();
    let first_uuid = Uuid::now_v7();
    let mut first = GraphWriter::open_at(&direct, OntologyMode::Strict, 1).unwrap();
    first.create_node(first_uuid, TypeId(0)).unwrap();
    first.flush().unwrap();
    let writer_limits = GraphWriterLimits {
        max_buffered_topology_rows: 1,
        max_buffered_topology_bytes: 16 * 1024,
        max_flush_scratch_bytes: 16 * 1024,
    };
    graphforge_storage::io_stats::reset();
    let mut second = GraphWriter::open_at(&direct, OntologyMode::Strict, 2)
        .unwrap()
        .with_limits(writer_limits);
    second.create_node(Uuid::now_v7(), TypeId(0)).unwrap();
    second.flush().unwrap();
    let work = second.topology_write_work();
    let io = graphforge_storage::io_stats::snapshot();
    assert_eq!(work.input_rows, 1);
    assert_eq!(work.prior_rows_decoded, 0);
    assert_eq!(work.rows_encoded, 1);
    assert_eq!(work.shard_count, 1);
    assert_eq!(work.existing_rows_rewritten, 0);
    assert_eq!(work.new_rows_written, 1);
    assert!(work.output_bytes > 0);
    assert_eq!(work.peak_buffered_rows, 1);
    assert!(work.peak_buffered_bytes <= writer_limits.max_buffered_topology_bytes as u64);
    assert!(work.peak_flush_scratch_bytes <= writer_limits.max_flush_scratch_bytes as u64);
    assert_eq!(
        io.node_full_reads, 0,
        "append must not decode prior node shards"
    );
    assert_eq!(
        io.edge_full_reads, 0,
        "append must not decode prior edge shards"
    );
    assert_eq!(io.topology_rewrite_existing_rows, 0);
    assert_eq!(io.topology_rewrite_new_rows, 1);
    assert_eq!(io.topology_rewrite_output_rows, 1);
}

#[test]
fn public_delete_keeps_multishard_index_readers_and_portability_consistent() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("delete-source");
    let graph = GraphForge::new(Some(source.to_str().unwrap())).unwrap();
    graph
        .execute("CREATE (:Person {name:'Ada'})-[:KNOWS]->(:Person {name:'Bob'})")
        .unwrap();
    let first_inventory = inventory_digests(&source);
    graph
        .execute("CREATE (:Person {name:'Cy'})-[:KNOWS]->(:Person {name:'Di'})")
        .unwrap();
    graph
        .execute("MATCH (p:Person {name:'Di'}) SET p:Selected")
        .unwrap();
    assert_eq!(graph.node_count("Selected").unwrap(), 1);
    let before_delete = inventory_digests(&source);
    let earlier_shards = before_delete
        .iter()
        .filter(|(path, _)| {
            first_inventory.contains_key(*path)
                && (*path == "topology/nodes.parquet"
                    || path.starts_with("topology/nodes/")
                    || path.starts_with("topology/edges/"))
                && path.ends_with(".parquet")
        })
        .map(|(path, digest)| (path.clone(), digest.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        earlier_shards.len(),
        2,
        "the first node and edge fragments remain distinct"
    );

    let deleted_uuid = scalar_uuid(
        &graph
            .execute("MATCH (p:Person {name:'Cy'}) RETURN p.node_uuid")
            .unwrap(),
    );
    let generation = resolve_project_generation(&source).unwrap();
    let mut index = UuidMembershipIndex::open(&generation.graph_tree_root()).unwrap();
    let deleted_surrogate = index.lookup_node_surrogates(&[deleted_uuid]).unwrap().0[0]
        .expect("Cy is indexed before deletion");

    graph
        .execute("MATCH (p:Person {name:'Cy'}) DETACH DELETE p")
        .unwrap();
    let after_delete = inventory_digests(&source);
    for (path, digest) in &earlier_shards {
        assert_eq!(
            after_delete.get(path),
            Some(digest),
            "deleting a later-shard entity must not rewrite earlier shard {path}"
        );
    }
    let generation = resolve_project_generation(&source).unwrap();
    let mut index = UuidMembershipIndex::open(&generation.graph_tree_root()).unwrap();
    assert_eq!(
        index.lookup_node_surrogates(&[deleted_uuid]).unwrap().0,
        [None]
    );
    assert_eq!(index.count(UuidIndexKind::Node), 3);
    assert_eq!(index.count(UuidIndexKind::Edge), 1);

    graph.execute("CREATE (:Person {name:'Eve'})").unwrap();
    let replacement_uuid = scalar_uuid(
        &graph
            .execute("MATCH (p:Person {name:'Eve'}) RETURN p.node_uuid")
            .unwrap(),
    );
    assert_ne!(
        replacement_uuid, deleted_uuid,
        "deleted identity is not reused"
    );
    let generation = resolve_project_generation(&source).unwrap();
    let mut index = UuidMembershipIndex::open(&generation.graph_tree_root()).unwrap();
    let replacement_surrogate = index.lookup_node_surrogates(&[replacement_uuid]).unwrap().0[0]
        .expect("replacement node is indexed");
    assert_ne!(
        replacement_surrogate, deleted_surrogate,
        "surrogate is not reused"
    );

    graph.rebuild_adjacency(None).unwrap();
    drop(graph);
    let reopened = GraphForge::new(Some(source.to_str().unwrap())).unwrap();
    assert_eq!(reopened.node_count("Person").unwrap(), 4);
    assert_eq!(reopened.node_count("Selected").unwrap(), 1);
    let traversal = "MATCH (a:Person)-[:KNOWS]->(b:Person) \
                     RETURN a.name AS from, b.name AS to ORDER BY from, to";
    assert!(
        reopened
            .explain(traversal)
            .unwrap()
            .contains("adjacency=hit")
    );
    let source_result = reopened.execute(traversal).unwrap();
    assert_eq!(source_result.stats.rows_produced, 1);
    let source_fingerprint = result_fingerprint(&source_result);

    let limits = PortableV2Limits::default();
    let package = root.path().join("deleted-sharded.gfpb");
    let exported = reopened
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    drop(reopened);
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(verified.package_digest, exported.package_digest);

    let imported_path = root.path().join("deleted-clean-import");
    GraphForge::import_portable_v2(
        &imported_path,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::from_u128(931_2)),
            limits,
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(Some(imported_path.to_str().unwrap())).unwrap();
    assert_eq!(imported.node_count("Person").unwrap(), 4);
    let imported_edges = imported
        .execute("MATCH ()-[:KNOWS]->() RETURN count(*) AS edges")
        .unwrap();
    assert_eq!(scalar_i64(&imported_edges), 1);
    assert_eq!(
        result_fingerprint(&imported.execute(traversal).unwrap()),
        source_fingerprint
    );
}

#[test]
fn file_backed_multi_file_fixture_reopens_without_snapshot_envelope() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    {
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})")
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
    }

    let generation = resolve_project_generation(root.path()).unwrap();
    let files = generation
        .participant_snapshot("graph", GRAPH_FILES_FAMILY)
        .unwrap()
        .expect("published generation must use graph/files");
    assert_eq!(files.encoding, "json");
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_none(),
        "new publications must not write the legacy snapshot envelope"
    );
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    assert!(inventory.file_count >= 2);
    assert!(inventory.total_byte_length > 0);
    assert!(generation.graph_tree_root().is_dir());

    let reopened = GraphForge::new(Some(path)).unwrap();
    let evidence = reopened.graph_open_evidence();
    assert_eq!(
        evidence.strategy,
        GraphFilesOpenStrategy::PrivateMaterialize
    );
    assert!(evidence.files_validated >= 2);
    assert_eq!(evidence.files_copied, evidence.files_validated);
    assert_eq!(evidence.files_opened_in_place, 0);
    assert!(evidence.bytes_copied > 0);
    assert_eq!(evidence.bytes_copied, evidence.bytes_validated);

    let result = reopened
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
}

#[test]
fn checkpoint_read_only_open_pins_graph_tree_in_place() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute("CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "pinned".into(),
            description: Some("file-backed read-only pin".into()),
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();

    let view = graph.open_checkpoint("pinned").unwrap();
    let evidence = view.graph_open_evidence();
    assert_eq!(evidence.strategy, GraphFilesOpenStrategy::PinnedInPlace);
    assert!(evidence.files_validated >= 2);
    assert_eq!(evidence.files_copied, 0);
    assert_eq!(evidence.bytes_copied, 0);
    assert_eq!(evidence.files_opened_in_place, evidence.files_validated);
    assert_eq!(
        view.project_open_recovery().kind,
        graphforge_storage::ProjectOpenRecoveryKind::CheckpointView
    );
    assert_eq!(
        view.project_open_recovery().selected_generation_class,
        graphforge_storage::ProjectRecoveryGenerationClass::CheckpointPinned
    );

    let result = view
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
}

#[test]
fn project_open_exposes_recovery_evidence_and_repeat_open_stays_stable() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let first = GraphForge::new(Some(path)).unwrap();
    assert_eq!(
        first.project_open_recovery().kind,
        graphforge_storage::ProjectOpenRecoveryKind::Initialization
    );
    let generation = first.project_open_recovery().selected_generation_uuid;
    drop(first);

    let second = GraphForge::new(Some(path)).unwrap();
    let evidence = second.project_open_recovery();
    assert_eq!(
        evidence.kind,
        graphforge_storage::ProjectOpenRecoveryKind::ProjectOpen
    );
    assert_eq!(evidence.selected_generation_uuid, generation);
    assert_eq!(
        evidence.selected_generation_class,
        graphforge_storage::ProjectRecoveryGenerationClass::CommittedCurrent
    );
    assert!(!evidence.work_detected);
    assert!(evidence.deferred.is_none());
}

#[test]
fn portable_export_of_file_backed_generation_is_structured_unsupported() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();

    let generation = resolve_project_generation(root.path()).unwrap();
    assert!(
        generation
            .participant_snapshot("graph", GRAPH_FILES_FAMILY)
            .unwrap()
            .is_some()
    );

    let output = root.path().join("file-backed.gfportable");
    let error = graph
        .export_portable(PortableExportRequest {
            selection: PortableSelection::Current,
            output,
        })
        .expect_err("file-backed portable export must fail closed");
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
    assert!(
        error
            .to_string()
            .contains("portable interchange does not yet encode file-backed graph trees")
    );
}

#[test]
fn legacy_snapshot_generations_remain_readable() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(workspace.path().join("topology")).unwrap();
    std::fs::write(workspace.path().join("topology/nodes.parquet"), b"legacy").unwrap();
    project_fixture::publish_graph_workspace(root.path(), workspace.path());

    let generation = resolve_project_generation(root.path()).unwrap();
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_some()
    );
    assert!(
        generation
            .participant_snapshot("graph", GRAPH_FILES_FAMILY)
            .unwrap()
            .is_none()
    );

    let opened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    assert_eq!(
        opened.graph_open_evidence().strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
}

#[test]
fn unsupported_graph_files_inventory_version_is_structured() {
    let error = graphforge_storage::decode_inventory(
        br#"{"format":"graphforge-graph-files","format_version":99,"file_count":0,"files":[],"total_byte_length":0}
"#,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
}

/// Manual large-class proof: publish a queryable file-backed generation whose
/// validated bytes exceed the legacy 2 GiB snapshot envelope, then reopen via
/// `GraphForge::new` without whole-graph Arrow hydrate.
///
/// Uses sparse padding beside a real small graph so RAM stays bounded. CI must
/// not run this (ignored; no external fixture download).
#[test]
#[ignore = "manual large-class evidence; sparse >2GiB tree, not CI"]
fn oversize_file_backed_generation_exceeds_legacy_snapshot_envelope() {
    let evidence_out = std::env::var_os("GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("build/file-backed-oversize-evidence.json"));
    if let Some(parent) = evidence_out.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir_all(&project).unwrap();
    let path = project.to_str().unwrap();

    {
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})")
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
    }

    let seed_generation = resolve_project_generation(&project).unwrap();
    let expected_parent = seed_generation.generation_uuid();
    let seed_tree = seed_generation.graph_tree_root();
    let workspace = root.path().join("workspace");
    copy_dir_recursive(&seed_tree, &workspace).unwrap();

    let padding_rel = Path::new("padding/oversize.bin");
    let padding_path = workspace.join(padding_rel);
    fs::create_dir_all(padding_path.parent().unwrap()).unwrap();
    create_sparse_file(&padding_path, OVERSIZE_PADDING_BYTES).unwrap();

    let (inventory, files_participant) = capture_graph_files(&workspace).unwrap();
    assert!(
        inventory.total_byte_length > LEGACY_SNAPSHOT_ENVELOPE_BYTES,
        "inventory must exceed legacy 2 GiB envelope (got {})",
        inventory.total_byte_length
    );

    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files_participant);
    let generation_uuid = Uuid::now_v7();
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid,
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(&project, &request, Some(workspace.as_path()))
            .unwrap()
    else {
        panic!("oversize publication unexpectedly replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();

    let rss_before_open = peak_rss_bytes();
    let reopened = GraphForge::new(Some(path)).unwrap();
    let open_evidence = reopened.graph_open_evidence().clone();
    assert_ne!(
        open_evidence.strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
    assert!(open_evidence.bytes_validated > LEGACY_SNAPSHOT_ENVELOPE_BYTES);
    assert_eq!(open_evidence.bytes_validated, inventory.total_byte_length);
    assert_eq!(
        open_evidence.strategy,
        GraphFilesOpenStrategy::PrivateMaterialize
    );
    assert_eq!(open_evidence.bytes_copied, open_evidence.bytes_validated);

    let result = reopened
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
    let rss_after_query = peak_rss_bytes();

    let generation = resolve_project_generation(&project).unwrap();
    assert_eq!(generation.generation_uuid(), generation_uuid);
    let committed_inventory = generation.graph_files_inventory().unwrap().unwrap();
    assert!(committed_inventory.total_byte_length > LEGACY_SNAPSHOT_ENVELOPE_BYTES);

    let payload = serde_json::json!({
        "schema": "graphforge-file-backed-oversize-evidence/1",
        "issue": 338,
        "source_sha": git_head_sha(),
        "strategy": "sparse_padding_beside_queryable_graph",
        "legacy_snapshot_envelope_bytes": LEGACY_SNAPSHOT_ENVELOPE_BYTES,
        "inventory": {
            "file_count": committed_inventory.file_count,
            "total_byte_length": committed_inventory.total_byte_length,
            "padding_relative_path": padding_rel.to_string_lossy(),
            "padding_byte_length": OVERSIZE_PADDING_BYTES,
        },
        "publication": {
            "generation_uuid": generation_uuid.hyphenated().to_string(),
            "path": "stage_project_generation_with_graph_tree + publish (same path as GraphForge)",
        },
        "open": {
            "api": "GraphForge::new",
            "strategy": format!("{:?}", open_evidence.strategy),
            "files_validated": open_evidence.files_validated,
            "bytes_validated": open_evidence.bytes_validated,
            "files_copied": open_evidence.files_copied,
            "bytes_copied": open_evidence.bytes_copied,
            "files_opened_in_place": open_evidence.files_opened_in_place,
        },
        "query": {
            "cypher": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to",
            "rows_produced": 1,
        },
        "resources": {
            "peak_rss_bytes_before_open": rss_before_open,
            "peak_rss_bytes_after_query": rss_after_query,
            "honest_limits": [
                "Padding is sparse on supporting filesystems; validated/copied byte lengths are logical sizes.",
                "Writable GraphForge::new materializes file-by-file (PrivateMaterialize); checkpoint/RO uses PinnedInPlace.",
                "This proves public persistence beyond the 2 GiB snapshot envelope; full 8M/128M (~15 GiB) remains optional measured evidence under local resource stops."
            ]
        },
        "recorded_at_unix_secs": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    });
    fs::write(&evidence_out, serde_json::to_vec_pretty(&payload).unwrap()).unwrap();
    eprintln!(
        "wrote oversize file-backed evidence to {}",
        evidence_out.display()
    );
}

fn create_sparse_file(path: &Path, logical_bytes: u64) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    if logical_bytes == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::Start(logical_bytes - 1))?;
    file.write_all(&[0])?;
    file.sync_all()?;
    Ok(())
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let to = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &to)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), to)?;
        } else {
            return Err(std::io::Error::other("special file in graph tree"));
        }
    }
    Ok(())
}

fn peak_rss_bytes() -> Option<u64> {
    let mut file = File::open("/proc/self/status").ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            let kb = value
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

fn git_head_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into())
}
