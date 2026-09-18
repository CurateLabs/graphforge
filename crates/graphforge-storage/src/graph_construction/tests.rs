use super::controls::control_temp;
use super::intake::{chunk_key_name, write_parquet};
use super::recovery::inject_shape_publication_failure;
use super::recovery::is_owned_artifact_temp;
use super::shape::resolve_endpoint_surrogates;
include!("../construction_detail_tests.rs");
include!("../construction_lifecycle_tests.rs");
include!("../construction_determinism_tests.rs");
include!("../transient_composition_tests.rs");
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
use tempfile::TempDir;

use super::*;

pub(super) fn fixed(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap()
}

pub(super) fn tree_has_no_temps(path: &Path) -> bool {
    std::fs::read_dir(path).unwrap().all(|entry| {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            tree_has_no_temps(&path)
        } else {
            !entry.file_name().to_string_lossy().ends_with(".tmp")
        }
    })
}

pub(super) fn node_batch(first: u128, rows: usize) -> RecordBatch {
    let uuids = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(&uuids)),
            Arc::new(StringArray::from(vec!["Person"; rows])),
        ],
    )
    .unwrap()
}

fn distinct_label_batch(first: u128, rows: usize) -> RecordBatch {
    let uuids = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let labels = (first..first + rows as u128)
        .map(|index| format!("Label{index:08}"))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![Arc::new(fixed(&uuids)), Arc::new(StringArray::from(labels))],
    )
    .unwrap()
}

/// `first` is this batch's edge-UUID base. `node_start` and `node_count`
/// (#1439) describe where this batch's src/dst references begin and how
/// many nodes exist to reference, 1-indexed: every reference wraps modulo
/// `node_count`, so it is always a valid node id regardless of chunk
/// position. A caller with more than one chunk must vary `node_start` per
/// chunk, or every chunk in the loop references the same low node-UUID band
/// no matter how many nodes or chunks actually exist. That was invisible
/// while endpoints were routed with the joint identity splitters, which
/// never resolve node UUIDs finely enough to notice; routing them with
/// node-only splitters surfaces it as a real (fixture-caused, not
/// production) skew. Single-chunk callers keep passing `node_start: 1`,
/// matching this function's previous fixed behaviour exactly whenever
/// `rows <= node_count`.
pub(super) fn edge_batch(
    first: u128,
    node_start: u128,
    node_count: u128,
    rows: usize,
) -> RecordBatch {
    let edges = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let src = (0..rows as u128)
        .map(|offset| 1 + (node_start - 1 + offset) % node_count)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let dst = (0..rows as u128)
        .map(|offset| 1 + (node_start + offset) % node_count)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(&edges)),
            Arc::new(StringArray::from(vec!["R"; rows])),
            Arc::new(fixed(&src)),
            Arc::new(fixed(&dst)),
        ],
    )
    .unwrap()
}

pub(super) fn node_property_batch(first: u128, rows: usize) -> RecordBatch {
    node_property_batch_for(first, rows, "Person")
}

pub(super) fn node_property_batch_for(first: u128, rows: usize, label: &str) -> RecordBatch {
    let uuids = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("score", DataType::Int64, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&uuids)),
            Arc::new(StringArray::from(vec![label; rows])),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| row as i64 + 10),
            )),
        ],
    )
    .unwrap()
}

pub(super) fn edge_property_batch(first: u128, rows: usize) -> RecordBatch {
    let edges = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let src = (1..=rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let dst = (2..=rows as u128 + 1)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_EDGE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("weight", DataType::Int64, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&edges)),
            Arc::new(StringArray::from(vec!["R"; rows])),
            Arc::new(fixed(&src)),
            Arc::new(fixed(&dst)),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| row as i64 + 20),
            )),
        ],
    )
    .unwrap()
}

pub(super) fn open(root: &TempDir, operation: u128) -> GraphConstructionSession {
    GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(operation),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap()
}

#[test]
fn generation_zero_accepts_empty_node_parquet() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    std::fs::create_dir_all(root.path().join("topology")).unwrap();
    ArrowWriter::try_new(
        File::create(root.path().join("topology/nodes.parquet")).unwrap(),
        crate::schemas::TOPOLOGY_NODES_SCHEMA.clone(),
        None,
    )
    .unwrap()
    .close()
    .unwrap();
    let _session = open(&root, 9876);
}

#[test]
fn generation_zero_rejects_unmarked_nonempty_legacy_parent() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut writer =
        crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Exploratory, 1)
            .unwrap();
    writer
        .create_node(
            Uuid::now_v7(),
            graphforge_value::EntityTypeId::decode(0).unwrap(),
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    std::fs::remove_file(root.path().join("topology/generation.json")).unwrap();
    assert_eq!(crate::read_topology_generation(root.path()).unwrap(), 0);
    let error = GraphConstructionSession::open(
        root.path(),
        Uuid::now_v7(),
        0,
        GraphConstructionBudgets::default(),
    )
    .err()
    .expect("nonempty legacy parent cannot be initial construction");
    assert!(
        error
            .to_string()
            .contains("generation-zero construction parent contains existing node rows"),
        "{error}"
    );
    assert!(!crate::has_runtime_entity_label_encoding_marker(
        root.path()
    ));
}

fn hydrate_parent_fixture(project: &TempDir) {
    let selected = crate::resolve_project_generation(project.path()).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let graph = project.path().join("fixture-graph");
    if graph.exists() {
        std::fs::remove_dir_all(&graph).unwrap();
    }
    std::fs::create_dir(&graph).unwrap();
    crate::materialize_graph_objects(project.path(), &inventory, &graph).unwrap();
}

pub(super) fn nonempty_project_with_nodes(node_count: u64) -> TempDir {
    let project = TempDir::new().unwrap();
    crate::open_or_initialize_project(project.path()).unwrap();
    let mut session = open(&project, Uuid::new_v4().as_u128());
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes",
            &node_batch(1, node_count as usize),
        )
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Edge,
            "edge",
            &edge_batch(100, 1, u128::from(node_count), 1),
        )
        .unwrap();
    session.seal().unwrap();
    let encoded = session.prepare_canonical_encoding(1).unwrap();
    session
        .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
        .unwrap();
    drop(session);
    hydrate_parent_fixture(&project);
    project
}

pub(super) fn nonempty_project_generation_two() -> TempDir {
    let project = nonempty_project_with_nodes(2);
    let mut session = GraphConstructionSession::open(
        project.path(),
        Uuid::new_v4(),
        1,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "node", &node_batch(3, 1))
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Edge,
            "edge",
            &edge_batch(101, 1, 3, 1),
        )
        .unwrap();
    session.seal().unwrap();
    let encoded = session.prepare_canonical_encoding(2).unwrap();
    session
        .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
        .unwrap();
    drop(session);
    hydrate_parent_fixture(&project);
    project
}

#[test]
fn parent_identity_payload_is_referenced_not_copied_at_1x_and_2x_base() {
    for (base_nodes, operation) in [(2_u64, 8_110_u128), (4, 8_111)] {
        let project = nonempty_project_with_nodes(base_nodes);
        let mut session = GraphConstructionSession::open(
            project.path(),
            Uuid::from_u128(operation),
            1,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "delta",
                &node_batch(u128::from(base_nodes + 1), 1),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let root = project
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(operation).simple().to_string());
        assert_eq!(
            std::fs::metadata(root.join(shape.identities))
                .unwrap()
                .len(),
            BASE_IDENTITY_WIDTH as u64
        );
        assert_eq!(shape.node_count, base_nodes + 1);
    }
}

#[test]
fn catalog_shape_preserves_parent_ids_history_and_ignores_null_observations() {
    let project = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("topology")).unwrap();
    let mut parent = RuntimeCatalog::new();
    parent.intern_label_at("BaseOnly", 11).unwrap();
    parent
        .intern_property_at("legacy", Some("BaseOnly"), 11)
        .unwrap();
    let parent_path = project.path().join("topology/runtime_catalog.parquet");
    let parent_batch = parent.to_record_batch();
    let mut parent_writer = ArrowWriter::try_new(
        File::create(&parent_path).unwrap(),
        parent_batch.schema(),
        None,
    )
    .unwrap();
    parent_writer.write(&parent_batch).unwrap();
    parent_writer.close().unwrap();

    let private_path = project.path().join("catalog-shape");
    std::fs::create_dir(&private_path).unwrap();
    let private = StableDirectory::open(&private_path).unwrap();
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
            Field::new("score", DataType::Int64, true),
        ])),
        vec![
            Arc::new(fixed(&[1_u128.to_be_bytes(), 2_u128.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["Person", "Person"])),
            Arc::new(Int64Array::from(vec![Some(7), None])),
        ],
    )
    .unwrap();
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
    write_parquet(&private, "nodes.parquet", &rows, &mut evidence).unwrap();
    let project_root = StableDirectory::open(project.path()).unwrap();
    let (parent_catalog, parent_catalog_sha256, _) =
        load_parent_runtime_catalog(&project_root, 1, GraphConstructionBudgets::default()).unwrap();
    assert!(parent_catalog_sha256.is_some());
    let output = build_runtime_catalog(
        parent_catalog,
        &private,
        CatalogSource::Rows(&["nodes.parquet".to_owned()]),
        CatalogSource::Rows(&[]),
        DetailCodec::from_version(FORMAT_VERSION).unwrap(),
        42,
        GraphConstructionBudgets::default(),
        &mut || false,
        &mut evidence,
    )
    .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        private.open_child_file(OsStr::new(&output)).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let kinds = merged
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names = merged
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = merged
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let first = merged
        .column(4)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    let base = (0..merged.num_rows())
        .find(|&row| kinds.value(row) == "entity_type" && names.value(row) == "BaseOnly")
        .unwrap();
    assert_eq!((counts.value(base), first.value(base)), (1, 11));
    let score = (0..merged.num_rows())
        .find(|&row| kinds.value(row) == "property" && names.value(row) == "score")
        .unwrap();
    assert_eq!(counts.value(score), 1);
}

#[test]
fn million_chunk_shaping_retains_name_state_bounded_by_the_partition_count() {
    // The online merge scheduler's logarithmic name state is gone: range
    // partitioning retains exactly one spill name per partition per family,
    // independent of how many chunks were staged.
    let budgets = GraphConstructionBudgets::default();
    assert_eq!(budgets.max_chunks, 1_000_000);
    let families = super::partition_shaping::PartitionFamily::ALL.len() as u64;
    let slots = u64::from(budgets.partition_count) * families;
    // Name state is a function of the recorded partition count and the family
    // set, never of the staged chunk count.
    assert_eq!(slots, 1_280, "retained slots: {slots}");
    assert!(slots < budgets.max_chunks / 100, "retained slots: {slots}");
    assert_eq!(budgets.max_schema_groups, 256);
    assert_eq!(
        budgets.partition_count,
        super::partition::DEFAULT_PARTITION_COUNT
    );
}

#[test]
fn parent_catalog_streaming_enforces_entry_and_decoded_byte_budgets() {
    let root = TempDir::new().unwrap();
    let project = StableDirectory::open(root.path()).unwrap();
    let topology = project
        .create_child_directory(OsStr::new("topology"))
        .unwrap();
    let mut source = RuntimeCatalog::new();
    for index in 0..5_000 {
        source
            .intern_label_at(&format!("Label{index:05}"), 42)
            .unwrap();
    }
    let mut evidence = GraphConstructionEvidence::default();
    write_parquet(
        &topology,
        "runtime_catalog.parquet",
        &source.to_record_batch(),
        &mut evidence,
    )
    .unwrap();

    let too_few = GraphConstructionBudgets {
        max_catalog_entries: 4_999,
        ..GraphConstructionBudgets::default()
    };
    assert!(
        load_parent_runtime_catalog(&project, 1, too_few)
            .unwrap_err()
            .to_string()
            .contains("admission budget")
    );
    let too_small = GraphConstructionBudgets {
        max_catalog_decoded_bytes: 1,
        ..GraphConstructionBudgets::default()
    };
    assert!(
        load_parent_runtime_catalog(&project, 1, too_small)
            .unwrap_err()
            .to_string()
            .contains("admission budget")
    );
    let (restored, digest, work) =
        load_parent_runtime_catalog(&project, 1, GraphConstructionBudgets::default()).unwrap();
    assert_eq!(restored.to_record_batch().num_rows(), 5_000);
    assert!(digest.is_some());
    assert!(work.bytes > 0);
    assert!(work.operations > 0);
}

pub(super) fn shape_temporary_names(root: &StableDirectory) -> Vec<String> {
    root.child_names()
        .unwrap()
        .into_iter()
        .filter_map(|name| name.into_string().ok())
        .filter(|name| is_owned_artifact_temp(name))
        .collect()
}

#[test]
fn staged_catalog_cardinality_is_bounded_at_one_and_two_windows() {
    for windows in [1_usize, 2] {
        let root = TempDir::new().unwrap();
        let rows = windows * 8;
        let expected_identifier_bytes = rows * "Label00000000".len();
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 8,
            max_run_records: 32,
            max_catalog_entries: rows,
            max_catalog_identifier_bytes: expected_identifier_bytes,
            ..GraphConstructionBudgets::default()
        };
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(8_000 + rows as u128),
            0,
            budgets,
        )
        .unwrap();
        for window in 0..windows {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{window}"),
                    &distinct_label_batch(1 + (window * 8) as u128, 8),
                )
                .unwrap();
        }
        session.seal().unwrap();
        session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(session.evidence().peak_catalog_entries, rows as u64);
        assert_eq!(
            session.evidence().peak_catalog_identifier_bytes,
            expected_identifier_bytes as u64
        );
        // Property-free chunks derive the catalog from the details family
        // (#1455): no row Parquet is scanned, so no batch is decoded. The
        // admission budgets above are what this test defends; the
        // property-bearing path's decode is asserted in `catalog::tests`.
        assert_eq!(session.evidence().peak_catalog_decoded_batch_bytes, 0);
        assert!(session.evidence().merge_read_records > 0);
        assert!(session.evidence().shape_input_validation_read_bytes > 0);
    }
}

#[test]
fn staged_catalog_rejects_entry_and_identifier_overflow_before_interning() {
    for budgets in [
        GraphConstructionBudgets {
            max_batch_rows: 8,
            max_run_records: 32,
            max_catalog_entries: 7,
            ..GraphConstructionBudgets::default()
        },
        GraphConstructionBudgets {
            max_batch_rows: 8,
            max_run_records: 32,
            max_catalog_identifier_bytes: 7 * "Label00000000".len(),
            ..GraphConstructionBudgets::default()
        },
    ] {
        let root = TempDir::new().unwrap();
        let mut session =
            GraphConstructionSession::open(root.path(), Uuid::new_v4(), 0, budgets).unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &distinct_label_batch(1, 8),
            )
            .unwrap();
        session.seal().unwrap();
        assert!(
            session
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("catalog admission budget")
        );
    }
}

#[cfg(unix)]
#[test]
fn session_drop_unlocks_before_a_duplicated_descriptor_closes() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(301);
    let session = open(&root, 301);
    let inherited_descriptor = session.session_lock.try_clone().unwrap();

    drop(session);

    let resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(resumed.accepted_chunks(), 0);
    drop(resumed);
    drop(inherited_descriptor);
}

#[test]
fn node_after_edge_and_concurrent_same_process_open_fail_closed() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 400);
    assert!(
        GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(400),
            0,
            GraphConstructionBudgets::default()
        )
        .is_err()
    );
    session
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_batch(100, 1, 2, 2),
        )
        .unwrap();
    assert!(
        session
            .append(ConstructionChunkKind::Node, "late-node", &node_batch(1, 1))
            .is_err()
    );
}

#[test]
fn session_coordination_file_is_exclusively_locked() {
    let root = TempDir::new().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    let owner = stable
        .open_or_create_child_file(OsStr::new(SESSION_LOCK))
        .unwrap();
    assert!(crate::file_lock::try_lock_exclusive(&owner).unwrap());

    let contender = stable.open_child_file(OsStr::new(SESSION_LOCK)).unwrap();
    assert!(!crate::file_lock::try_lock_exclusive(&contender).unwrap());
    crate::file_lock::unlock(&owner).unwrap();
    assert!(crate::file_lock::try_lock_exclusive(&contender).unwrap());
}

#[test]
fn crash_subprocess_helper() {
    let Ok(root) = std::env::var("GF_CONSTRUCTION_CRASH_ROOT") else {
        return;
    };
    if std::env::var_os("GF_CONSTRUCTION_PUBLICATION_CRASH").is_some() {
        crate::open_or_initialize_project(Path::new(&root)).unwrap();
        let mut session = GraphConstructionSession::open(
            Path::new(&root),
            Uuid::from_u128(9_470),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        session
            .publish_canonical(&encoding, Uuid::from_u128(9_471), Uuid::from_u128(9_472))
            .unwrap();
        return;
    }
    let mut session = GraphConstructionSession::open(
        Path::new(&root),
        Uuid::from_u128(600),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    // `GF_CONSTRUCTION_SHAPE_CRASH=rows` stages property-bearing chunks so
    // shaping takes the row path; any other value stages bare chunks, whose
    // catalog is derived from the details family and which install no row
    // artifact (#1455). The crash fixtures mirror this choice exactly.
    let shape_crash = std::env::var("GF_CONSTRUCTION_SHAPE_CRASH").ok();
    let batch = |first: u128| {
        if shape_crash.as_deref() == Some("rows") {
            node_property_batch(first, 8)
        } else {
            node_batch(first, 8)
        }
    };
    session
        .append(ConstructionChunkKind::Node, "nodes", &batch(1))
        .unwrap();
    if std::env::var_os("GF_CONSTRUCTION_UUID_ENCODE_CRASH").is_some() {
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        session.encode_canonical(&shape, 1).unwrap();
    }
    if shape_crash.is_some() {
        session
            .append(ConstructionChunkKind::Node, "nodes-2", &batch(9))
            .unwrap();
        session.seal().unwrap();
        session.shape_canonical_with_cancellation(|| false).unwrap();
    }
}

#[test]
fn publication_crash_after_current_finalizes_same_target_on_reopen() {
    let root = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("graph_construction::tests::crash_subprocess_helper")
        .arg("--nocapture")
        .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
        .env("GF_CONSTRUCTION_PUBLICATION_CRASH", "1")
        .env(
            "GF_CONSTRUCTION_FAILPOINT_COOKIE",
            "graphforge-construction-test-v1",
        )
        .env(
            "GF_CONSTRUCTION_FAILPOINT",
            "publication.after_current_before_receipt",
        )
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));

    let target = Uuid::from_u128(9_471);
    assert_eq!(
        crate::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        target
    );
    let operation = Uuid::from_u128(9_470);
    let operation_root = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string());
    assert!(!operation_root.join(PUBLICATION_RECEIPT).exists());
    let encoding: GraphConstructionEncoding = serde_json::from_slice(
        &std::fs::read(operation_root.join("encoded-v1/inventory.json")).unwrap(),
    )
    .unwrap();
    let mut resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let receipt = resumed
        .publish_canonical(&encoding, target, Uuid::from_u128(9_472))
        .unwrap();
    assert_eq!(receipt.generation_uuid, target);
    assert!(receipt.idempotent_replay);
    assert!(operation_root.join(PUBLICATION_RECEIPT).is_file());

    let current = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(current.generation_uuid(), target);
    let inventory = current.graph_files_inventory().unwrap().unwrap();
    let materialized = TempDir::new().unwrap();
    let graph = materialized.path().join("graph");
    std::fs::create_dir(&graph).unwrap();
    crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
    let uuid_index = crate::UuidMembershipIndex::open(&graph).unwrap();
    assert_eq!(uuid_index.count(crate::UuidIndexKind::Node), 2);
}

#[test]
fn uuid_encoding_crashes_recover_every_durable_boundary() {
    for failpoint in [
        "encode.parquet.after_temp_fsync.topology/nodes/00000000000000000001-00000000000000000008.parquet",
        "encode.parquet.after_install.topology/nodes/00000000000000000001-00000000000000000008.parquet",
        "encode.copy.after_temp_fsync.topology/runtime_catalog.parquet",
        "encode.copy.after_install.topology/runtime_catalog.parquet",
        "uuid_encode.after_intent",
        "uuid_encode.after_temps",
        "uuid_encode.after_delta_runs",
        "uuid_encode.after_manifest",
        "uuid_encode.after_intent_removal",
        "v4_publish.after_artifacts",
        "v4_publish.after_artifacts_fsync",
        "v4_publish.after_receipt_temp_fsync",
        "v4_publish.after_receipt_install",
        "v4_publish.after_manifest_temp_fsync",
        "v4_publish.after_manifest_install",
        "v4_publish.after_lock_temp_fsync",
        "v4_publish.after_lock_install",
        "encode.after_v4_before_inventory",
        "encode.control.after_temp_fsync.inventory.json",
        "encode.control.after_install.inventory.json",
    ] {
        let root = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::crash_subprocess_helper")
            .arg("--nocapture")
            .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
            .env("GF_CONSTRUCTION_UUID_ENCODE_CRASH", "1")
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{failpoint}");
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        let shape = resumed.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = resumed.encode_canonical(&shape, 1).unwrap();
        assert_eq!(encoded.evidence.membership_records, 8, "{failpoint}");
        let membership = root
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(600).simple().to_string())
            .join("encoded-v1/graph/topology/uuid-membership");
        assert!(!membership.join(".construction-intent.json").exists());
        assert!(std::fs::read_dir(membership).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        let encoded_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(600).simple().to_string())
            .join("encoded-v1");
        assert!(tree_has_no_temps(&encoded_root));
        assert!(!encoded_root.join("encoding-intent.json").exists());
    }
}

#[test]
fn subprocess_crashes_recover_each_durable_boundary() {
    let receipt = receipt_name(0);
    let key = chunk_key_name("nodes");
    let parquet = format!("{}.parquet", artifact_stem(0, ConstructionChunkKind::Node));
    let cases = vec![
        (
            "control.install.after_partial.checkpoint.json".to_owned(),
            0_u64,
        ),
        (
            "control.install.after_temp_fsync.checkpoint.json".to_owned(),
            0_u64,
        ),
        (
            "control.install.after_install.checkpoint.json".to_owned(),
            0,
        ),
        ("control.install.after_temp_fsync.intent.json".to_owned(), 0),
        ("control.install.after_install.intent.json".to_owned(), 0),
        (format!("artifact.after_temp_fsync.{parquet}"), 0),
        (format!("artifact.after_install.{parquet}"), 0),
        ("control.replace.after_replace.intent.json".to_owned(), 0),
        (format!("control.install.after_partial.{receipt}"), 0),
        (format!("control.install.after_temp_fsync.{receipt}"), 0),
        (format!("control.install.after_install.{receipt}"), 1),
        (format!("control.install.after_temp_fsync.{key}"), 1),
        (format!("control.install.after_install.{key}"), 1),
        (
            "control.replace.after_partial.checkpoint.json".to_owned(),
            1,
        ),
        (
            "control.replace.after_temp_fsync.checkpoint.json".to_owned(),
            1,
        ),
        (
            "control.replace.after_replace.checkpoint.json".to_owned(),
            1,
        ),
    ];
    for (failpoint, accepted) in cases {
        let root = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::crash_subprocess_helper")
            .arg("--nocapture")
            .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", &failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "failpoint {failpoint}");
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(resumed.accepted_chunks(), accepted, "{failpoint}");
        if accepted == 1 {
            assert!(
                resumed.evidence().recovery_application_read_bytes > 0,
                "accepted interrupted append must report recovery bytes: {failpoint}"
            );
            assert!(
                resumed.evidence().recovery_application_read_operations > 0,
                "accepted interrupted append must report recovery calls: {failpoint}"
            );
        }
        if accepted == 0 {
            resumed
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
                .unwrap();
        }
        resumed.seal().unwrap();
    }
}

fn expected_shape_recovery_delta(root: &Path, evidence: &mut GraphConstructionEvidence) {
    let intent: ShapeIntent =
        serde_json::from_slice(&std::fs::read(root.join(SHAPE_INTENT)).unwrap()).unwrap();
    if intent.complete {
        // Reopen authenticates the completed successor before retirement;
        // the original successful shape already included its own pass.
        for receipt in &intent.outputs {
            let bytes = std::fs::metadata(root.join(&receipt.name)).unwrap().len();
            assert_eq!(bytes, receipt.bytes);
            assert!(bytes <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES);
            evidence.recovery_application_read_bytes += bytes;
            evidence.recovery_application_read_operations += bytes.div_ceil(BLOCK_BYTES as u64);
            if bytes != 0 {
                if cfg!(target_os = "linux") {
                    evidence.cache_release_operations += 1;
                    evidence.cache_released_bytes += bytes;
                } else {
                    evidence.cache_release_unsupported_operations += 1;
                }
                evidence.peak_cache_release_window_bytes =
                    evidence.peak_cache_release_window_bytes.max(bytes);
            }
        }
        // Reopen seals the retirement checkpoint, then its recovery-I/O checkpoint.
        evidence.recovery_checkpoint_fsync_operations += 6;
        return;
    }
    let mut controls = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("shape-receipt-")
        {
            continue;
        }
        let bytes = std::fs::read(entry.path()).unwrap();
        evidence.recovery_application_read_bytes += 2 * bytes.len() as u64;
        evidence.recovery_application_read_operations += 2;
        controls += 1;
        let receipt: ArtifactReceipt = serde_json::from_slice(&bytes).unwrap();
        if !is_shape_artifact_name(&receipt.name) {
            continue;
        }
        let path = root.join(&receipt.name);
        if !path.exists() {
            continue;
        }
        let bytes = path.metadata().unwrap().len();
        assert_eq!(bytes, receipt.bytes);
        assert!(bytes <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES);
        evidence.recovery_application_read_bytes += bytes;
        evidence.recovery_application_read_operations += bytes.div_ceil(BLOCK_BYTES as u64);
        if bytes != 0 {
            if cfg!(target_os = "linux") {
                evidence.cache_release_operations += 1;
                evidence.cache_released_bytes += bytes;
            } else {
                evidence.cache_release_unsupported_operations += 1;
            }
            evidence.peak_cache_release_window_bytes =
                evidence.peak_cache_release_window_bytes.max(bytes);
        }
    }
    if controls != 0 {
        evidence.recovery_checkpoint_fsync_operations += 3;
    }
}

#[test]
fn shape_inventory_and_evidence_commit_recover_without_double_counting() {
    fn without_native_identities(
        mut evidence: GraphConstructionEvidence,
    ) -> GraphConstructionEvidence {
        evidence.storage_active_identity_allocated_bytes.clear();
        evidence.storage_allocation_transitions.clear();
        evidence
    }
    const ROW_INSTALL: &str = "shape.row_partition.after_install";
    const FAILPOINTS: [&str; 5] = [
        "shape.partition_spill.after_install",
        "shape.partition_output.after_install",
        ROW_INSTALL,
        "shape.after_complete_inventory",
        "shape.after_evidence_checkpoint",
    ];
    // Both shaping paths (#1455): bare chunks derive the catalog from the
    // details family and never install a row artifact, so the row-partition
    // install point is not on their path and the helper runs to completion
    // there; property-bearing chunks take the row path and crash at all five
    // points. Every point that is reached must recover to the uninterrupted
    // run's evidence, counted once.
    for fixture in ["bare", "rows"] {
        let batch = |first: u128| {
            if fixture == "rows" {
                node_property_batch(first, 8)
            } else {
                node_batch(first, 8)
            }
        };
        let reference_root = TempDir::new().unwrap();
        let mut reference = GraphConstructionSession::open(
            reference_root.path(),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        reference
            .append(ConstructionChunkKind::Node, "nodes", &batch(1))
            .unwrap();
        reference
            .append(ConstructionChunkKind::Node, "nodes-2", &batch(9))
            .unwrap();
        reference.seal().unwrap();
        let reference_shape = reference
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
        assert_eq!(reference_shape.node_rows.is_empty(), fixture == "bare");
        let expected = without_native_identities(reference.evidence().clone());

        for failpoint in FAILPOINTS {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::crash_subprocess_helper")
                .arg("--nocapture")
                .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
                .env("GF_CONSTRUCTION_SHAPE_CRASH", fixture)
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            if fixture == "bare" && failpoint == ROW_INSTALL {
                // Not reached: no row artifact is installed on this path, so
                // the helper shapes to completion instead of crashing.
                assert_eq!(status.code(), Some(0), "{fixture} {failpoint}");
                continue;
            }
            assert_eq!(status.code(), Some(86), "{fixture} {failpoint}");
            let mut expected_recovered = expected.clone();
            expected_shape_recovery_delta(
                &root
                    .path()
                    .join(PRIVATE_ROOT)
                    .join(Uuid::from_u128(600).simple().to_string()),
                &mut expected_recovered,
            );
            let mut resumed = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(600),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            resumed.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(
                without_native_identities(resumed.evidence().clone()),
                expected_recovered,
                "{fixture} {failpoint}"
            );
        }
    }
}

#[test]
fn discard_reclaims_open_and_sealed_session_trees() {
    for (index, seal) in [false, true].into_iter().enumerate() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_350 + index as u128);
        let mut session = open(&root, operation.as_u128());
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        if seal {
            session.seal().unwrap();
        }
        let private_root = construction_session_root(&root, operation);
        assert!(private_root.exists());

        session.discard().unwrap();

        assert!(!private_root.exists());
    }
}

pub(super) fn ordinal_append_session(
    root: &TempDir,
    generation: u64,
    first: u128,
    rows: usize,
) -> (TempDir, GraphConstructionSession, ConstructionShape) {
    let source = TempDir::new().unwrap();
    let source_graph = source.path().join("graph");
    std::fs::create_dir(&source_graph).unwrap();
    if let Some(inventory) = crate::resolve_project_generation(root.path())
        .unwrap()
        .graph_files_inventory()
        .unwrap()
    {
        crate::materialize_graph_objects(root.path(), &inventory, &source_graph).unwrap();
    }
    let mut session = GraphConstructionSession::open_with_mode_and_lifecycle_from_graph(
        root.path(),
        &source_graph,
        Uuid::new_v4(),
        generation - 1,
        graphforge_core::OntologyMode::Exploratory,
        GraphConstructionBudgets::default(),
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
    .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes",
            &node_batch(first, rows),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    (source, session, shape)
}

pub(super) fn construction_session_root(root: &TempDir, operation: Uuid) -> PathBuf {
    root.path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
}

/// Endpoint-shaped fixture for the partition balance check: 64 partitions of
/// exactly 32 node keys each, so the key domain is perfectly balanced, with
/// the node keys in `hubs` referencing `hub_degree` edges apiece and every
/// other node exactly one. A route plan that agrees with that key layout is
/// built by the caller.
fn route_endpoint_fixture(
    root: &StableDirectory,
    plan: &super::partition::PartitionPlan,
    hubs: &[u16],
    hub_degree: u32,
    evidence: &mut GraphConstructionEvidence,
) -> Result<Option<String>, GfError> {
    use super::partition_shaping::{FixedRangePartitioner, PartitionFamily};
    use crate::construction_record_layout::ENDPOINT_WIDTH;
    const PARTITIONS: usize = 64;
    const KEYS_PER_PARTITION: u16 = 32;
    let mut partitioner = FixedRangePartitioner::<ENDPOINT_WIDTH>::new(
        root,
        PartitionFamily::Endpoints,
        PARTITIONS,
        None,
        false,
    )?;
    let mut edge = 0_u128;
    for node in 0..(PARTITIONS as u16 * KEYS_PER_PARTITION) {
        let degree = if hubs.contains(&node) { hub_degree } else { 1 };
        let mut key = [0_u8; 16];
        key[..2].copy_from_slice(&node.to_be_bytes());
        for _ in 0..degree {
            edge += 1;
            let mut record = [0_u8; ENDPOINT_WIDTH];
            record[..16].copy_from_slice(&key);
            record[16..32].copy_from_slice(&edge.to_be_bytes());
            partitioner.route(plan, &key, &record, evidence)?;
        }
    }
    partitioner.seal(evidence)?;
    partitioner.finish_optional("shaped-fixture-endpoints.run", &mut || false, evidence)
}

/// Splitters at every 32nd node key: the plan the fixture's keys are laid out
/// for, so each partition owns exactly 32 distinct keys.
fn endpoint_fixture_plan() -> super::partition::PartitionPlan {
    let splitters = (1_u16..64)
        .map(|partition| {
            let mut splitter = [0_u8; 16];
            splitter[..2].copy_from_slice(&(partition * 32).to_be_bytes());
            splitter
        })
        .collect();
    super::partition::PartitionPlan::from_recorded(64, splitters).unwrap()
}

#[test]
fn hub_heavy_endpoints_over_balanced_keys_are_accepted() {
    // Three hubs in partition 0 and two elsewhere, each with 400 incident
    // edges against a mean of ~63 rows per partition: partition 0 holds 1,229
    // of 4,043 rows, more than 4x the mean even after discounting any one
    // hub. That is the Graph500 shape (`scale_g500_ladder` at scale 10:
    // "largest partition holds 315 of 1858 rows across 64 partitions,
    // largest single-key run 91"), and it is not a splitter defect: every
    // partition owns exactly 32 distinct keys.
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_041);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let GraphConstructionSession {
        root: session_root,
        checkpoint,
        ..
    } = &mut session;
    let output = route_endpoint_fixture(
        session_root,
        &endpoint_fixture_plan(),
        &[5, 6, 7, 700, 1_500],
        400,
        &mut checkpoint.evidence,
    )
    .unwrap();
    assert!(output.is_some());
}

#[test]
fn collapsed_endpoint_splitters_are_still_refused() {
    // The same 2,048 keys, one record each, under a splitter set that lies
    // entirely above the key domain: every key lands in partition 0. No hub
    // is involved, so the refusal can only come from key concentration --
    // the property the balance check exists to guarantee.
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_042);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let GraphConstructionSession {
        root: session_root,
        checkpoint,
        ..
    } = &mut session;
    let collapsed = (1_u8..64)
        .map(|partition| {
            let mut splitter = [0xff_u8; 16];
            splitter[1] = partition;
            splitter
        })
        .collect();
    let plan = super::partition::PartitionPlan::from_recorded(64, collapsed).unwrap();
    let error = route_endpoint_fixture(session_root, &plan, &[], 1, &mut checkpoint.evidence)
        .expect_err("a collapsed key partitioning must be refused")
        .to_string();
    assert!(error.contains("endpoints distinct keys"), "{error}");
    assert!(error.contains("skewed"), "{error}");
    assert!(error.contains("2048 of 2048"), "{error}");
}
