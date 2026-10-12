//! A node anchor `(:L {id: $x})` is answered by a persisted equality index, so
//! it reads the matches and a fixed amount of index, not the label (#1983).
//!
//! The gate counts bytes through the lifecycle read-path counters, never time.
//! `id` is a permutation of the row index, so no fragment's footer statistics
//! can exclude it: without the index every fragment holding the label is
//! authenticated and decoded to find the one row.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, Int64Builder,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage, GraphConstructionBudgets,
    GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase, lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
/// Bytes of incompressible text on every node, so that a fragment fills its
/// byte cap with a few thousand rows and the route spans several fragments.
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;
const ANCHOR: &str = "MATCH (n:Entity {id: $ident}) RETURN n.id AS id";
const INDEX_IN_PLAN: &str = "index=property_equality(id)";

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

fn padding(index: usize) -> String {
    let mut text = String::with_capacity(PADDING_BYTES);
    let mut state = index as u64 + 1;
    while text.len() < PADDING_BYTES {
        state = mix(state);
        text.push_str(&format!("{state:016x}"));
    }
    text.truncate(PADDING_BYTES);
    text
}

/// A permutation of `0..nodes` that scatters every value across all fragments.
fn scattered(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
}

fn node_schema(id_type: DataType) -> Arc<Schema> {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", id_type, true)));
    fields.push(Arc::new(Field::new("padding", DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

fn append_nodes(
    forge: &GraphForge,
    nodes: usize,
    id_type: DataType,
    id_of: impl Fn(usize) -> ArrayRef + Copy,
) {
    let schema = node_schema(id_type);
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for node in start..end {
            uuids.append_value(node_uuid(node).as_bytes()).unwrap();
        }
        let ids = {
            // `id_of` builds the whole column for the window.
            let column = id_of(start);
            column.slice(0, end - start)
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(uuids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                ids,
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

fn construct_int_ids(forge: &GraphForge, nodes: usize) {
    append_nodes(forge, nodes, DataType::Int64, move |start| {
        let mut ids = Int64Builder::with_capacity(WRITE_WINDOW);
        for node in start..start + WRITE_WINDOW {
            ids.append_value(scattered(node, nodes));
        }
        Arc::new(ids.finish()) as ArrayRef
    });
}

fn open(project: &Path, target_partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn run(forge: &GraphForge, query: &str, params: &[(&str, IrLiteral)]) -> Vec<RecordBatch> {
    let params = params
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect::<HashMap<_, _>>();
    forge
        .execute_with_params(query, &params)
        .expect("statement executes")
        .batches
}

fn read_bytes(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, u64) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let batches = run(forge, query, &[("ident", IrLiteral::Int(ident))]);
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    (
        batches,
        delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
    )
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("plan")
}

/// Storage-layer accounting is process-wide, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The property fragments of a project: how many, and the largest object.
fn property_fragments(project: &Path) -> (usize, u64) {
    let inventory = graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory");
    let sizes = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .map(|file| file.byte_length)
        .collect::<Vec<_>>();
    (sizes.len(), sizes.into_iter().max().unwrap_or(0))
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    fragments: usize,
    largest: u64,
}

fn build(nodes: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    let forge = GraphForge::new(Some(project.to_str().expect("utf-8 path"))).unwrap();
    construct_int_ids(&forge, nodes);
    drop(forge);
    let (fragments, largest) = property_fragments(&project);
    Built {
        _root: root,
        project,
        fragments,
        largest,
    }
}

/// The index files the generation's inventory declares.
fn index_files(project: &Path) -> Vec<graphforge_storage::GraphFileEntry> {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .into_iter()
        .filter(|file| file.relative_path.starts_with("indexes/property/"))
        .collect()
}

#[test]
fn the_index_is_published_with_the_generation_and_named_by_explain() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let files = index_files(&built.project);
    assert!(
        files.iter().any(|f| f.relative_path.ends_with(".eq"))
            && files.iter().any(|f| f.relative_path.ends_with(".eqs")),
        "the constructed generation declares the index: {files:?}"
    );
    let forge = open(&built.project, 1);
    let literal = plan(&forge, "MATCH (n:Entity {id: 5}) RETURN n.id AS id");
    assert!(literal.contains(INDEX_IN_PLAN), "{literal}");
    // A WHERE equality on one label plans the same access.
    let filtered = plan(&forge, "MATCH (n:Entity) WHERE n.id = 5 RETURN n.id AS id");
    assert!(filtered.contains(INDEX_IN_PLAN), "{filtered}");
    assert_eq!(
        ids(&run(&forge, ANCHOR, &[("ident", IrLiteral::Int(5))])),
        [5]
    );
}

#[test]
fn an_unindexed_property_is_scanned_and_still_answers() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let forge = GraphForge::new(Some(root.path().join("p").to_str().unwrap())).unwrap();
    // `id` is a float here, which the index does not cover.
    append_nodes(&forge, 64, DataType::Float64, |start| {
        Arc::new(Float64Array::from_iter_values(
            (start..start + WRITE_WINDOW).map(|n| n as f64 + 0.5),
        )) as ArrayRef
    });
    let scan = plan(&forge, "MATCH (n:Entity {id: 5.5}) RETURN count(*) AS n");
    assert!(!scan.contains("index="), "{scan}");
    let batches = run(
        &forge,
        "MATCH (n:Entity {id: 5.5}) RETURN count(*) AS n",
        &[],
    );
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
}

#[test]
fn an_anchored_lookup_reads_the_match_not_the_label() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(small.fragments >= 3, "the label spans fragments");
    assert!(large.fragments >= 2 * small.fragments - 1);
    let mut reads = Vec::new();
    for partitions in [1, 2, 4] {
        let mut by_size = Vec::new();
        for built in [&small, &large] {
            let forge = open(&built.project, partitions);
            // The first statement admits the label's footers once per session.
            let (warm, _) = read_bytes(&forge, ANCHOR, 0);
            assert_eq!(ids(&warm), [0]);
            for ident in [5_i64, 4_097, 11_111] {
                let (batches, bytes) = read_bytes(&forge, ANCHOR, ident);
                assert_eq!(ids(&batches), [ident], "partitions={partitions}");
                // The match's fragment is authenticated to read it; the other
                // fragments, and the index beyond its shards, are not read.
                let bound = 2 * built.largest + (1 << 20);
                assert!(
                    bytes <= bound,
                    "partitions={partitions}, {} fragments, largest {}: read {bytes} bytes, bound {bound}",
                    built.fragments,
                    built.largest
                );
                by_size.push(bytes);
            }
            // A value no node holds reads no fragment.
            let (missing, bytes) = read_bytes(&forge, ANCHOR, 99_999_999);
            assert_eq!(rows(&missing), 0);
            assert!(bytes <= (1 << 20), "a miss read {bytes} bytes");
        }
        reads.push((partitions, by_size));
    }
    for (partitions, by_size) in reads {
        let (small_max, large_max) = (
            by_size[..3].iter().max().copied().unwrap(),
            by_size[3..].iter().max().copied().unwrap(),
        );
        assert!(
            large_max <= small_max + (256 << 10),
            "partitions={partitions}: reads grew with the label: {small_max} -> {large_max}"
        );
    }
}

#[test]
fn string_ids_are_indexed_and_boolean_ids_answer_exactly() {
    let _serial = serial();
    let forge = GraphForge::new(None).unwrap();
    append_nodes(&forge, 5, DataType::Utf8, |start| {
        Arc::new(StringArray::from_iter_values(
            (start..start + WRITE_WINDOW).map(|n| format!("k{}", n % 5)),
        )) as ArrayRef
    });
    let query = "MATCH (n:Entity {id: 'k3'}) RETURN count(*) AS n";
    assert!(plan(&forge, query).contains(INDEX_IN_PLAN));
    let count = |batches: Vec<RecordBatch>| {
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(count(run(&forge, query, &[])), 1);
    assert_eq!(
        count(run(
            &forge,
            "MATCH (n:Entity {id: 'zz'}) RETURN count(*) AS n",
            &[]
        )),
        0
    );
    let flags = GraphForge::new(None).unwrap();
    append_nodes(&flags, 6, DataType::Boolean, |start| {
        Arc::new(BooleanArray::from_iter(
            (start..start + WRITE_WINDOW).map(|n| Some(n % 3 == 0)),
        )) as ArrayRef
    });
    // The optimizer rewrites `id = true` to `id`, which is no equality for
    // the scan to push, so booleans are answered by the scan, exactly.
    let query = "MATCH (n:Entity {id: true}) RETURN count(*) AS n";
    assert_eq!(count(run(&flags, query, &[])), 2);
    let query = "MATCH (n:Entity {id: false}) RETURN count(*) AS n";
    assert_eq!(count(run(&flags, query, &[])), 4);
}

#[test]
fn an_in_memory_project_builds_and_serves_the_index() {
    let _serial = serial();
    let forge = GraphForge::new(None).unwrap();
    construct_int_ids(&forge, 1_000);
    assert!(plan(&forge, "MATCH (n:Entity {id: 7}) RETURN n.id AS id").contains(INDEX_IN_PLAN));
    assert_eq!(
        ids(&run(&forge, ANCHOR, &[("ident", IrLiteral::Int(7))])),
        [7]
    );
    assert_eq!(
        rows(&run(&forge, ANCHOR, &[("ident", IrLiteral::Int(1_000))])),
        0
    );
}

/// A project written without the index answers through the scan; an explicit
/// rebuild publishes the index, and a later write makes it stale again, which
/// the scan answers exactly. A read never builds or repairs an index.
#[test]
fn a_project_without_the_index_scans_until_rebuilt_and_after_a_write() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    {
        let forge = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
        for id in 0..40 {
            forge
                .execute(&format!("CREATE (:Entity {{id: {id}}})"))
                .unwrap();
        }
    }
    assert!(index_files(&project).is_empty(), "no index was published");
    let forge = open(&project, 1);
    let query = "MATCH (n:Entity {id: 17}) RETURN n.id AS id";
    assert!(!plan(&forge, query).contains("index="));
    assert_eq!(ids(&run(&forge, query, &[])), [17]);
    // Reading did not build one.
    assert!(index_files(&project).is_empty());

    let built = forge.rebuild_property_indexes(None).unwrap();
    assert!(
        built.equality.indexes >= 1 && built.equality.files >= 2,
        "{built:?}"
    );
    assert!(plan(&forge, query).contains(INDEX_IN_PLAN));
    assert_eq!(ids(&run(&forge, query, &[])), [17]);
    drop(forge);

    // The rebuilt index is part of the published generation: a fresh process
    // finds it.
    assert!(!index_files(&project).is_empty());
    let forge = open(&project, 1);
    assert!(plan(&forge, query).contains(INDEX_IN_PLAN));
    assert_eq!(ids(&run(&forge, query, &[])), [17]);

    // A write changes the route; the index no longer describes it.
    forge.execute("CREATE (:Entity {id: 1000})").unwrap();
    forge
        .execute("MATCH (n:Entity {id: 17}) SET n.id = 18")
        .unwrap();
    let new_query = "MATCH (n:Entity {id: 1000}) RETURN n.id AS id";
    assert!(!plan(&forge, new_query).contains("index="));
    assert_eq!(ids(&run(&forge, new_query, &[])), [1000]);
    assert!(ids(&run(&forge, query, &[])).is_empty());
    assert_eq!(
        ids(&run(
            &forge,
            "MATCH (n:Entity {id: 18}) RETURN n.id AS id",
            &[]
        )),
        [18, 18]
    );
    forge.rebuild_property_indexes(None).unwrap();
    assert!(plan(&forge, new_query).contains(INDEX_IN_PLAN));
    assert_eq!(ids(&run(&forge, new_query, &[])), [1000]);
}

/// The same bytes for the same content: rebuilding an unchanged generation
/// writes the same files.
#[test]
fn the_index_bytes_are_a_function_of_the_content() {
    let _serial = serial();
    let mut digests = Vec::new();
    for _ in 0..2 {
        let built = build(SMALL_NODES);
        let mut files = index_files(&built.project);
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        digests.push(
            files
                .into_iter()
                .map(|f| (f.relative_path, f.content_sha256))
                .collect::<Vec<_>>(),
        );
    }
    assert!(!digests[0].is_empty());
    assert_eq!(digests[0], digests[1]);
}

/// The index a bulk construction writes from the rows it encodes is the index
/// a rebuild reads back from the published fragments: same files, same bytes.
#[test]
fn the_construction_index_is_the_one_a_rebuild_reads_back() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let digests = |project: &Path| {
        let mut files = index_files(project)
            .into_iter()
            .map(|f| (f.relative_path, f.content_sha256))
            .collect::<Vec<_>>();
        files.sort();
        files
    };
    let at_construction = digests(&built.project);
    assert!(!at_construction.is_empty());
    let forge = open(&built.project, 1);
    let rebuilt = forge.rebuild_property_indexes(None).unwrap();
    assert_eq!(rebuilt.equality.files as usize, at_construction.len());
    drop(forge);
    assert_eq!(digests(&built.project), at_construction);
}

fn corrupt_object(project: &Path, entry: &graphforge_storage::GraphFileEntry) {
    let generation = graphforge_storage::resolve_project_generation(project).unwrap();
    let object =
        graphforge_storage::graph_object_path(generation.container_root(), &entry.content_sha256)
            .unwrap();
    drop(generation);
    let mut permissions = std::fs::metadata(&object).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(0o600);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    std::fs::set_permissions(&object, permissions).unwrap();
    let mut bytes = std::fs::read(&object).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&object, bytes).unwrap();
}

/// A shard or manifest that fails its checksum is refused with `GF_VALIDATION`
/// by the lookup that reads it. It is not served, and the scan is not
/// substituted for it.
#[test]
fn a_corrupted_index_is_refused_not_rebuilt_or_bypassed() {
    let _serial = serial();
    for suffix in [".eqs", ".eq"] {
        let built = build(SMALL_NODES);
        let entry = index_files(&built.project)
            .into_iter()
            .find(|f| f.relative_path.ends_with(suffix))
            .expect("published index file");
        corrupt_object(&built.project, &entry);
        // Opening reads no index payload.
        let forge = open(&built.project, 1);
        let error = forge
            .execute_with_params(
                ANCHOR,
                &HashMap::from([("ident".to_owned(), IrLiteral::Int(5))]),
            )
            .expect_err("a corrupted index must be refused");
        assert!(
            error.to_string().contains("GF_VALIDATION") || error.to_string().contains("checksum"),
            "{suffix}: {error}"
        );
        assert_eq!(error.code(), "GF_VALIDATION", "{suffix}: {error}");
        // A statement that does not use the index is unaffected.
        assert_eq!(
            rows(&run(
                &forge,
                "MATCH (n:Entity) RETURN n.id AS id LIMIT 3",
                &[]
            )),
            3
        );
    }
}
