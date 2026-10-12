//! Edge properties of a traversed frontier are read from a persisted index, so
//! a one-hop `e.timestamp` reads the frontier's shards, not the route (#1984).
//!
//! The edge property route spans several fragments, each authenticated as a
//! whole object by the property overlay. Without the index every statement
//! opens and checksums every fragment of the route to find the few rows of the
//! frontier. The gate counts bytes through the lifecycle read-path counters,
//! never time, and checks every value against the arithmetic that generated it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WINDOW: usize = 4_096;
/// Bytes of incompressible text on every edge, so that a property fragment fills
/// its byte cap with a few thousand rows and the route spans several fragments.
const MEMO_BYTES: usize = 1_024;
const SMALL_EDGES: usize = 12_288;
const LARGE_EDGES: usize = 2 * SMALL_EDGES;
/// Edge UUIDs are `edge_uuid(slot)`. The small graph takes every second slot of
/// the large one, so the large graph doubles the edges between the small
/// graph's, and every edge the two share has the same UUID and values.
const SLOTS: usize = LARGE_EDGES;
/// The measured frontier: `FRONTIER` edges of node 0 at slots `0, STRIDE,
/// 2 * STRIDE, ...`. They are spread over the whole UUID range, so a read of
/// them reaches every property fragment, and they are present at both sizes.
const FRONTIER: usize = 16;
const STRIDE: usize = 1_500;
const SUM_TIMESTAMPS: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN sum(e.timestamp) AS n";

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(slot: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (slot as u128 + 1))
}

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

fn memo(index: usize) -> String {
    let mut text = String::with_capacity(MEMO_BYTES);
    let mut state = index as u64 + 1;
    while text.len() < MEMO_BYTES {
        state = mix(state);
        text.push_str(&format!("{state:016x}"));
    }
    text.truncate(MEMO_BYTES);
    text
}

/// Whether the edge at `slot` is one of node 0's frontier edges.
fn in_frontier(slot: usize) -> bool {
    slot % STRIDE == 0 && slot / STRIDE < FRONTIER
}

/// The slots of the frontier edges.
fn frontier_slots() -> impl Iterator<Item = usize> {
    (0..FRONTIER).map(|edge| edge * STRIDE)
}

/// The frontier edges leave node 0; every other edge leaves
/// node `1 + slot % (nodes - 1)`.
fn source_of(slot: usize, nodes: usize) -> usize {
    if in_frontier(slot) {
        0
    } else {
        1 + slot % (nodes - 1)
    }
}

fn timestamp_of(slot: usize) -> i64 {
    1_000 + 3 * slot as i64
}

fn amount_of(slot: usize) -> f64 {
    slot as f64 * 0.25
}

/// Some edges have no `amount`.
fn has_amount(slot: usize) -> bool {
    slot % 7 != 0
}

fn seen_of(slot: usize) -> bool {
    slot % 11 < 5
}

/// Build a graph of `edges` edges (and as many nodes) in the large graph's slots.
fn construct(dir: &Path, edges: usize) {
    let nodes = edges;
    let step = SLOTS / edges;
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("timestamp", DataType::Int64, true)));
    edge_fields.push(Arc::new(Field::new("amount", DataType::Float64, true)));
    edge_fields.push(Arc::new(Field::new("memo", DataType::Utf8, true)));
    edge_fields.push(Arc::new(Field::new("seen", DataType::Boolean, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WINDOW) {
        let end = (start + WINDOW).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..nodes).step_by(WINDOW) {
        let end = (start + WINDOW).min(nodes);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut timestamps = Int64Builder::with_capacity(rows);
        let mut amounts = Float64Builder::with_capacity(rows);
        let mut memos = StringBuilder::new();
        let mut seen = Vec::with_capacity(rows);
        for edge in start..end {
            let slot = edge * step;
            let source = source_of(slot, nodes);
            ids.append_value(edge_uuid(slot).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + 1) % nodes).as_bytes())
                .unwrap();
            timestamps.append_value(timestamp_of(slot));
            amounts.append_option(has_amount(slot).then(|| amount_of(slot)));
            memos.append_value(memo(slot));
            seen.push(seen_of(slot));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(timestamps.finish()),
                Arc::new(amounts.finish()),
                Arc::new(memos.finish()),
                Arc::new(BooleanArray::from(seen)),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
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

fn run(forge: &GraphForge, query: &str, ident: i64) -> Vec<RecordBatch> {
    forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes")
        .batches
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, u64) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let batches = run(forge, query, ident);
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    (
        batches,
        delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
    )
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

fn scalar_f64(batches: &[RecordBatch]) -> f64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("n is Float64")
        .value(0)
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    edges: usize,
}

fn build(edges: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, edges);
    Built {
        _root: root,
        project,
        edges,
    }
}

fn inventory(project: &Path) -> Vec<graphforge_storage::GraphFileEntry> {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
}

fn index_files(project: &Path) -> Vec<graphforge_storage::GraphFileEntry> {
    inventory(project)
        .into_iter()
        .filter(|file| file.relative_path.starts_with("indexes/edge-properties/"))
        .collect()
}

/// The edge property fragments of a project: how many, and their total bytes.
fn property_fragments(project: &Path) -> (usize, u64) {
    let sizes = inventory(project)
        .into_iter()
        .filter(|file| file.relative_path.starts_with("edge_properties/"))
        .map(|file| file.byte_length)
        .collect::<Vec<_>>();
    (sizes.len(), sizes.into_iter().sum())
}

fn frontier_timestamps() -> i64 {
    frontier_slots().map(timestamp_of).sum()
}

#[test]
fn the_index_is_published_with_the_generation() {
    let _serial = serial();
    let built = build(SMALL_EDGES);
    let files = index_files(&built.project);
    assert!(
        files.iter().any(|f| f.relative_path.ends_with(".epx"))
            && files.iter().any(|f| f.relative_path.ends_with(".epk")),
        "the constructed generation declares the index: {files:?}"
    );
    // The index is a manifest and the packs that hold its blobs, never a file
    // per blob: every durable open links every declared artifact.
    assert!(
        files
            .iter()
            .all(|f| f.relative_path.ends_with(".epx") || f.relative_path.ends_with(".epk")),
        "the index is a manifest and packs: {files:?}"
    );
    let manifest = manifest(&built.project);
    let shards = manifest["shards"].as_array().unwrap().len();
    let packs = files
        .iter()
        .filter(|f| f.relative_path.ends_with(".epk"))
        .count();
    assert!(
        shards > packs,
        "{shards} shards of several blobs each share {packs} packs"
    );
    let (fragments, _) = property_fragments(&built.project);
    assert!(fragments >= 3, "the route spans fragments: {fragments}");
}

/// The values of every property type come back as generated, from the index
/// and again from the property overlay once a write has made the index stale.
#[test]
fn every_stored_type_reads_back_exactly_with_and_without_the_index() {
    let _serial = serial();
    let built = build(SMALL_EDGES);
    let forge = open(&built.project, 1);
    let nodes = built.edges;
    // The frontier edges leave node 0. Node 1's edges are the others whose
    // slot is a multiple of `nodes - 1`.
    let node_one = (0..built.edges)
        .map(|edge| edge * (SLOTS / built.edges))
        .filter(|slot| source_of(*slot, nodes) == 1)
        .collect::<Vec<_>>();
    assert!(!node_one.is_empty());
    let check = |forge: &GraphForge| {
        let sum = scalar_i64(&run(forge, SUM_TIMESTAMPS, 0));
        assert_eq!(sum, frontier_timestamps());
        // Variable-length expansion resolves its `rels` properties the same way.
        let var_len = run(
            forge,
            "MATCH (a:Entity {ident: $ident})-[r:LINK*1..2]->(b) WHERE size(r) = 1 \
             RETURN [e IN r | e.timestamp] AS timestamps",
            0,
        );
        let mut timestamps = var_len
            .iter()
            .flat_map(|batch| {
                let lists = batch
                    .column_by_name("timestamps")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                (0..lists.len())
                    .flat_map(|row| {
                        let values = lists.value(row);
                        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                        (0..values.len())
                            .map(|i| values.value(i))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        timestamps.sort_unstable();
        let mut expected_timestamps = frontier_slots().map(timestamp_of).collect::<Vec<_>>();
        expected_timestamps.sort_unstable();
        assert_eq!(timestamps, expected_timestamps);
        let amounts = scalar_f64(&run(
            forge,
            "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN sum(e.amount) AS n",
            0,
        ));
        let expected: f64 = frontier_slots()
            .filter(|slot| has_amount(*slot))
            .map(amount_of)
            .sum();
        assert!((amounts - expected).abs() < 1e-9, "{amounts} vs {expected}");
        // A property some edges lack counts only those that have it.
        let present = scalar_i64(&run(
            forge,
            "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN count(e.amount) AS n",
            0,
        ));
        assert_eq!(
            present,
            frontier_slots().filter(|slot| has_amount(*slot)).count() as i64
        );
        let seen = scalar_i64(&run(
            forge,
            "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) WHERE e.seen RETURN count(*) AS n",
            0,
        ));
        assert_eq!(
            seen,
            frontier_slots().filter(|slot| seen_of(*slot)).count() as i64
        );
        let memos = run(
            forge,
            "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN e.memo AS memo",
            1,
        );
        let mut got = memos
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("memo")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..column.len())
                    .map(|row| column.value(row).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut expected = node_one.iter().map(|slot| memo(*slot)).collect::<Vec<_>>();
        got.sort();
        expected.sort();
        assert_eq!(got, expected);
    };
    check(&forge);
    // A write changes the route; the index no longer describes it.
    forge
        .execute("MATCH (a:Entity {ident: 5})-[e:LINK]->(b) SET e.seen = false")
        .unwrap();
    check(&forge);
}

/// The path of the stored object that holds an inventory entry's bytes.
fn object_of(project: &Path, entry: &graphforge_storage::GraphFileEntry) -> std::path::PathBuf {
    let generation = graphforge_storage::resolve_project_generation(project).unwrap();
    graphforge_storage::graph_object_path(generation.container_root(), &entry.content_sha256)
        .unwrap()
}

/// The decoded manifest of the project's one edge property route.
fn manifest(project: &Path) -> serde_json::Value {
    let entry = index_files(project)
        .into_iter()
        .find(|file| file.relative_path.ends_with(".epx"))
        .expect("the generation declares a manifest");
    let bytes = std::fs::read(object_of(project, &entry)).unwrap();
    assert_eq!(bytes[16], b'\n');
    serde_json::from_slice(&bytes[17..]).expect("manifest body is JSON")
}

/// Where a manifest names a blob: its pack file, and the bytes inside it.
#[derive(Clone, Copy, Debug)]
struct Blob {
    pack: u64,
    offset: u64,
    byte_length: u64,
}

fn blob_of(seal: &serde_json::Value) -> Blob {
    let field = |name: &str| seal[name].as_u64().expect(name);
    Blob {
        pack: field("pack"),
        offset: field("offset"),
        byte_length: field("byte_length"),
    }
}

/// The blob of `column` in shard `shard`, or the shard's UUID blob.
fn column_blob(manifest: &serde_json::Value, shard: usize, column: Option<&str>) -> Blob {
    let shard = &manifest["shards"][shard];
    let Some(column) = column else {
        return blob_of(&shard["uuids"]);
    };
    let index = manifest["columns"]
        .as_array()
        .unwrap()
        .iter()
        .position(|name| name == column)
        .expect("the route stores the column");
    let entry = shard["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["column"] == index)
        .expect("the shard stores the column");
    blob_of(entry)
}

/// The largest blob of the column (or of the UUIDs) over all shards.
fn largest_blob(project: &Path, column: Option<&str>) -> u64 {
    let manifest = manifest(project);
    (0..manifest["shards"].as_array().unwrap().len())
        .map(|shard| column_blob(&manifest, shard, column).byte_length)
        .max()
        .unwrap()
}

/// A one-hop read of a fixed frontier's edge property is bounded by the
/// frontier: at most the UUID blob and the demanded column blob of the one
/// shard that holds each frontier edge, whatever the size of the route. The
/// route doubles, and every edge of the frontier is still found in one shard.
#[test]
fn a_one_hop_edge_property_read_is_bounded_by_the_frontier_not_the_relation() {
    let _serial = serial();
    let small = build(SMALL_EDGES);
    let large = build(LARGE_EDGES);
    let (small_fragments, small_bytes) = property_fragments(&small.project);
    let (large_fragments, large_bytes) = property_fragments(&large.project);
    assert!(large_fragments >= 2 * small_fragments - 1);
    for partitions in [1, 2, 4] {
        let mut reads = Vec::new();
        for built in [&small, &large] {
            let forge = open(&built.project, partitions);
            let plan = forge
                .explain_stage(
                    &SUM_TIMESTAMPS.replace("$ident", "0"),
                    ExplainStage::PhysicalPlan,
                )
                .expect("plan");
            assert!(plan.contains("ExpandExec"), "{plan}");
            // The first statement admits the session's footers and the
            // adjacency index once.
            let (warm, _) = measured(&forge, SUM_TIMESTAMPS, 0);
            assert_eq!(scalar_i64(&warm), frontier_timestamps());
            let (batches, bytes) = measured(&forge, SUM_TIMESTAMPS, 0);
            assert_eq!(scalar_i64(&batches), frontier_timestamps());
            reads.push(bytes);
        }
        eprintln!("partitions={partitions}: read bytes {reads:?}");
        // The bound does not mention the route. Beyond the property shards a
        // statement reads the anchor, the adjacency index and the edge
        // topology, which the margin covers. Under one partition the whole
        // statement runs on the capturing thread; with more, the capture sees
        // less, never more.
        let bound = FRONTIER as u64
            * (largest_blob(&large.project, None)
                + largest_blob(&large.project, Some("timestamp")))
            + (512 << 10);
        for (size, bytes) in ["small", "large"].into_iter().zip(&reads) {
            assert!(
                *bytes <= bound,
                "partitions={partitions}: the {size} route read {bytes} bytes, bound {bound} \
                 (route of {small_fragments} -> {large_fragments} fragments, \
                 {small_bytes} -> {large_bytes} bytes)"
            );
        }
    }
}

/// Flip one bit of the byte at `position` of the stored object of `entry`.
fn corrupt_object(
    project: &Path,
    entry: &graphforge_storage::GraphFileEntry,
    position: impl FnOnce(usize) -> usize,
) {
    let object = object_of(project, entry);
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
    let at = position(bytes.len());
    bytes[at] ^= 1;
    std::fs::write(&object, bytes).unwrap();
}

/// Corrupt the manifest of the route.
fn corrupt_manifest(project: &Path) {
    let entry = index_files(project)
        .into_iter()
        .find(|file| file.relative_path.ends_with(".epx"))
        .expect("the generation declares a manifest");
    corrupt_object(project, &entry, |length| length - 1);
}

/// Corrupt the middle of one blob, inside the pack that holds it. The pack's
/// other blobs, which have their own seals, are left intact.
fn corrupt_blob(project: &Path, blob: Blob) {
    let suffix = format!("/{:08}.epk", blob.pack);
    let entry = index_files(project)
        .into_iter()
        .find(|file| file.relative_path.ends_with(&suffix))
        .expect("the generation declares the pack");
    let at = (blob.offset + blob.byte_length / 2) as usize;
    corrupt_object(project, &entry, |length| {
        assert!(at < length, "the blob lies inside its pack");
        at
    });
}

fn digests(project: &Path) -> Vec<(String, String)> {
    let mut files = index_files(project)
        .into_iter()
        .map(|file| (file.relative_path, file.content_sha256))
        .collect::<Vec<_>>();
    files.sort();
    files
}

const NO_PROPERTY: &str = "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN count(b) AS n";
const MEMOS: &str = "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN e.memo AS memo";

/// Execute `query` for the node `ident` and return the error it fails with.
fn refusal(forge: &GraphForge, query: &str, ident: i64) -> graphforge_core::GfError {
    forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect_err("a corrupted index must be refused")
}

/// A manifest, or a blob of the UUIDs or of a demanded column, that fails its
/// checksum is refused with `GF_VALIDATION` by the lookup that reads it. It is
/// not served, the property overlay is not substituted for it, and nothing is
/// rebuilt by the read.
#[test]
fn a_corrupted_index_is_refused_not_rebuilt_or_bypassed() {
    let _serial = serial();
    for label in ["manifest", "uuid blob", "timestamp blob"] {
        let built = build(SMALL_EDGES);
        let before = digests(&built.project);
        // The frontier's edges hold the smallest UUIDs, so shard 0 serves it.
        match label {
            "manifest" => corrupt_manifest(&built.project),
            "uuid blob" => {
                let manifest = manifest(&built.project);
                corrupt_blob(&built.project, column_blob(&manifest, 0, None));
            }
            _ => {
                let manifest = manifest(&built.project);
                corrupt_blob(&built.project, column_blob(&manifest, 0, Some("timestamp")));
            }
        }
        // Opening reads no index payload.
        let forge = open(&built.project, 1);
        let error = refusal(&forge, SUM_TIMESTAMPS, 0);
        assert_eq!(error.code(), "GF_VALIDATION", "{label}: {error}");
        // A statement that reads no edge property is unaffected.
        let count = scalar_i64(&run(&forge, NO_PROPERTY, 0));
        assert_eq!(count, FRONTIER as i64, "{label}");
        drop(forge);
        assert_eq!(digests(&built.project), before, "{label}: the read rebuilt");
    }
}

/// Every blob carries its own seal, so damage to the blob of one column
/// refuses the statements that demand that column and no others, though the
/// blob shares its pack with the blobs of the other columns.
#[test]
fn a_corrupted_blob_refuses_only_the_statements_that_demand_its_column() {
    let _serial = serial();
    let built = build(SMALL_EDGES);
    let before = digests(&built.project);
    let manifest = manifest(&built.project);
    let damaged = column_blob(&manifest, 0, Some("memo"));
    let intact = column_blob(&manifest, 0, Some("timestamp"));
    assert_eq!(damaged.pack, intact.pack, "both blobs are in one pack");
    corrupt_blob(&built.project, damaged);

    let forge = open(&built.project, 1);
    let error = refusal(&forge, MEMOS, 0);
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    // The timestamp blob lies in the damaged blob's pack and is verified on
    // its own seal.
    let sum = scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0));
    assert_eq!(sum, frontier_timestamps());
    drop(forge);
    assert_eq!(digests(&built.project), before, "the read rebuilt");
}

/// A write changes the route, so the index no longer describes it and is not
/// used. A rebuild publishes a current one, and a fresh session finds it.
#[test]
fn a_stale_index_is_not_used_until_rebuilt() {
    let _serial = serial();
    let built = build(SMALL_EDGES);
    let (_, fragment_bytes) = property_fragments(&built.project);
    let forge = open(&built.project, 1);
    let (warm, _) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&warm), frontier_timestamps());
    let (batches, indexed) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&batches), frontier_timestamps());
    assert!(indexed < fragment_bytes / 4, "indexed read {indexed} bytes");

    forge
        .execute("MATCH (a:Entity {ident: 5})-[e:LINK]->(b) SET e.seen = false")
        .unwrap();
    let (batches, stale) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&batches), frontier_timestamps());
    assert!(
        stale > 4 * indexed,
        "a stale index must not serve the read: {stale} vs {indexed} bytes"
    );

    let built_report = forge.rebuild_property_indexes(None).unwrap();
    assert!(
        built_report.edge_properties.indexes >= 1,
        "{built_report:?}"
    );
    // The published generation is admitted once, as at open.
    let (warm, _) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&warm), frontier_timestamps());
    let (batches, rebuilt) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&batches), frontier_timestamps());
    assert!(rebuilt <= 2 * indexed, "{rebuilt} vs {indexed} bytes");
    drop(forge);

    // The rebuilt index is part of the published generation.
    let forge = open(&built.project, 1);
    let (warm, _) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&warm), frontier_timestamps());
    let (batches, reopened) = measured(&forge, SUM_TIMESTAMPS, 0);
    assert_eq!(scalar_i64(&batches), frontier_timestamps());
    assert!(reopened <= 2 * indexed, "{reopened} vs {indexed} bytes");
}

/// The same bytes for the same content, from construction and from a rebuild.
#[test]
fn the_index_bytes_are_a_function_of_the_content() {
    let _serial = serial();
    let first = build(SMALL_EDGES);
    let second = build(SMALL_EDGES);
    let at_construction = digests(&first.project);
    assert!(!at_construction.is_empty());
    assert_eq!(at_construction, digests(&second.project));

    let mut rebuilt = Vec::new();
    for _ in 0..2 {
        let forge = open(&first.project, 1);
        forge.rebuild_property_indexes(None).unwrap();
        drop(forge);
        rebuilt.push(digests(&first.project));
    }
    assert!(!rebuilt[0].is_empty());
    assert_eq!(rebuilt[0], rebuilt[1]);
}
