//! A property-equality anchor on a durable project reads the compared column
//! of the fragments that can hold the value, and a read writes nothing (#1931).
//!
//! The route is several property fragments wide. Before the fix every query
//! authenticated each fragment into a scratch copy, decoded all of its
//! columns, spooled the decoded rows as JSON runs and merged them, so a lookup
//! cost the whole route in reads and a multiple of it in writes. The gate is
//! deterministic and counts bytes, never time:
//!
//! - the lifecycle read-path counters, which count the application's reads and
//!   writes through the storage layer, and
//! - on Linux, `/proc/self/io`'s `write_bytes`, which counts bytes this process
//!   caused to be sent to the storage layer, including scratch writes the
//!   lifecycle counters do not see. `wchar` was the wrong gate: it counts every
//!   byte handed to `write`, including async-runtime eventfd and pipe writes.
//!   `write_bytes` is accounted only for filesystems with writeback accounting,
//!   so tmpfs is not counted. Durable projects require ext4, xfs, or btrfs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
const FAN_OUT: usize = 2;
/// Bytes of incompressible text on every node, so that a fragment fills its
/// byte cap with a few thousand rows and the route spans several fragments.
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
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

/// `ident` is the node's index, so it rises with the UUID and each fragment
/// holds a disjoint range of it. `shuffled` is a permutation of the indices
/// that scatters every value across all fragments.
fn shuffled(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
}

fn construct_with_fanout(dir: &Path, nodes: usize, fan_out: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("shuffled", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("padding", DataType::Utf8, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        let mut scattered = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
            scattered.append_value(shuffled(node, nodes));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
                Arc::new(scattered.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = nodes * fan_out;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % fan_out + 1) % nodes).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

/// One target partition keeps the whole statement on the calling thread, where
/// the lifecycle capture is installed, so it observes every read-path byte.
fn open(project: &Path) -> GraphForge {
    open_with_partitions(project, 1)
}

fn open_with_partitions(project: &Path, target_partitions: usize) -> GraphForge {
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

/// What one statement read and wrote.
#[derive(Debug, Clone, Copy, Default)]
struct Io {
    read_bytes: u64,
    write_bytes: u64,
    write_calls: u64,
    /// Bytes this process caused to be sent to the storage layer; `None` off Linux.
    storage_write_bytes: Option<u64>,
}

#[cfg(target_os = "linux")]
fn storage_write_bytes() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io")
        .expect("task I/O accounting unavailable: cannot read /proc/self/io");
    Some(
        io.lines()
            .find_map(|line| line.strip_prefix("write_bytes: ")?.trim().parse().ok())
            .expect("task I/O accounting unavailable: /proc/self/io has no valid write_bytes line"),
    )
}

#[cfg(not(target_os = "linux"))]
fn storage_write_bytes() -> Option<u64> {
    None
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, Io) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let storage_write_bytes_before = storage_write_bytes();
    let result = forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes");
    let storage_write_bytes_after = storage_write_bytes();
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    let io = Io {
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        write_bytes: delta.totals.write_bytes,
        write_calls: delta.totals.write_calls,
        storage_write_bytes: storage_write_bytes_before
            .zip(storage_write_bytes_after)
            .map(|(b, a)| a - b),
    };
    (result.batches, io)
}

/// Make a read statement's storage-write gate depend only on the statement.
///
/// Reading a file can update its atime, and on some admitted filesystems
/// (ext4 without a journal, btrfs) that dirties a metadata page charged to
/// the reader's `write_bytes`. Under `relatime` a file's atime is updated by
/// a read only while it is not newer than its mtime, so reading every project
/// file once, until each atime is newer than its mtime, leaves nothing for
/// the measured statement to update. `strictatime` updates atime on every
/// read, so the gate cannot hold there and refuses to run.
#[cfg(target_os = "linux")]
fn settle_for_write_gate(project: &Path) {
    use std::io::Read;

    let canonical = project.canonicalize().expect("canonical project path");
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").expect("mountinfo");
    let mount = mountinfo
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(' ').collect();
            let point = Path::new(fields.get(4)?);
            let super_options = line.split(" - ").nth(1)?.split(' ').nth(2).unwrap_or("");
            canonical.starts_with(point).then(|| {
                (
                    point.as_os_str().len(),
                    fields[5].to_owned(),
                    super_options.to_owned(),
                )
            })
        })
        .max_by_key(|(depth, _, _)| *depth)
        .expect("the project's mount is listed");
    let options: Vec<&str> = mount.1.split(',').chain(mount.2.split(',')).collect();
    assert!(
        options
            .iter()
            .any(|option| matches!(*option, "relatime" | "noatime" | "lazytime")),
        "the storage-write gate needs a relatime, noatime or lazytime mount; options {options:?}"
    );
    fn files(directory: &Path, found: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("read project directory") {
            let entry = entry.expect("project entry");
            let kind = entry.file_type().expect("entry type");
            if kind.is_dir() {
                files(&entry.path(), found);
            } else if kind.is_file() {
                found.push(entry.path());
            }
        }
    }
    let mut found = Vec::new();
    files(&canonical, &mut found);
    for _ in 0..50 {
        let mut settled = true;
        for path in &found {
            let mut byte = [0_u8; 1];
            if let Ok(mut file) = std::fs::File::open(path) {
                let _ = file.read(&mut byte);
                let metadata = file.metadata().expect("project file metadata");
                settled &=
                    metadata.accessed().expect("atime") > metadata.modified().expect("mtime");
            }
        }
        if settled {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("project file atimes did not settle after their mtimes");
}

#[cfg(not(target_os = "linux"))]
fn settle_for_write_gate(_project: &Path) {}

fn count(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

/// Storage-layer write accounting is process-wide and the fixtures of this
/// binary write files, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const ANCHORED_LOOKUP: &str = "MATCH (a:Entity {ident: $ident}) RETURN count(*) AS n";
const ANCHORED_HOP: &str = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(*) AS n";
/// The same anchor on a column whose values are scattered over every fragment,
/// so no fragment can be excluded by its statistics.
const SCATTERED_HOP: &str = "MATCH (a:Entity {shuffled: $ident})-[:LINK]->(b) RETURN count(*) AS n";
/// The anchor written so that no equality can be pushed to the property scan.
const UNPUSHED_HOP: &str =
    "MATCH (a:Entity)-[:LINK]->(b) WHERE a.ident + 0 = $ident RETURN count(*) AS n";

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
    build_with_fanout(nodes, FAN_OUT)
}

fn build_with_fanout(nodes: usize, fan_out: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct_with_fanout(&project, nodes, fan_out);
    let (fragments, largest) = property_fragments(&project);
    Built {
        _root: root,
        project,
        fragments,
        largest,
    }
}

#[test]
fn the_equality_is_planned_into_the_property_scan() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    let plan = forge
        .explain_stage(
            "MATCH (a:Entity {ident: 5})-[:LINK]->(b) RETURN count(*) AS n",
            graphforge_api::ExplainStage::PhysicalPlan,
        )
        .expect("plan");
    assert!(
        plan.contains("PropertyOverlayExec: route=_untyped, equality=ident=Int(5)"),
        "{plan}"
    );
    // The destination's route is still joined by key: a statement that reads
    // no property of it still authenticates it (permanent_storage_budgets).
    assert_eq!(plan.matches("PropertyOverlayExec").count(), 2, "{plan}");
}

/// An id-anchored lookup reads the fragment that holds the id, not the route:
/// its reads do not grow when the route does.
#[test]
fn an_anchored_lookup_reads_what_the_match_costs_not_what_the_route_costs() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(small.fragments >= 3, "the route spans fragments");
    assert!(large.fragments >= 2 * small.fragments - 1);
    let mut reads = Vec::new();
    for built in [&small, &large] {
        let forge = open(&built.project);
        // The first statement admits the route's footers once per session.
        let (warm, _) = measured(&forge, ANCHORED_LOOKUP, 0);
        assert_eq!(count(&warm), 1);
        let (batches, io) = measured(&forge, ANCHORED_LOOKUP, 5);
        assert_eq!(count(&batches), 1);
        // The anchor's fragment is authenticated to find it and again to read
        // it, plus the footers and topology; never the other fragments.
        let bound = 2 * built.largest + (1 << 20);
        assert!(
            io.read_bytes <= bound,
            "{} fragments, largest {}: read {} bytes, bound {bound}",
            built.fragments,
            built.largest,
            io.read_bytes
        );
        reads.push(io.read_bytes);
    }
    // The large route holds twice the fragments and bytes.
    assert!(
        reads[1] <= reads[0] + (256 << 10),
        "reads grew with the route: {reads:?}"
    );
}

/// A selected one-hop destination frontier must not scan unrelated destination
/// rows; doubling the destination route leaves the read path bounded.
#[test]
fn an_anchored_one_hop_reads_only_its_selected_destination_frontier() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(large.fragments >= 2 * small.fragments - 1);
    let mut reads_by_partitions = Vec::new();
    for target_partitions in [1, 2, 4] {
        let mut reads = Vec::new();
        for built in [&small, &large] {
            let forge = open_with_partitions(&built.project, target_partitions);
            let (warm, _) = measured(&forge, ANCHORED_HOP, 0);
            assert_eq!(count(&warm), FAN_OUT as i64);
            let (hop, io) = measured(&forge, ANCHORED_HOP, 5);
            assert_eq!(count(&hop), FAN_OUT as i64);
            assert_eq!(io.write_bytes, 0, "{io:?}");
            assert_eq!(io.write_calls, 0, "{io:?}");
            reads.push(io.read_bytes);
        }
        reads_by_partitions.push((target_partitions, reads));
    }
    for (target_partitions, reads) in reads_by_partitions {
        assert!(
            reads[1] <= reads[0] + (256 << 10),
            "target_partitions={target_partitions}: one-hop reads grew with unrelated nodes: {reads:?}"
        );
    }
}

/// An anchored expansion reads destination properties for the matching UUIDs.
/// Doubling unrelated rows must not double those authentication reads.
#[test]
fn anchored_destination_property_reads_do_not_grow_with_unrelated_rows() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(large.fragments >= 2 * small.fragments - 1);
    let query = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(b.ident) AS n";
    for target_partitions in [1, 2, 4] {
        let mut reads = Vec::new();
        for built in [&small, &large] {
            let forge = open_with_partitions(&built.project, target_partitions);
            if reads.is_empty() {
                let plan = forge
                    .explain_stage(
                        &query.replace("$ident", "0"),
                        graphforge_api::ExplainStage::PhysicalPlan,
                    )
                    .expect("physical plan");
                assert!(plan.contains("UuidBuildKeyTapExec"), "{plan}");
                if target_partitions > 1 {
                    assert!(
                        plan.contains(&format!(
                            "RepartitionExec: partitioning=RoundRobinBatch({target_partitions})"
                        )),
                        "{plan}"
                    );
                    assert!(plan.contains("join_type=Left"), "{plan}");
                }
            }
            let (warm, _) = measured(&forge, query, 0);
            assert_eq!(count(&warm), FAN_OUT as i64);
            let (batches, io) = measured(&forge, query, 5);
            assert_eq!(count(&batches), FAN_OUT as i64);
            assert_eq!(io.write_bytes, 0);
            assert_eq!(io.write_calls, 0);
            reads.push(io.read_bytes);
        }
        assert!(
            reads[1] <= reads[0] + (256 << 10),
            "target_partitions={target_partitions}: destination reads grew with unrelated rows: {reads:?}"
        );
    }
}

#[test]
fn topk_variable_length_frontier_nominates_only_its_enrichment_keys() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let query = "MATCH path = (a:Entity {ident: $ident})-[:LINK*1..3]-(b:Entity) \
                 WHERE NOT a = b \
                 WITH b, min(length(path)) AS distance \
                 ORDER BY distance ASC, b.ident ASC \
                 LIMIT 20 \
                 OPTIONAL MATCH (b)-[:LINK]->(c:Entity) \
                 RETURN b.ident AS selected, b.padding AS payload, c.ident AS neighbor";
    let forge = open(&built.project);
    let plan = forge
        .explain_stage(
            &query.replace("$ident", "5"),
            graphforge_api::ExplainStage::PhysicalPlan,
        )
        .expect("physical plan");
    assert!(
        plan.contains("VarLenExpandExec: rel=LINK, hops=1..3"),
        "{plan}"
    );
    assert!(plan.contains("TopK(fetch=20)"), "{plan}");
    assert!(
        plan.contains("UuidBuildKeyTapExec"),
        "the selected top-K endpoint must nominate its exact enrichment UUIDs:\n{plan}"
    );
}

/// A partitioned hash join switches to its map representation for a larger
/// UUID frontier. The producer tap must still finish and preserve the exact
/// result when that frontier contains more than 150 destinations.
#[test]
fn large_partitioned_uuid_frontier_preserves_destination_results() {
    let _serial = serial();
    const MAP_FAN_OUT: usize = 160;
    let built = build_with_fanout(512, MAP_FAN_OUT);
    let forge = open_with_partitions(&built.project, 4);
    let query = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(b.ident) AS n";

    for ident in [0_i64, 5] {
        let (batches, io) = measured(&forge, query, ident);
        assert_eq!(count(&batches), MAP_FAN_OUT as i64);
        assert_eq!(io.write_bytes, 0);
        assert_eq!(io.write_calls, 0);
    }

    // The frontier build nomination must complete even when the parent only
    // consumes one output row from the restored four-partition result.
    let limited = forge
        .execute_with_params(
            "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN b.ident LIMIT 1",
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(0))]),
        )
        .expect("limited statement executes");
    assert_eq!(
        limited
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
}

/// Statistics cannot exclude a fragment from a scattered column, so its lookup
/// reads that column of every fragment; it returns the same rows.
#[test]
fn a_scattered_column_still_answers_exactly() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    for ident in [0_i64, 7, (SMALL_NODES / 2) as i64, (SMALL_NODES - 1) as i64] {
        let (pushed, _) = measured(&forge, SCATTERED_HOP, ident);
        assert_eq!(count(&pushed), FAN_OUT as i64, "shuffled = {ident}");
    }
    let (absent, _) = measured(&forge, SCATTERED_HOP, -1);
    assert_eq!(count(&absent), 0);
}

/// The pushed-down plan and a plan that cannot push return the same answers.
#[test]
fn pushing_the_equality_does_not_change_an_answer() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    for ident in [0_i64, 1, 4095, 4096, 8191, 12_287, 12_288, -3] {
        let (pushed, _) = measured(&forge, ANCHORED_HOP, ident);
        let (full, _) = measured(&forge, UNPUSHED_HOP, ident);
        assert_eq!(count(&pushed), count(&full), "ident = {ident}");
    }
}

/// A newer snapshot decides which value a UUID holds, whichever fragment the
/// equality finds it in, and a reopened project agrees.
#[test]
fn a_newer_snapshot_shadows_an_older_match() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    forge
        .execute("MATCH (a:Entity {ident: 7}) SET a.ident = 70000")
        .expect("update");
    drop(forge);
    for _ in 0..2 {
        let forge = open(&built.project);
        let (old, _) = measured(&forge, ANCHORED_HOP, 7);
        assert_eq!(count(&old), 0, "7 was overwritten");
        let (new, _) = measured(&forge, ANCHORED_HOP, 70_000);
        assert_eq!(count(&new), FAN_OUT as i64);
        let (untouched, _) = measured(&forge, ANCHORED_HOP, 8);
        assert_eq!(count(&untouched), FAN_OUT as i64);
    }
}

/// A read statement writes nothing: no scratch copy of a fragment and no
/// spooled run, in the lifecycle counters or storage-layer write accounting.
#[test]
fn a_read_statement_writes_nothing() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    settle_for_write_gate(&built.project);
    for query in [ANCHORED_HOP, SCATTERED_HOP, UNPUSHED_HOP] {
        let (batches, io) = measured(&forge, query, 5);
        assert_eq!(count(&batches), FAN_OUT as i64);
        assert_eq!(io.write_bytes, 0, "{query}: {io:?}");
        assert_eq!(io.write_calls, 0, "{query}: {io:?}");
        #[cfg(target_os = "linux")]
        assert_eq!(io.storage_write_bytes, Some(0), "{query}: {io:?}");
        #[cfg(not(target_os = "linux"))]
        if let Some(written) = io.storage_write_bytes {
            assert_eq!(written, 0, "{query}: {io:?}");
        }
    }
    // A statement that scans the whole route is the harshest case.
    let (batches, io) = measured(&forge, "MATCH (a:Entity) RETURN count(a.padding) AS n", 0);
    assert_eq!(count(&batches), SMALL_NODES as i64);
    assert_eq!(io.write_bytes, 0, "{io:?}");
    #[cfg(target_os = "linux")]
    assert_eq!(io.storage_write_bytes, Some(0), "{io:?}");
}

#[test]
fn the_storage_write_gate_sees_a_file_write() {
    use std::io::Write;

    let _serial = serial();
    let built = build(SMALL_NODES);
    let path = built.project.join("storage-write-gate-positive.bin");
    let before = storage_write_bytes();
    let mut file = std::fs::File::create(&path).expect("create project-local file");
    file.write_all(&vec![0x5a; 64 * 1024])
        .expect("write project-local file");
    file.sync_all().expect("fsync project-local file");
    drop(file);
    let after = storage_write_bytes();
    #[cfg(target_os = "linux")]
    assert!(
        after.unwrap() - before.unwrap() >= 65_536,
        "project-local file write was not visible to storage accounting: before={before:?}, after={after:?}"
    );
}

/// The gate's negative control: 64 KiB handed to `write` on a pipe, as an
/// eventfd wakeup is, dirties no page, so the gate does not see it. `wchar`
/// counts all of it (a socket would not do: `send` bypasses `wchar`).
#[cfg(target_os = "linux")]
#[test]
fn the_storage_write_gate_ignores_pipe_writes() {
    use std::io::{Read, Write};

    let _serial = serial();
    let (mut reader, mut writer) = std::io::pipe().expect("pipe");
    let drain = std::thread::spawn(move || {
        let mut received = Vec::new();
        reader.read_to_end(&mut received).expect("drain pipe");
        received.len()
    });
    let before = storage_write_bytes().expect("write_bytes on Linux");
    writer
        .write_all(&vec![0x5a; 64 * 1024])
        .expect("write pipe");
    drop(writer);
    let after = storage_write_bytes().expect("write_bytes on Linux");
    assert_eq!(drain.join().expect("drain thread"), 64 * 1024);
    assert_eq!(after - before, 0, "pipe writes reached storage accounting");
}

#[test]
fn strict_property_anchors_stream_topology_against_the_equality_build() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    for partitions in [1, 2, 4] {
        let forge = open_with_partitions(&built.project, partitions);
        settle_for_write_gate(&built.project);
        let query = "MATCH (a:Entity {ident: 5})-[:LINK]->(b) RETURN count(*) AS n";
        let physical = forge
            .explain_stage(query, graphforge_api::ExplainStage::PhysicalPlan)
            .unwrap();
        if partitions > 1 {
            assert!(
                physical.contains("HashJoinExec: mode=CollectLeft, join_type=Inner"),
                "{physical}"
            );
        }
        assert!(
            physical.contains("PropertyOverlayExec: route=_untyped, equality=ident=Int(5)"),
            "{physical}"
        );
        let (batches, io) = measured(&forge, ANCHORED_HOP, 5);
        assert_eq!(count(&batches), FAN_OUT as i64);
        assert_eq!(io.write_bytes, 0, "{io:?}");
        // Parallel execution wakes the runtime through eventfd and pipe writes,
        // which never dirty a page, so the gate holds at every partition count.
        #[cfg(target_os = "linux")]
        assert_eq!(io.storage_write_bytes, Some(0), "{io:?}");
        let (values, io) = measured(
            &forge,
            "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) \
             RETURN a.ident AS anchor, b.ident AS neighbor",
            5,
        );
        let mut pairs = Vec::new();
        for batch in &values {
            let anchors = batch
                .column_by_name("anchor")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let neighbors = batch
                .column_by_name("neighbor")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!anchors.is_null(row) && !neighbors.is_null(row));
                pairs.push((anchors.value(row), neighbors.value(row)));
            }
        }
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(5, 6), (5, 7)]);
        assert_eq!(io.write_bytes, 0, "{io:?}");
        #[cfg(target_os = "linux")]
        assert_eq!(io.storage_write_bytes, Some(0), "{io:?}");
        let (batches, io) = measured(&forge, ANCHORED_LOOKUP, i64::MAX);
        assert_eq!(count(&batches), 0);
        assert_eq!(io.write_bytes, 0, "{io:?}");
        #[cfg(target_os = "linux")]
        assert_eq!(io.storage_write_bytes, Some(0), "{io:?}");
    }
}

#[test]
fn optional_enrichment_preserves_fanout_and_missing_properties() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let query = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) \
                 OPTIONAL MATCH (b)-[:LINK]->(c) \
                 OPTIONAL MATCH (c)-[:ABSENT]->(d:Entity) \
                 RETURN a.ident AS anchor, b.ident AS neighbor, \
                        c.ident AS destination, d.ident AS missing";
    for partitions in [1, 2, 4] {
        let forge = open_with_partitions(&built.project, partitions);
        settle_for_write_gate(&built.project);
        let plan = forge
            .explain_stage(
                &query.replace("$ident", "5"),
                graphforge_api::ExplainStage::PhysicalPlan,
            )
            .unwrap();
        assert!(plan.contains("UuidBuildKeyTapExec"), "{plan}");
        assert!(
            plan.contains("HashJoinExec: mode=CollectLeft, join_type=Left"),
            "{plan}"
        );
        let (batches, io) = measured(&forge, query, 5);
        let mut rows = Vec::new();
        for batch in &batches {
            let values = ["anchor", "neighbor", "destination"].map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
            });
            let missing = batch.column_by_name("missing").unwrap();
            for row in 0..batch.num_rows() {
                assert!(values.iter().all(|array| !array.is_null(row)));
                assert!(
                    missing
                        .logical_nulls()
                        .is_some_and(|nulls| nulls.is_null(row))
                );
                rows.push(values.map(|array| array.value(row)));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![[5, 6, 7], [5, 6, 8], [5, 7, 8], [5, 7, 9]]);
        assert_eq!(io.write_bytes, 0, "{io:?}");
        assert_eq!(io.write_calls, 0, "{io:?}");
        #[cfg(target_os = "linux")]
        assert_eq!(io.storage_write_bytes, Some(0), "{io:?}");
    }
}

#[test]
fn whole_graph_counts_do_not_materialize_property_uuid_nominations() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let query = "MATCH (n) RETURN count(n) AS n";
    for partitions in [1, 2, 4] {
        let forge = open_with_partitions(&built.project, partitions);
        let plan = forge
            .explain_stage(query, graphforge_api::ExplainStage::PhysicalPlan)
            .unwrap();
        assert!(!plan.contains("UuidBuildKeyTapExec"), "{plan}");
        let result = forge.execute(query).unwrap();
        assert_eq!(count(&result.batches), SMALL_NODES as i64);
    }
}
