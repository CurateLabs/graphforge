//! An id anchor written beside a label disjunction, `(m:Post OR m:Comment) AND
//! m.id = $x`, reads the matching nodes' topology rows, not the node topology
//! (#1984).
//!
//! The GDC harness writes the `:Message` supertype as `(m)` with that
//! disjunction in `WHERE`. A variable-length expansion from such a node
//! (`(m)-[:REPLY_OF*0..]->(p:Post)`) kept the whole filter above the expansion,
//! so the node scan that seeds it was unanchored and decoded every node of the
//! graph to find one. The seed predicates now run below the expansion, where
//! the persisted equality index answers them.
//!
//! The growth gate counts bytes, never time, and counts them two ways because
//! the lifecycle read-path counters belong to the calling thread: with one
//! target partition the whole statement runs there, with more the plan's
//! exchanges run on worker threads the counters do not see. `rchar` in
//! `/proc/self/io` counts every byte any thread of the process asked the
//! kernel to read, so it holds at every partition count. UUIDs are random, so
//! the topology's identity column cannot compress away, and every node carries
//! incompressible text, so the property route spans several fragments.
//!
//! Only the first `CONNECTED` nodes reply to one another, so the edges the
//! expansion reads do not change when the graph doubles: the nodes that are
//! added are on no reply chain.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
/// Bytes of incompressible text on every node, so that a property fragment
/// fills its byte cap with a few thousand rows and the route spans fragments.
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;
/// Nodes `0..CONNECTED` form the reply forest; the rest reply to nothing.
const CONNECTED: usize = 4_096;
const ANCHORED: &str = "anchor=property_equality(id)";
const INDEXED: &str = "index=property_equality(id)";

/// The disjunction beside an equality, written the ways a statement can anchor
/// a message. The first returns the node's own properties; the others return a
/// row for each post on the reply chain.
const DISJUNCTION_ONLY: &str =
    "MATCH (m {id: $ident}) WHERE (m:Post OR m:Comment) RETURN m.id AS id, m.name AS name";
const SEED_IN_WHERE: &str = "MATCH (m)-[:REPLY_OF*0..]->(p:Post) \
     WHERE (m:Post OR m:Comment) AND m.id = $ident RETURN m.id AS id";
const SEED_IN_MAP: &str = "MATCH (m {id: $ident})-[:REPLY_OF*0..]->(p:Post) \
     WHERE (m:Post OR m:Comment) RETURN m.id AS id";

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

/// A random-looking UUID per node, so identities are neither compressible nor
/// ordered like the node index.
fn node_uuid(index: usize) -> Uuid {
    let high = mix(index as u64 + 1);
    let low = mix(high ^ 0x9e37_79b9_7f4a_7c15);
    Uuid::from_u128((u128::from(high) << 64) | u128::from(low))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
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
/// An id names one node of the whole graph, as a message id does.
fn scattered(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
}

/// Node 0 is a post and so is every fourth node after it; a quarter are
/// persons, which carry ids too; the rest are comments.
fn label(index: usize) -> &'static str {
    match index % 4 {
        0 => "Post",
        3 => "Person",
        _ => "Comment",
    }
}

/// Every connected node but the first replies to a node nearer the first: a
/// chain of at most `log2(CONNECTED)` replies ends at node 0.
fn replied_to(index: usize) -> usize {
    (index - 1) / 2
}

/// The nodes a reply chain visits from `seed`, the seed included.
fn chain(seed: usize) -> Vec<usize> {
    let mut at = seed;
    let mut visited = vec![at];
    while at != 0 && seed < CONNECTED {
        at = replied_to(at);
        visited.push(at);
    }
    visited
}

fn construct(project: &Path, nodes: usize) {
    let forge = GraphForge::new(Some(project.to_str().expect("utf-8 path"))).unwrap();
    construct_into(&forge, nodes);
}

fn construct_into(forge: &GraphForge, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("name", DataType::Utf8, true)));
    fields.push(Arc::new(Field::new("padding", DataType::Utf8, true)));
    let schema = Arc::new(Schema::new(fields));
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
        let mut labels = StringBuilder::new();
        let mut ids = Int64Builder::with_capacity(end - start);
        let mut names = StringBuilder::new();
        for node in start..end {
            uuids.append_value(node_uuid(node).as_bytes()).unwrap();
            labels.append_value(label(node));
            ids.append_value(scattered(node, nodes));
            names.append_value(format!("n{node}"));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(uuids.finish()) as ArrayRef,
                Arc::new(labels.finish()),
                Arc::new(ids.finish()),
                Arc::new(names.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let connected = CONNECTED.min(nodes);
    for start in (1..connected).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(connected);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for source in start..end {
            ids.append_value(edge_uuid(source).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid(replied_to(source)).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["REPLY_OF"; rows])),
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

fn params(ident: i64) -> HashMap<String, IrLiteral> {
    HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))])
}

fn plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("plan")
}

/// Bytes the process asked the kernel to read, on any thread; `None` off Linux.
#[cfg(target_os = "linux")]
fn process_read_bytes() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io")
        .expect("task I/O accounting unavailable: cannot read /proc/self/io");
    Some(
        io.lines()
            .find_map(|line| line.strip_prefix("rchar: ")?.trim().parse().ok())
            .expect("task I/O accounting unavailable: /proc/self/io has no valid rchar line"),
    )
}

#[cfg(not(target_os = "linux"))]
fn process_read_bytes() -> Option<u64> {
    None
}

/// What one statement read.
#[derive(Debug, Clone, Copy)]
struct Reads {
    /// Read-path bytes the calling thread observed.
    observed: u64,
    /// Bytes any thread of the process read.
    process: Option<u64>,
    /// Node topology rows decoded by ordinal selection.
    selected_rows: u64,
    /// Node topology rows read by any other path the counters see.
    full_rows: u64,
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, Reads) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    graphforge_storage::io_stats::reset();
    let process_before = process_read_bytes();
    let result = forge
        .execute_with_params(query, &params(ident))
        .expect("statement executes");
    let process_after = process_read_bytes();
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    let stats = graphforge_storage::io_stats::snapshot().expect("requested I/O statistics");
    (
        result.batches,
        Reads {
            observed: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
            process: process_before.zip(process_after).map(|(b, a)| a - b),
            selected_rows: stats.node_exact_rows_selected,
            full_rows: stats.node_full_rows,
        },
    )
}

type Rows = Vec<Vec<Option<String>>>;

fn rows_of(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        (!column.is_null(row)).then(|| {
                            arrow::util::display::array_value_to_string(column, row)
                                .expect("value formats")
                        })
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

fn answer(forge: &GraphForge, query: &str, ident: i64) -> Rows {
    rows_of(
        &forge
            .execute_with_params(query, &params(ident))
            .unwrap_or_else(|error| panic!("{query}: {error}"))
            .batches,
    )
}

fn row(id: i64, name: Option<&str>) -> Vec<Option<String>> {
    vec![Some(id.to_string()), name.map(str::to_owned)]
}

fn id_row(id: i64) -> Vec<Option<String>> {
    vec![Some(id.to_string())]
}

/// Storage-layer accounting is process-wide, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: PathBuf,
    nodes: usize,
}

fn build(nodes: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, nodes);
    Built {
        _root: root,
        project,
        nodes,
    }
}

/// What the construction holds for the message at node index `seed`.
fn expected(seed: usize, nodes: usize, query: &str) -> Rows {
    let ident = scattered(seed, nodes);
    if label(seed) == "Person" {
        return Vec::new();
    }
    if query == DISJUNCTION_ONLY {
        return vec![row(ident, Some(&format!("n{seed}")))];
    }
    // A row for each post on the chain, the seed included when it is one.
    let mut rows = chain(seed)
        .into_iter()
        .filter(|node| label(*node) == "Post")
        .map(|_| id_row(ident))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Nodes the growth gate looks up: connected, in different fragments, and of
/// both message labels.
const LOOKUPS: [usize; 3] = [5, 1_500, 4_001];

/// A fixed amount of the node topology answers a message anchor however large
/// the graph is, at every partition count.
///
/// The rows decoded from the topology are counted exactly for every form: the
/// seed and the nodes its reply chain visits, and no node file read whole. The
/// total bytes a statement reads are gated for the form that returns the node's
/// own properties. A statement that expands from the seed also joins the nodes
/// it reaches to the property route, and the bytes of that join are not the
/// seed scan's: this test does not bound them.
#[test]
fn a_message_anchor_reads_the_match_not_the_node_topology() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    for query in [DISJUNCTION_ONLY, SEED_IN_WHERE, SEED_IN_MAP] {
        for partitions in [1, 2, 4] {
            let mut by_size = Vec::new();
            for built in [&small, &large] {
                let forge = open(&built.project, partitions);
                let physical = plan(&forge, &query.replace("$ident", "5"));
                assert!(
                    physical.contains(ANCHORED) && physical.contains(INDEXED),
                    "partitions={partitions}: {query}\n{physical}"
                );
                // The first statement admits the topology and property
                // footers once per session.
                let (warm, _) = measured(&forge, query, scattered(LOOKUPS[0], built.nodes));
                assert_eq!(
                    rows_of(&warm),
                    expected(LOOKUPS[0], built.nodes, query),
                    "partitions={partitions}: {query}"
                );
                let mut worst = (0_u64, Some(0_u64));
                for seed in LOOKUPS {
                    let ident = scattered(seed, built.nodes);
                    let (batches, reads) = measured(&forge, query, ident);
                    assert_eq!(
                        rows_of(&batches),
                        expected(seed, built.nodes, query),
                        "partitions={partitions}, {} nodes: {query}",
                        built.nodes
                    );
                    // No path reads a node file whole, and the rows decoded by
                    // ordinal are the seed and the nodes the chain visits.
                    // Only one partition runs on the thread these counters
                    // belong to.
                    if partitions == 1 {
                        assert_eq!(reads.full_rows, 0, "{query}: {reads:?}");
                        let visited = if query == DISJUNCTION_ONLY {
                            1
                        } else {
                            chain(seed).len() as u64
                        };
                        assert!(
                            (1..=visited).contains(&reads.selected_rows),
                            "{query}: seed {seed} visits {visited}: {reads:?}"
                        );
                    }
                    worst = (
                        worst.0.max(reads.observed),
                        worst.1.zip(reads.process).map(|(a, b)| a.max(b)),
                    );
                }
                by_size.push(worst);
            }
            if query != DISJUNCTION_ONLY {
                continue;
            }
            let (small_reads, large_reads) = (by_size[0], by_size[1]);
            // One identity column is 16 bytes a node and does not compress, so
            // a scan of the topology alone grows by far more than this.
            let allowance = 32 << 10;
            if partitions == 1 {
                assert!(
                    large_reads.0 <= small_reads.0 + allowance,
                    "{query}: reads grew with the node topology: {small_reads:?} -> {large_reads:?}"
                );
            }
            if let (Some(small), Some(large)) = (small_reads.1, large_reads.1) {
                assert!(
                    large <= small + allowance,
                    "{query}: partitions={partitions}: process reads grew with the node \
                     topology: {small} -> {large}"
                );
            }
        }
    }
}

/// The label is checked on the row the index named: a node that carries the
/// value under a label outside the disjunction does not match.
#[test]
fn an_id_of_another_label_matches_nothing() {
    let _serial = serial();
    let built = build(2_048);
    let forge = open(&built.project, 1);
    let post = scattered(4, built.nodes);
    let comment = scattered(5, built.nodes);
    let person = scattered(3, built.nodes);
    assert_eq!(
        (label(4), label(5), label(3)),
        ("Post", "Comment", "Person")
    );
    for query in [DISJUNCTION_ONLY, SEED_IN_WHERE, SEED_IN_MAP] {
        let physical = plan(&forge, &query.replace("$ident", "1"));
        assert!(physical.contains(ANCHORED), "{query}\n{physical}");
        for (ident, seed) in [(post, 4), (comment, 5), (person, 3)] {
            assert_eq!(
                answer(&forge, query, ident),
                expected(seed, built.nodes, query),
                "{query} with {ident}"
            );
        }
        // No node holds the value.
        assert_eq!(answer(&forge, query, 99_999_999), Rows::new(), "{query}");
        assert!(
            answer(&forge, query, person).is_empty(),
            "a person is not a message: {query}"
        );
    }
    // Written with the person's label too, the same equality finds the person.
    let wider = "MATCH (m {id: $ident}) WHERE (m:Post OR m:Comment OR m:Person) \
                 RETURN m.id AS id, m.name AS name";
    assert!(plan(&forge, &wider.replace("$ident", "1")).contains(ANCHORED));
    assert_eq!(
        answer(&forge, wider, person),
        [row(person, Some("n3"))],
        "{wider}"
    );
}

/// An index that does not describe the route is not used: the topology scan
/// reads every node again, the answers do not change, and rebuilding the index
/// restores the anchor.
#[test]
fn a_stale_index_falls_back_to_the_scan_and_stays_correct() {
    let _serial = serial();
    let built = build(2_048);
    let forge = open(&built.project, 1);
    let nodes = built.nodes;
    let comment = scattered(5, nodes);
    let queries = [DISJUNCTION_ONLY, SEED_IN_WHERE, SEED_IN_MAP];
    for query in queries {
        let physical = plan(&forge, &query.replace("$ident", "1"));
        assert!(physical.contains(ANCHORED), "{query}\n{physical}");
        assert_eq!(answer(&forge, query, comment), expected(5, nodes, query));
    }

    // A write changes the route, so the index no longer describes it. The new
    // comment replies to the first post.
    let first_post = scattered(0, nodes);
    forge
        .execute(&format!(
            "MATCH (t:Post {{id: {first_post}}}) \
             CREATE (:Comment {{id: 1000000, name: 'late'}})-[:REPLY_OF]->(t)"
        ))
        .unwrap();
    let check = |anchored: bool| {
        for query in queries {
            let physical = plan(&forge, &query.replace("$ident", "1"));
            assert_eq!(
                physical.contains("anchor=") || physical.contains("index="),
                anchored,
                "{query}\n{physical}"
            );
            assert_eq!(
                answer(&forge, query, comment),
                expected(5, nodes, query),
                "{query}"
            );
            // The new comment, and for a chain the first post it replies to.
            let late = if query == DISJUNCTION_ONLY {
                vec![row(1_000_000, Some("late"))]
            } else {
                vec![id_row(1_000_000)]
            };
            assert_eq!(answer(&forge, query, 1_000_000), late, "{query}");
        }
    };
    check(false);
    // The unanchored statement reads the node topology whole.
    let (_, stale) = measured(&forge, DISJUNCTION_ONLY, comment);
    assert_eq!(stale.selected_rows, 0, "{stale:?}");

    forge.rebuild_property_indexes(None).unwrap();
    check(true);
    // The same statement reads far less through the anchor than it did through
    // the scan: its answer is one row of the property route and one of the
    // topology, not the whole of either.
    let (_, anchored) = measured(&forge, DISJUNCTION_ONLY, comment);
    assert!(anchored.selected_rows >= 1, "{anchored:?}");
    assert!(
        anchored.observed + (256 << 10) < stale.observed,
        "the anchor did not narrow the reads: {stale:?} -> {anchored:?}"
    );
}

/// Nodes with several labels, nodes that share a value, deleted nodes, and
/// lower bounds above zero answer the same through the anchor as through the
/// scan.
#[test]
fn multi_label_shared_and_deleted_nodes_answer_as_the_scan_does() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let forge = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    for statement in [
        "CREATE (:Post:Comment {id: 1, name: 'both'})",
        "CREATE (:Post {id: 2, name: 'p2'})",
        "CREATE (:Comment {id: 2, name: 'c2'})",
        "CREATE (:Person {id: 2, name: 'x2'})",
        "CREATE (:Person {id: 3, name: 'x3'})",
        "CREATE (:Comment {id: 5, name: 'c5'})-[:REPLY_OF]->(:Comment {id: 6, name: 'c6'})\
         -[:REPLY_OF]->(:Post {id: 4, name: 'p4'})",
    ] {
        forge.execute(statement).unwrap();
    }
    forge.rebuild_property_indexes(None).unwrap();

    let own = "MATCH (m {id: $ident}) WHERE (m:Post OR m:Comment) \
               RETURN m.id AS id, m.name AS name";
    let from_zero = "MATCH (m)-[:REPLY_OF*0..]->(p:Post) \
                     WHERE (m:Post OR m:Comment) AND m.id = $ident \
                     RETURN m.id AS id, p.name AS name";
    let from_one = "MATCH (m {id: $ident})-[:REPLY_OF*1..]->(p:Post) \
                    WHERE (m:Post OR m:Comment) RETURN m.id AS id, p.name AS name";
    let cases: Vec<(&str, i64, Rows)> = vec![
        // One node with both labels is one row, not two.
        (own, 1, vec![row(1, Some("both"))]),
        (from_zero, 1, vec![row(1, Some("both"))]),
        (from_one, 1, vec![]),
        // Three labels share a value; the person does not match.
        (own, 2, vec![row(2, Some("c2")), row(2, Some("p2"))]),
        (from_zero, 2, vec![row(2, Some("p2"))]),
        (from_one, 2, vec![]),
        (own, 3, vec![]),
        (from_zero, 3, vec![]),
        (own, 4, vec![row(4, Some("p4"))]),
        (from_zero, 4, vec![row(4, Some("p4"))]),
        (own, 5, vec![row(5, Some("c5"))]),
        (from_zero, 5, vec![row(5, Some("p4"))]),
        (from_one, 5, vec![row(5, Some("p4"))]),
        (from_zero, 99, vec![]),
    ];
    let run = |anchored: bool, cases: &[(&str, i64, Rows)], stage: &str| {
        for (query, ident, expected) in cases {
            let physical = plan(&forge, &query.replace("$ident", &ident.to_string()));
            assert_eq!(
                physical.contains("anchor="),
                anchored,
                "{stage}: {query}\n{physical}"
            );
            assert_eq!(
                &answer(&forge, query, *ident),
                expected,
                "{stage}: {query} with {ident}"
            );
        }
    };
    run(true, &cases, "anchored");

    // Delete the post that shares its value with a comment and a person.
    forge
        .execute("MATCH (n:Post {id: 2}) DETACH DELETE n")
        .unwrap();
    forge.rebuild_property_indexes(None).unwrap();
    let after_delete: Vec<(&str, i64, Rows)> = vec![
        (own, 2, vec![row(2, Some("c2"))]),
        (from_zero, 2, vec![]),
        (own, 4, vec![row(4, Some("p4"))]),
        (from_zero, 5, vec![row(5, Some("p4"))]),
    ];
    run(true, &after_delete, "after delete");

    // The scan gives the same answers once the index is stale.
    forge.execute("CREATE (:Place {id: 99})").unwrap();
    run(false, &after_delete, "scanned");
}

/// The message-to-forum-moderator statement of the GDC harness: a variable-length
/// expansion from the message, then fixed hops to the forum and its moderator.
/// The relationship-uniqueness tests the lowering adds between the hops must not
/// keep the seed predicates above the expansion.
const MODERATOR_OF_MESSAGE: &str = "MATCH (m)-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)\
     -[:HAS_MODERATOR]->(mod:Person) \
     WHERE (m:Post OR m:Comment) AND m.id = $ident \
     RETURN f.id AS forumId, f.title AS forumTitle, mod.id AS moderatorId, \
            mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName";

/// The same expansion followed by six fixed hops, a pattern deeper than the
/// optimizer's passes could carry a seed predicate across one operator at a time.
const DEEP_FROM_MESSAGE: &str = "MATCH (m)-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)\
     -[:HAS_MODERATOR]->(u:Person)<-[:HAS_MODERATOR]-(g:Forum)-[:CONTAINER_OF]->(q:Post)\
     <-[:REPLY_OF]-(c:Comment)<-[:REPLY_OF]-(d:Comment) \
     WHERE (m:Post OR m:Comment) AND m.id = $ident RETURN d.id AS id";

fn moderator_row(
    forum: i64,
    title: &str,
    person: i64,
    first: &str,
    last: &str,
) -> Vec<Option<String>> {
    vec![
        Some(forum.to_string()),
        Some(title.to_owned()),
        Some(person.to_string()),
        Some(first.to_owned()),
        Some(last.to_owned()),
    ]
}

/// A message anchored through the index seeds the expansion that continues
/// through fixed hops to the forum and its moderator, and answers as the scan
/// does, anchored and not.
#[test]
fn a_message_anchor_seeds_the_expansion_through_to_the_forum_moderator() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let writer = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    for statement in [
        "CREATE (:Forum {id: 1, title: 'Wall of Ann'})\
         -[:HAS_MODERATOR]->(:Person {id: 2, firstName: 'Ann', lastName: 'Lee'})",
        "MATCH (f:Forum {id: 1}) CREATE (f)-[:CONTAINER_OF]->(:Post {id: 10})",
        "MATCH (p:Post {id: 10}) CREATE (:Comment {id: 11})-[:REPLY_OF]->(p)",
        "MATCH (c:Comment {id: 11}) CREATE (:Comment {id: 12})-[:REPLY_OF]->(c)",
        "CREATE (:Forum {id: 3, title: 'Wall of Bo'})\
         -[:HAS_MODERATOR]->(:Person {id: 4, firstName: 'Bo', lastName: 'Ray'})",
        "MATCH (f:Forum {id: 3}) CREATE (f)-[:CONTAINER_OF]->(:Post {id: 20})",
        "MATCH (p:Post {id: 20}) CREATE (:Comment {id: 21})-[:REPLY_OF]->(p)",
        // A post in no forum, and a person that shares a message's id.
        "CREATE (:Post {id: 30})",
        "CREATE (:Person {id: 11, firstName: 'Cy', lastName: 'Day'})",
    ] {
        writer.execute(statement).unwrap();
    }
    drop(writer);
    let forge = open(&project, 1);
    forge.rebuild_property_indexes(None).unwrap();

    let ann = vec![moderator_row(1, "Wall of Ann", 2, "Ann", "Lee")];
    let bo = vec![moderator_row(3, "Wall of Bo", 4, "Bo", "Ray")];
    let cases: Vec<(i64, Rows)> = vec![
        // The post itself, a reply to it, and a reply to that reply.
        (10, ann.clone()),
        (11, ann.clone()),
        (12, ann),
        (20, bo.clone()),
        (21, bo),
        // A post in no forum, a person, and a value nothing holds.
        (30, vec![]),
        (2, vec![]),
        (99, vec![]),
    ];
    let run = |anchored: bool, stage: &str| {
        for (ident, expected) in &cases {
            let physical = plan(
                &forge,
                &MODERATOR_OF_MESSAGE.replace("$ident", &ident.to_string()),
            );
            assert_eq!(
                physical.contains(ANCHORED) && physical.contains(INDEXED),
                anchored,
                "{stage}: {ident}\n{physical}"
            );
            assert_eq!(
                &answer(&forge, MODERATOR_OF_MESSAGE, *ident),
                expected,
                "{stage}: {ident}"
            );
        }
    };
    run(true, "anchored");

    // The scan gives the same answers once a write leaves the index stale, and
    // rebuilding the index restores the anchor.
    forge.execute("CREATE (:Place {id: 99})").unwrap();
    run(false, "stale");
    forge.rebuild_property_indexes(None).unwrap();
    run(true, "rebuilt");
    // A seed predicate crosses every later hop, however many the pattern has.
    let physical = plan(&forge, &DEEP_FROM_MESSAGE.replace("$ident", "10"));
    assert!(
        physical.contains(ANCHORED) && physical.contains(INDEXED),
        "{physical}"
    );
}

/// The anchored seed narrows what the whole statement reads: the seed scan of
/// the message reads the matching node's rows by ordinal instead of every
/// node's, through the hops to a forum and its moderator.
#[test]
fn a_message_anchor_narrows_the_reads_of_the_statement_to_the_moderator() {
    let _serial = serial();
    let built = build(2_048);
    let nodes = built.nodes;
    let forge = open(&built.project, 1);
    // One forum holds the root post of the reply forest and has a moderator.
    let post = scattered(0, nodes);
    let person = scattered(3, nodes);
    forge
        .execute(&format!(
            "MATCH (p:Post {{id: {post}}}), (u:Person {{id: {person}}}) \
             CREATE (f:Forum {{id: 5000000, title: 'forum'}}), \
                    (f)-[:CONTAINER_OF]->(p), (f)-[:HAS_MODERATOR]->(u) \
             SET u.firstName = 'Ann', u.lastName = 'Lee'"
        ))
        .unwrap();
    let forum = vec![
        Some("5000000".to_owned()),
        Some("forum".to_owned()),
        Some(person.to_string()),
        Some("Ann".to_owned()),
        Some("Lee".to_owned()),
    ];
    // The comment two replies below the root post, the post itself, and a
    // person, which is no message and so matches the disjunction nowhere.
    let cases: Vec<(usize, Rows)> = vec![(5, vec![forum.clone()]), (0, vec![forum]), (3, vec![])];
    let check = |anchored: bool| {
        for (seed, expected) in &cases {
            let ident = scattered(*seed, nodes);
            let physical = plan(
                &forge,
                &MODERATOR_OF_MESSAGE.replace("$ident", &ident.to_string()),
            );
            assert_eq!(
                physical.contains(ANCHORED) && physical.contains(INDEXED),
                anchored,
                "{seed}\n{physical}"
            );
            assert_eq!(
                &answer(&forge, MODERATOR_OF_MESSAGE, ident),
                expected,
                "{seed}"
            );
        }
        measured(&forge, MODERATOR_OF_MESSAGE, scattered(5, nodes)).1
    };
    let stale = check(false);
    forge.rebuild_property_indexes(None).unwrap();
    let anchored = check(true);
    // The fixed hops to the forum read the same rows either way, so the row
    // counters agree; the seed scan is what the anchor narrows, in bytes.
    assert!(
        anchored.observed + (64 << 10) < stale.observed,
        "the anchor did not narrow the reads: {stale:?} -> {anchored:?}"
    );
}

/// An in-memory project builds the index and anchors the same way.
#[test]
fn an_in_memory_project_anchors_too() {
    let _serial = serial();
    let forge = GraphForge::new(None).unwrap();
    construct_into(&forge, 1_000);
    let nodes = 1_000;
    for query in [DISJUNCTION_ONLY, SEED_IN_WHERE, SEED_IN_MAP] {
        let physical = plan(&forge, &query.replace("$ident", "1"));
        assert!(
            physical.contains(ANCHORED) && physical.contains(INDEXED),
            "{query}\n{physical}"
        );
        for seed in [5, 20, 3] {
            assert_eq!(
                answer(&forge, query, scattered(seed, nodes)),
                expected(seed, nodes, query),
                "{query} from {seed}"
            );
        }
    }
}
