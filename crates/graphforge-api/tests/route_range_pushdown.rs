//! A range predicate on one property of a durable project is planned into the
//! property scan, which skips the row groups and fragments whose statistics
//! cannot satisfy it (#1982).
//!
//! The route is several property fragments wide and `ident` rises with the
//! UUID, so each base fragment holds a disjoint range of it; `shuffled` is a
//! permutation that spreads every value over all fragments and so cannot be
//! pruned. The gates are deterministic and count bytes, never time:
//!
//! - the lifecycle read-path counters, which count what a statement reads
//!   through the storage layer, and
//! - answers, compared with an oracle computed from the data the test wrote and
//!   with a statement written so that no range can be pushed.
//!
//! Pruning only chooses what to decode: the predicate stays above the scan, so
//! a mutation that moves a value into or out of the range, a tombstone and a
//! NULL must give the answer a full scan gives, at every partition count and
//! after a reopen.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage, GraphConstructionBudgets,
    GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase, lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;

const WRITE_WINDOW: usize = 4 * 1024;
/// Bytes of incompressible text on every node, so that a fragment fills its
/// byte cap with a few thousand rows and the route spans several fragments.
const PADDING_BYTES: usize = 1024;
const NODES: usize = 12_288;
/// 2021-01-01T00:00:00Z in epoch milliseconds, the form the LDBC benchmarks
/// store their timestamps in; node `i` was created `i` seconds later.
const EPOCH_2021_MILLIS: i64 = 1_609_459_200_000;

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

/// A permutation of the indices: every fragment holds values from the whole span.
fn shuffled(index: usize) -> i64 {
    ((index * 7919 + 13) % NODES) as i64
}

/// Zero-padded, so byte order is numeric order and each fragment holds a
/// disjoint run of tags.
fn tag(index: usize) -> String {
    format!("k{index:06}")
}

/// NULL for every fifth node.
fn sparse(index: usize) -> Option<i64> {
    (index % 5 != 0).then_some(index as i64)
}

/// What the data holds, kept in step with every mutation the test applies, so
/// that an expected count never comes from the engine under test.
#[derive(Clone)]
struct Model {
    /// `None` for a deleted node.
    nodes: Vec<Option<Node>>,
}

#[derive(Clone)]
struct Node {
    ident: Option<i64>,
    tag: Option<String>,
    sparse: Option<i64>,
}

impl Model {
    fn new() -> Self {
        Self {
            nodes: (0..NODES)
                .map(|index| {
                    Some(Node {
                        ident: Some(index as i64),
                        tag: Some(tag(index)),
                        sparse: sparse(index),
                    })
                })
                .collect(),
        }
    }

    fn live(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().flatten()
    }

    fn count_ident(&self, holds: impl Fn(i64) -> bool) -> i64 {
        self.live()
            .filter(|node| node.ident.is_some_and(&holds))
            .count() as i64
    }

    fn count_tag(&self, holds: impl Fn(&str) -> bool) -> i64 {
        self.live()
            .filter(|node| node.tag.as_deref().is_some_and(&holds))
            .count() as i64
    }

    fn count_sparse(&self, holds: impl Fn(i64) -> bool) -> i64 {
        self.live()
            .filter(|node| node.sparse.is_some_and(&holds))
            .count() as i64
    }

    fn node_with_ident(&mut self, ident: i64) -> &mut Option<Node> {
        self.nodes
            .iter_mut()
            .find(|node| {
                node.as_ref()
                    .is_some_and(|node| node.ident == Some(ident))
            })
            .expect("a node holds the ident")
    }
}

fn construct(dir: &Path) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("shuffled", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("sparse", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("tag", DataType::Utf8, true)));
    fields.push(Arc::new(Field::new("created", DataType::Int64, true)));
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
    for start in (0..NODES).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(NODES);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        let mut scattered = Int64Builder::with_capacity(end - start);
        let mut sparse_values = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
            scattered.append_value(shuffled(node));
            sparse_values.append_option(sparse(node));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
                Arc::new(scattered.finish()),
                Arc::new(sparse_values.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(tag))),
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|node| EPOCH_2021_MILLIS + node as i64 * 1_000),
                )),
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

/// Storage-layer accounting is process-wide and the fixtures of this binary
/// write files, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
}

fn build() -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project);
    Built {
        _root: root,
        project,
    }
}

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

fn matching(predicate: &str) -> String {
    format!("MATCH (a:Entity) WHERE {predicate} RETURN count(*) AS n")
}

fn answer(forge: &GraphForge, predicate: &str) -> i64 {
    let result = forge.execute(&matching(predicate)).expect("statement runs");
    count(&result.batches)
}

/// One target partition keeps the statement on the calling thread, where the
/// lifecycle capture is installed, so it observes every read-path byte.
fn read_bytes(forge: &GraphForge, predicate: &str) -> (i64, u64) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge.execute(&matching(predicate)).expect("statement runs");
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    (
        count(&result.batches),
        delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
    )
}

fn plan(forge: &GraphForge, predicate: &str) -> String {
    forge
        .explain_stage(&matching(predicate), ExplainStage::PhysicalPlan)
        .expect("plan")
}

fn property_fragments(project: &Path) -> usize {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .count()
}

#[test]
fn a_range_is_planned_into_the_property_scan() {
    let _serial = serial();
    let built = build();
    let forge = open(&built.project, 1);
    for pushable in [
        "a.ident >= 100",
        "a.ident > 100 AND a.ident <= 200",
        "a.ident < 100",
        "100 <= a.ident",
        "a.tag >= 'k000100' AND a.tag < 'k000200'",
    ] {
        let plan = plan(&forge, pushable);
        assert!(
            plan.contains("PropertyOverlayExec") && plan.contains(", range="),
            "{pushable}\n{plan}"
        );
    }
    // Arithmetic on the column cannot be compared with statistics.
    let unpushed = plan(&forge, "a.ident + 0 >= 100");
    assert!(!unpushed.contains(", range="), "{unpushed}");
}

/// The fragments are disjoint in `ident`, so a narrow range is one fragment's
/// bytes where a range that cannot be pruned is the route's.
#[test]
fn a_narrow_range_reads_only_the_fragments_that_can_hold_it() {
    let _serial = serial();
    let built = build();
    let fragments = property_fragments(&built.project);
    assert!(fragments >= 4, "{fragments} property fragments");
    let forge = open(&built.project, 1);
    // The first statement opens and authenticates every fragment, which later
    // ones do not repeat; measure against the statement that repeats it.
    read_bytes(&forge, "a.ident >= 0");
    let (full_count, full) = read_bytes(&forge, "a.ident >= 0");
    assert_eq!(full_count, NODES as i64);
    let (narrow_count, narrow) = read_bytes(&forge, "a.ident >= 100 AND a.ident < 200");
    assert_eq!(narrow_count, 100);
    let (high_count, high) = read_bytes(&forge, "a.ident >= 12000");
    assert_eq!(high_count, NODES as i64 - 12_000);
    let (scattered_count, scattered) = read_bytes(&forge, "a.shuffled >= 100 AND a.shuffled < 200");
    assert_eq!(scattered_count, 100);
    let (strings_count, strings) = read_bytes(&forge, "a.tag >= 'k000100' AND a.tag < 'k000200'");
    assert_eq!(strings_count, 100);
    let (stamps_count, stamps) = read_bytes(
        &forge,
        &format!(
            "a.created >= {} AND a.created < {}",
            EPOCH_2021_MILLIS + 100_000,
            EPOCH_2021_MILLIS + 200_000
        ),
    );
    assert_eq!(stamps_count, 100);
    eprintln!(
        "{fragments} fragments; read bytes: full={full} narrow={narrow} high={high} \
         scattered={scattered} strings={strings} timestamps={stamps}"
    );
    // A fragment is authenticated whole, so what a range saves is the fragments
    // it never opens: at most one or two of the route's own here.
    assert!(narrow * 2 < full, "narrow={narrow} full={full}");
    assert!(high * 2 < full, "high={high} full={full}");
    assert!(strings * 2 < full, "strings={strings} full={full}");
    assert!(stamps * 2 < full, "timestamps={stamps} full={full}");
    // The same width over a column no statistics can bound reads the route.
    assert!(
        scattered * 10 >= full * 9,
        "scattered={scattered} narrow={narrow} full={full}"
    );
}

/// `thorough` adds the edge cases; the mutation test runs the short form after
/// each statement and the long one on the final state.
fn assert_answers(forge: &GraphForge, model: &Model, label: &str, thorough: bool) {
    let ident = |predicate: &str, expected: i64| {
        assert_eq!(answer(forge, predicate), expected, "{label}: {predicate}");
        // The same predicate written so that no range reaches the scan.
        let unpushed = predicate.replace("a.ident", "(a.ident + 0)");
        assert_eq!(answer(forge, &unpushed), expected, "{label}: {unpushed}");
    };
    ident("a.ident > 5", model.count_ident(|v| v > 5));
    ident("a.ident < 5", model.count_ident(|v| v < 5));
    ident(
        "a.ident >= 100 AND a.ident < 200",
        model.count_ident(|v| (100..200).contains(&v)),
    );
    ident(
        "a.ident >= 3990 AND a.ident <= 4010",
        model.count_ident(|v| (3990..=4010).contains(&v)),
    );
    ident("a.ident > 9000", model.count_ident(|v| v > 9000));
    if thorough {
        ident("a.ident >= 0", model.count_ident(|v| v >= 0));
        ident("a.ident <= 5", model.count_ident(|v| v <= 5));
        ident(
            "a.ident > 4000 AND a.ident <= 4010",
            model.count_ident(|v| v > 4000 && v <= 4010),
        );
        ident("a.ident >= 8999", model.count_ident(|v| v >= 8999));
        ident(
            "a.ident >= 50 AND a.ident <= 10",
            model.count_ident(|v| (50..=10).contains(&v)),
        );
        ident("a.ident >= 50000", model.count_ident(|v| v >= 50_000));
        ident("a.ident < -1", model.count_ident(|v| v < -1));
        ident("a.ident >= 12280", model.count_ident(|v| v >= 12_280));
    }

    for (predicate, holds) in [
        (
            "a.tag >= 'k000100' AND a.tag < 'k000200'",
            (|tag: &str| ("k000100".."k000200").contains(&tag)) as fn(&str) -> bool,
        ),
        ("a.tag > 'k012280'", |tag: &str| tag > "k012280"),
        ("a.tag >= 'k999999'", |tag: &str| tag >= "k999999"),
        ("a.tag <= 'k000005'", |tag: &str| tag <= "k000005"),
        ("a.tag >= 'k100000'", |tag: &str| tag >= "k100000"),
        ("a.tag < 'k000000'", |tag: &str| tag < "k000000"),
    ]
    .into_iter()
    .take(if thorough { 6 } else { 3 })
    {
        assert_eq!(
            answer(forge, predicate),
            model.count_tag(holds),
            "{label}: {predicate}"
        );
    }

    for (predicate, holds) in [
        ("a.sparse >= 0", (|v: i64| v >= 0) as fn(i64) -> bool),
        ("a.sparse < 1000", |v: i64| v < 1000),
        ("a.sparse > 5 AND a.sparse <= 100", |v: i64| (6..=100).contains(&v)),
        ("a.sparse <= 0", |v: i64| v <= 0),
    ]
    .into_iter()
    .take(if thorough { 4 } else { 2 })
    {
        assert_eq!(
            answer(forge, predicate),
            model.count_sparse(holds),
            "{label}: {predicate}"
        );
    }
}

#[test]
fn pushing_a_range_does_not_change_an_answer() {
    let _serial = serial();
    let built = build();
    let model = Model::new();
    for partitions in [1, 4] {
        let forge = open(&built.project, partitions);
        assert_answers(&forge, &model, &format!("{partitions} partitions"), true);
        // A moment of the construction's own timestamps: every node 100..200.
        assert_eq!(
            answer(
                &forge,
                &format!(
                    "a.created >= {} AND a.created < {}",
                    EPOCH_2021_MILLIS + 100_000,
                    EPOCH_2021_MILLIS + 200_000
                )
            ),
            100,
            "{partitions} partitions: created"
        );
    }
}

/// Mutation fragments move values into and out of a range, delete nodes and set
/// a value to NULL. The newest snapshot of a UUID decides, whichever fragment a
/// range finds an older value in, and a reopened project agrees.
#[test]
fn mutations_that_cross_the_range_give_the_answer_a_full_scan_gives() {
    let _serial = serial();
    let built = build();
    let mut model = Model::new();
    let forge = open(&built.project, 1);
    let statements: [(&str, Box<dyn Fn(&mut Model)>); 8] = [
        // Out of the low range, into the top of the route.
        (
            "MATCH (a:Entity {ident: 7}) SET a.ident = 9000",
            Box::new(|model| {
                model.node_with_ident(7).as_mut().unwrap().ident = Some(9000);
            }),
        ),
        // Out of a high range, into the low one.
        (
            "MATCH (a:Entity {ident: 9001}) SET a.ident = 3",
            Box::new(|model| {
                model.node_with_ident(9001).as_mut().unwrap().ident = Some(3);
            }),
        ),
        // Tombstones: one inside the range of the next queries, one at its edge.
        (
            "MATCH (a:Entity {ident: 4000}) DELETE a",
            Box::new(|model| *model.node_with_ident(4000) = None),
        ),
        (
            "MATCH (a:Entity {ident: 150}) DELETE a",
            Box::new(|model| *model.node_with_ident(150) = None),
        ),
        // A value replaced by NULL.
        (
            "MATCH (a:Entity {ident: 4001}) SET a.ident = null",
            Box::new(|model| {
                model.node_with_ident(4001).as_mut().unwrap().ident = None;
            }),
        ),
        // Strings and a sparse column move too.
        (
            "MATCH (a:Entity {ident: 120}) SET a.tag = 'k999999'",
            Box::new(|model| {
                model.node_with_ident(120).as_mut().unwrap().tag = Some("k999999".into());
            }),
        ),
        (
            "MATCH (a:Entity {ident: 11}) SET a.sparse = 50, a.tag = null",
            Box::new(|model| {
                let node = model.node_with_ident(11).as_mut().unwrap();
                node.sparse = Some(50);
                node.tag = None;
            }),
        ),
        // A second node moves into the range the deletions emptied.
        (
            "MATCH (a:Entity {ident: 12000}) SET a.ident = 4005",
            Box::new(|model| {
                model.node_with_ident(12000).as_mut().unwrap().ident = Some(4005);
            }),
        ),
    ];
    for (statement, apply) in &statements {
        forge.execute(statement).expect(statement);
        apply(&mut model);
        assert_answers(&forge, &model, statement, false);
    }
    assert_answers(&forge, &model, "final state", true);
    drop(forge);
    for partitions in [1, 4] {
        let forge = open(&built.project, partitions);
        assert_answers(
            &forge,
            &model,
            &format!("reopen at {partitions} partitions"),
            true,
        );
    }
}
