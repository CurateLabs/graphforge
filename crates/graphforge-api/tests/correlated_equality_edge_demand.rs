//! A MATCH path whose unbound node is pinned by an equality with a value the
//! query already has reads no edge file for a relationship nobody reads,
//! although the planner starts the path at the pinned node and looks the bound
//! node up along the relationship (#1984, the third MATCH of LDBC SNB
//! Interactive IC7).
//!
//! The expansion from the message only needs the creator's identity, and the
//! comparison with the bound person is made on identities, so the relationship
//! is neither projected nor decoded. The gate is deterministic and counts the
//! edge reads of the expansion operators and of the storage layer, never time.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::io_stats;

/// The persons are `0..PERSONS`, the posts `100..100 + POSTS` and the comments
/// `200..200 + COMMENTS`; no two nodes share an `ident`.
const PERSONS: usize = 4;
const POSTS: usize = 12;
const COMMENTS: usize = 6;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// The graph as plain data: nodes as `(label, ident)` and relationships as
/// `(type, source node index, target node index)`.
struct Fixture {
    nodes: Vec<(&'static str, i64)>,
    edges: Vec<(&'static str, usize, usize)>,
}

fn post(index: usize) -> usize {
    PERSONS + index
}

fn comment(index: usize) -> usize {
    PERSONS + POSTS + index
}

fn fixture() -> Fixture {
    let mut nodes = Vec::new();
    nodes.extend((0..PERSONS).map(|ident| ("Person", i64::try_from(ident).expect("small"))));
    nodes.extend((0..POSTS).map(|ident| ("Post", 100 + i64::try_from(ident).expect("small"))));
    nodes
        .extend((0..COMMENTS).map(|ident| ("Comment", 200 + i64::try_from(ident).expect("small"))));

    // Message `n` is created by person `n % PERSONS`, so the start person 0
    // created posts 100, 104 and 108 and comment 200 and 204. Post 104 is
    // created twice, by two parallel relationships, and the message 101 by
    // somebody else.
    let mut edges = Vec::new();
    edges.extend((0..POSTS).map(|n| ("HAS_CREATOR", post(n), n % PERSONS)));
    edges.extend((0..COMMENTS).map(|n| ("HAS_CREATOR", comment(n), n % PERSONS)));
    edges.push(("HAS_CREATOR", post(4), 0));
    // A relationship of another type between the same endpoints and a self
    // loop on the start person, which no message may be mistaken for.
    edges.push(("KNOWS", 0, 1));
    edges.push(("HAS_CREATOR", 0, 0));
    Fixture { nodes, edges }
}

impl Fixture {
    /// The `(latest, message)` rows of the pattern for the ids in `latest`, in
    /// order, one per HAS_CREATOR relationship from the message to person 0.
    fn rows(&self, latest: &[i64]) -> Vec<(i64, i64)> {
        let mut rows = Vec::new();
        for &id in latest {
            for &(rel, source, target) in &self.edges {
                let (label, ident) = self.nodes[source];
                if rel == "HAS_CREATOR" && target == 0 && ident == id && label != "Person" {
                    rows.push((id, ident));
                }
            }
        }
        rows.sort_unstable();
        rows
    }
}

fn construct(dir: &Path, fixture: &Fixture) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let edge_schema = Arc::new(Schema::new(CONSTRUCTION_EDGE_SCHEMA.fields().to_vec()));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();

    let mut ids = FixedSizeBinaryBuilder::with_capacity(fixture.nodes.len(), 16);
    let mut idents = Int64Builder::with_capacity(fixture.nodes.len());
    for (index, &(_, ident)) in fixture.nodes.iter().enumerate() {
        ids.append_value(node_uuid(index).as_bytes()).unwrap();
        idents.append_value(ident);
    }
    let labels: Vec<&str> = fixture.nodes.iter().map(|&(label, _)| label).collect();
    let batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(labels)),
            Arc::new(idents.finish()),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &batch).unwrap();

    let rows = fixture.edges.len();
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for (index, &(_, source, target)) in fixture.edges.iter().enumerate() {
        ids.append_value(edge_uuid(index).as_bytes()).unwrap();
        sources.append_value(node_uuid(source).as_bytes()).unwrap();
        targets.append_value(node_uuid(target).as_bytes()).unwrap();
    }
    let types: Vec<&str> = fixture.edges.iter().map(|&(rel, _, _)| rel).collect();
    let batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ],
    )
    .unwrap();
    session.append_edges("edges", &batch).unwrap();
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

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug)]
struct Measured {
    batches: Vec<RecordBatch>,
    io: io_stats::IoSnapshot,
    /// What the expansion operators themselves report of their edge reads,
    /// summed over the hops: filtered reads started, rows returned, rows
    /// scanned, and edge columns materialised.
    hop_edge_work: [u64; 4],
}

impl Measured {
    /// The edge-file reads of every path: full, filtered, and the rows each
    /// decoded or scanned.
    fn edge_work(&self) -> [u64; 4] {
        [
            self.io.edge_full_reads,
            self.io.edge_filtered_reads,
            self.io.edge_filtered_rows,
            self.io.edge_scanned_rows,
        ]
    }

    fn int_column(&self, name: &str) -> Vec<i64> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"));
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{name} is Int64"));
            values.extend((0..column.len()).map(|row| {
                assert!(column.is_valid(row), "{name} is never NULL here");
                column.value(row)
            }));
        }
        values
    }
}

/// Runs the statement and reports its I/O.
///
/// The storage counters (`io`) are attributed through a thread-local capture
/// that DataFusion's partition workers do not carry, so they are exact only at
/// one target partition, where the statement runs on the calling thread.
/// `hop_edge_work` comes from the expansion operators' own process-wide
/// counters and is exact at every partition count.
fn measured(forge: &GraphForge, query: &str) -> Measured {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let (result, demand) = graphforge_exec::demand::capture(|| forge.execute(query));
    let result = result.expect("statement executes");
    let hop_edge_work = demand.hops.values().fold([0_u64; 4], |sum, hop| {
        [
            sum[0] + hop.edge_reads_started,
            sum[1] + hop.edge_rows_returned,
            sum[2] + hop.edge_rows_scanned,
            sum[3] + hop.edge_projected_columns,
        ]
    });
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    assert!(delta.phases.contains_key(&StorageIoPhase::ReadPathScan));
    Measured {
        batches: result.batches,
        io: io_stats::snapshot().expect("requested I/O statistics"),
        hop_edge_work,
    }
}

/// The statement after one warm-up run, which admits the footers, the
/// adjacency index and the node topology once per session.
fn warm_measured(forge: &GraphForge, query: &str) -> Measured {
    let _ = measured(forge, query);
    measured(forge, query)
}

const LATEST: [i64; 7] = [100, 101, 104, 200, 201, 204, 999];

/// IC7's third MATCH over the ids in `LATEST`. `relationship` is the
/// relationship pattern, anonymous or named but never read.
fn query(relationship: &str) -> String {
    format!(
        "MATCH (person:Person {{ident: 0}}) UNWIND {LATEST:?} AS latest
         MATCH (person)<-{relationship}-(message)
         WHERE (message:Post OR message:Comment) AND message.ident = latest
         RETURN latest, message.ident AS message ORDER BY latest, message"
    )
}

#[test]
fn a_pinned_node_reads_no_edge_file_for_an_unused_relationship() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    let expected = fixture.rows(&LATEST);
    assert!(
        expected.len() > 4 && expected.windows(2).any(|pair| pair[0] == pair[1]),
        "the fixture has several messages and a parallel relationship: {expected:?}"
    );
    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        for relationship in ["[:HAS_CREATOR]", "[r:HAS_CREATOR]"] {
            let query = query(relationship);
            let plan = forge.explain(&query).unwrap();
            assert!(plan.contains("dir=->"), "{relationship}: {plan}");

            let run = warm_measured(&forge, &query);
            let rows: Vec<(i64, i64)> = run
                .int_column("latest")
                .into_iter()
                .zip(run.int_column("message"))
                .collect();
            assert_eq!(rows, expected, "{relationship}, {partitions} partitions");
            assert_eq!(
                run.hop_edge_work, [0; 4],
                "{relationship}, {partitions} partitions"
            );
            if partitions == 1 {
                assert_eq!(run.edge_work(), [0; 4], "{relationship}: {:?}", run.io);
            }
        }
    }
}

/// The counters are live: collecting the relationship whole decodes the edge
/// rows of the expansion, so the zeros above are a measurement and not an
/// absence.
#[test]
fn a_returned_relationship_reads_the_edge_file() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    let expected = i64::try_from(fixture.rows(&LATEST).len()).expect("small");
    let query = format!(
        "MATCH (person:Person {{ident: 0}}) UNWIND {LATEST:?} AS latest
         MATCH (person)<-[r:HAS_CREATOR]-(message)
         WHERE (message:Post OR message:Comment) AND message.ident = latest
         RETURN size(collect(r)) AS n"
    );
    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        let plan = forge.explain(&query).unwrap();
        assert!(plan.contains("dir=->"), "{plan}");
        let run = warm_measured(&forge, &query);
        assert_eq!(run.int_column("n"), [expected], "{partitions} partitions");
        assert!(
            run.hop_edge_work[0] > 0 && run.hop_edge_work[1] > 0,
            "a returned edge variable reads edge rows: {:?}",
            run.hop_edge_work
        );
        if partitions == 1 {
            assert!(run.io.edge_filtered_rows > 0, "{:?}", run.io);
        }
    }
}
