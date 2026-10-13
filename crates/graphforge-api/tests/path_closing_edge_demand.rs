//! A path that closes on a bound node reads no edge file for the hops whose
//! edge variable is never demanded, although the planner splits it at a meeting
//! node and checks relationship uniqueness across both halves (#1984).
//!
//! The fixture is the LDBC SNB Interactive IC10 shape: the friends' posts that
//! carry a tag the start person is interested in. The anonymous relationships
//! of `(friend)<-[:HAS_CREATOR]-(post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)`
//! take part in the uniqueness check, which must compare their ordinals and
//! must not ask for their edge columns. The gate is deterministic and counts
//! the edge reads of the expansion operators and of the storage layer, never
//! time.
//!
//! The demand pass walks through the OPTIONAL MATCH and its hash joins, so the
//! gate holds for both clauses. A closing relationship that is named and read
//! only for a property is served by the edge property index by edge id and
//! reads no edge file either; only a statement that returns the relationship
//! itself decodes its edge rows.

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

/// The start person; persons `1..=FRIENDS` are the friends whose posts count.
const START: i64 = 0;
const FRIENDS: i64 = 4;
const PERSONS: i64 = 8;
const TAGS: i64 = 4;
const POSTS: i64 = 16;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// The graph as plain data: nodes as `(label, ident)` and relationships as
/// `(type, source node index, target node index)`. Node indexes run over the
/// persons, then the tags, then the posts.
struct Fixture {
    nodes: Vec<(&'static str, i64)>,
    edges: Vec<(&'static str, usize, usize)>,
    /// `(friend, post)` for every path the IC10 pattern matches, with the
    /// multiplicity the graph defines, found by walking the lists directly.
    paths: Vec<(i64, i64)>,
}

fn person(ident: i64) -> usize {
    ident as usize
}

fn tag(ident: i64) -> usize {
    (PERSONS + ident) as usize
}

fn post(ident: i64) -> usize {
    (PERSONS + TAGS + ident) as usize
}

fn fixture() -> Fixture {
    let mut nodes = Vec::new();
    nodes.extend((0..PERSONS).map(|ident| ("Person", ident)));
    nodes.extend((0..TAGS).map(|ident| ("Tag", ident)));
    nodes.extend((0..POSTS).map(|ident| ("Post", ident)));

    // `(creator, tags)` of every post. Post 0 carries tag 0 twice and post 1
    // carries tags 0 and 1, so one post reaches the start person's interests
    // through several relationships. The posts of the last friend carry tag 2
    // only, which the start person lacks and person 5 has: that friend matches
    // no path and gets the NULL row, while the closing hop still meets another
    // interested person.
    let mut creators = Vec::new();
    let mut tagged = Vec::new();
    for ident in 0..POSTS {
        let creator = 1 + ident % (FRIENDS + 1);
        creators.push((ident, creator));
        if creator == FRIENDS {
            tagged.push((ident, 2));
            continue;
        }
        tagged.push((ident, ident % TAGS));
        if ident % 2 == 1 {
            tagged.push((ident, (ident + 1) % TAGS));
        }
    }
    tagged.push((0, 0));
    // The start person is interested in tags 0 and 1, friend 2 in tag 0 only,
    // and person 5 in every tag. Tag 1 is listed twice for the start person.
    let interests = [
        (START, 0),
        (START, 1),
        (START, 1),
        (2, 0),
        (3, 3),
        (5, 0),
        (5, 1),
        (5, 2),
        (5, 3),
    ];

    let mut edges = Vec::new();
    edges.extend(
        creators
            .iter()
            .map(|&(p, creator)| ("HAS_CREATOR", post(p), person(creator))),
    );
    edges.extend(tagged.iter().map(|&(p, t)| ("HAS_TAG", post(p), tag(t))));
    edges.extend(
        interests
            .iter()
            .map(|&(who, t)| ("HAS_INTEREST", person(who), tag(t))),
    );

    let mut paths = Vec::new();
    for friend in 1..=FRIENDS {
        for &(p, creator) in &creators {
            if creator != friend {
                continue;
            }
            for &(tagged_post, t) in &tagged {
                if tagged_post != p {
                    continue;
                }
                for &(who, interest_tag) in &interests {
                    if who == START && interest_tag == t {
                        paths.push((friend, p));
                    }
                }
            }
        }
    }
    Fixture {
        nodes,
        edges,
        paths,
    }
}

impl Fixture {
    /// The `(friend, post)` rows of the OPTIONAL MATCH: every path, and one
    /// NULL post for a friend with none.
    fn optional_rows(&self) -> Vec<(i64, Option<i64>)> {
        let mut rows: Vec<(i64, Option<i64>)> = self
            .paths
            .iter()
            .map(|&(friend, post)| (friend, Some(post)))
            .collect();
        for friend in 1..=FRIENDS {
            if !self.paths.iter().any(|&(f, _)| f == friend) {
                rows.push((friend, None));
            }
        }
        rows.sort();
        rows
    }

    /// `count(DISTINCT post)` per friend.
    fn distinct_posts(&self) -> Vec<(i64, i64)> {
        (1..=FRIENDS)
            .map(|friend| {
                let mut posts: Vec<i64> = self
                    .paths
                    .iter()
                    .filter(|&&(f, _)| f == friend)
                    .map(|&(_, p)| p)
                    .collect();
                posts.sort_unstable();
                posts.dedup();
                (friend, posts.len() as i64)
            })
            .collect()
    }
}

fn construct(dir: &Path, fixture: &Fixture) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
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
    let mut weights = Int64Builder::with_capacity(rows);
    for (index, &(_, source, target)) in fixture.edges.iter().enumerate() {
        ids.append_value(edge_uuid(index).as_bytes()).unwrap();
        sources.append_value(node_uuid(source).as_bytes()).unwrap();
        targets.append_value(node_uuid(target).as_bytes()).unwrap();
        weights.append_value(index as i64);
    }
    let types: Vec<&str> = fixture.edges.iter().map(|&(rel, _, _)| rel).collect();
    let batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(weights.finish()),
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
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    fn int_column(&self, name: &str) -> Vec<Option<i64>> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"));
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{name} is Int64"));
            values.extend(
                (0..column.len()).map(|row| column.is_valid(row).then(|| column.value(row))),
            );
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

const BOUND_NODES: &str = "MATCH (person:Person {ident: 0})
     MATCH (friend:Person) WHERE friend.ident IN [1, 2, 3, 4]";

/// The IC10 path: `friend` and `person` are bound before it and it names no
/// relationship.
const CLOSING_PATH: &str =
    "(friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)";

/// Rows `(friend, post)` of the IC10 path under `clause` (`MATCH` or
/// `OPTIONAL MATCH`), in the order they are returned.
fn paths_query(clause: &str) -> String {
    format!(
        "{BOUND_NODES} {clause} {CLOSING_PATH} RETURN friend.ident AS f, post.ident AS p ORDER BY f, p"
    )
}

/// The IC10 aggregate over the same path.
fn counts_query(clause: &str) -> String {
    format!(
        "{BOUND_NODES} {clause} {CLOSING_PATH}
         WITH friend, count(DISTINCT post.ident) AS common
         RETURN friend.ident AS f, common ORDER BY f"
    )
}

/// The OPTIONAL MATCH as the planner lowered it before it split a path that
/// closes on a bound node: the last node is a fresh variable and the closing
/// is a predicate on it, so the path expands from the tags to every person
/// interested in them and then keeps the one bound person.
fn unsplit_paths_query() -> String {
    format!(
        "{BOUND_NODES}
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)\
         <-[:HAS_INTEREST]-(closing) WHERE closing = person
         RETURN friend.ident AS f, post.ident AS p ORDER BY f, p"
    )
}

/// The same path with the closing relationship named and a property of it read.
fn property_query() -> String {
    format!(
        "{BOUND_NODES} MATCH {} RETURN count(r.weight) AS n",
        CLOSING_PATH.replace("[:HAS_INTEREST]", "[r:HAS_INTEREST]")
    )
}

/// The same path with the closing relationship named and collected whole, which
/// needs the stored edge identity.
fn identity_query() -> String {
    format!(
        "{BOUND_NODES} MATCH {} RETURN size(collect(r)) AS n",
        CLOSING_PATH.replace("[:HAS_INTEREST]", "[r:HAS_INTEREST]")
    )
}

fn rows(run: &Measured) -> Vec<(i64, Option<i64>)> {
    run.int_column("f")
        .iter()
        .zip(run.int_column("p"))
        .map(|(friend, post)| (friend.expect("a friend"), post))
        .collect()
}

fn counts(run: &Measured) -> Vec<(i64, i64)> {
    run.int_column("f")
        .iter()
        .zip(run.int_column("common"))
        .map(|(friend, common)| (friend.expect("a friend"), common.expect("a count")))
        .collect()
}

#[test]
fn a_path_closing_on_a_bound_node_reads_no_edge_file_for_unused_relationships() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    let mut expected_paths: Vec<(i64, Option<i64>)> = fixture
        .paths
        .iter()
        .map(|&(friend, post)| (friend, Some(post)))
        .collect();
    expected_paths.sort();
    let expected_counts: Vec<(i64, i64)> = fixture
        .distinct_posts()
        .into_iter()
        .filter(|&(_, common)| common > 0)
        .collect();
    assert!(
        expected_counts.len() < FRIENDS as usize && expected_paths.len() > expected_counts.len(),
        "the fixture has a friend without a path and friends with several: {expected_paths:?}"
    );
    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        let plan = forge.explain(&paths_query("MATCH")).unwrap();
        assert!(plan.contains("Expand"), "{plan}");

        let run = warm_measured(&forge, &paths_query("MATCH"));
        assert_eq!(rows(&run), expected_paths, "{partitions} partitions");
        assert_eq!(run.hop_edge_work, [0; 4], "{partitions} partitions");
        if partitions == 1 {
            assert_eq!(run.edge_work(), [0; 4], "{:?}", run.io);
        }

        let run = warm_measured(&forge, &counts_query("MATCH"));
        assert_eq!(counts(&run), expected_counts, "{partitions} partitions");
        assert_eq!(run.hop_edge_work, [0; 4], "{partitions} partitions");
        if partitions == 1 {
            assert_eq!(run.edge_work(), [0; 4], "{:?}", run.io);
        }
    }
}

/// The OPTIONAL MATCH of IC10 keeps the rows of the lowering it replaces and,
/// like the MATCH, reads no edge file for its unused relationships: the demand
/// pass walks through the OPTIONAL MATCH and its hash joins.
#[test]
fn an_optional_closing_path_reads_no_edge_file_for_unused_relationships() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    let expected = fixture.optional_rows();
    assert!(expected.iter().any(|&(_, post)| post.is_none()));
    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        let split = warm_measured(&forge, &paths_query("OPTIONAL MATCH"));
        let unsplit = warm_measured(&forge, &unsplit_paths_query());
        assert_eq!(rows(&split), expected, "{partitions} partitions");
        assert_eq!(rows(&unsplit), expected, "{partitions} partitions");
        assert_eq!(split.hop_edge_work, [0; 4], "{partitions} partitions");
        if partitions == 1 {
            assert_eq!(split.edge_work(), [0; 4], "{:?}", split.io);
        }

        let grouped = warm_measured(&forge, &counts_query("OPTIONAL MATCH"));
        let mut expected_counts = fixture.distinct_posts();
        expected_counts.sort();
        assert_eq!(counts(&grouped), expected_counts, "{partitions} partitions");
        assert_eq!(grouped.hop_edge_work, [0; 4], "{partitions} partitions");
        if partitions == 1 {
            assert_eq!(grouped.edge_work(), [0; 4], "{:?}", grouped.io);
        }
    }
}

/// A property of the closing relationship that is the only thing read of it
/// comes from the edge property index by edge id and reads no edge file.
#[test]
fn a_closing_relationship_read_only_for_a_property_reads_no_edge_file() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        let run = warm_measured(&forge, &property_query());
        assert_eq!(
            run.int_column("n"),
            [Some(fixture.paths.len() as i64)],
            "{partitions} partitions"
        );
        assert_eq!(run.hop_edge_work, [0; 4], "{partitions} partitions");
        if partitions == 1 {
            assert_eq!(run.edge_work(), [0; 4], "{:?}", run.io);
        }
    }
}

/// The counters are live: returning the closing relationship decodes the edge
/// rows of the frontier, so the zeros above are a measurement and not an
/// absence.
#[test]
fn a_returned_closing_relationship_reads_the_edge_file() {
    let _serial = serial();
    let fixture = fixture();
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, &fixture);

    for partitions in [1, 4] {
        let forge = open(&project, partitions);
        let run = warm_measured(&forge, &identity_query());
        assert_eq!(
            run.int_column("n"),
            [Some(fixture.paths.len() as i64)],
            "{partitions} partitions"
        );
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
