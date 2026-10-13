//! A per-relationship `ALL` predicate narrows a reachability-mode search to the
//! relationships that pass it, and reads their properties from the persisted
//! edge property index when one describes the route (#1984).
//!
//! The graphs are constructed in bulk, so the generation publishes the index.
//! Every answer is checked against a breadth-first search written here over
//! the edge list, and the bounded ones also against forced trail enumeration.
//! A write makes the index stale, which sends the same searches to the
//! property overlay; writing the values back leaves the data as it was, so
//! the answers of the two read paths must be identical.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExplainStage, GraphConstructionBudgets,
    GraphForge,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_storage::io_stats;
use graphforge_storage::io_stats::CaptureScope;

const NODES: usize = 11;

/// One relationship of the generated graph. `timestamp` is `NULL` for some.
#[derive(Clone, Copy, Debug)]
struct Edge {
    source: usize,
    target: usize,
    timestamp: Option<i64>,
}

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

/// A ring, chords that close short cycles, a self-loop, parallel
/// relationships, and timestamps that vary (and are sometimes `NULL`), so the
/// subgraph of passing relationships differs from the graph for every window.
fn graph() -> Vec<Edge> {
    let mut edges = Vec::new();
    let mut state = 0x5eed_u64;
    let mut stamp = |edges: &mut Vec<Edge>, source: usize, target: usize| {
        state = mix(state);
        let timestamp = (state % 9 != 0).then(|| (state >> 8) as i64 % 100);
        edges.push(Edge {
            source,
            target,
            timestamp,
        });
    };
    for node in 0..NODES {
        stamp(&mut edges, node, (node + 1) % NODES);
    }
    for node in (0..NODES).step_by(2) {
        stamp(&mut edges, node, (node * 3 + 2) % NODES);
    }
    for node in (1..NODES).step_by(3) {
        stamp(&mut edges, node, (node + 5) % NODES);
        stamp(&mut edges, node, (node + 1) % NODES);
    }
    stamp(&mut edges, 5, 5);
    stamp(&mut edges, 0, 1);
    edges
}

fn construct(dir: &Path, edges: &[Edge]) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("timestamp", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();

    let mut ids = FixedSizeBinaryBuilder::with_capacity(NODES, 16);
    let mut idents = Int64Builder::with_capacity(NODES);
    for node in 0..NODES {
        ids.append_value(node_uuid(node).as_bytes()).unwrap();
        idents.append_value(node as i64);
    }
    let batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; NODES])),
            Arc::new(idents.finish()),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &batch).unwrap();

    let rows = edges.len();
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut timestamps = Int64Builder::with_capacity(rows);
    for (index, edge) in edges.iter().enumerate() {
        ids.append_value(edge_uuid(index).as_bytes()).unwrap();
        sources
            .append_value(node_uuid(edge.source).as_bytes())
            .unwrap();
        targets
            .append_value(node_uuid(edge.target).as_bytes())
            .unwrap();
        timestamps.append_option(edge.timestamp);
    }
    let batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; rows])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(timestamps.finish()),
        ],
    )
    .unwrap();
    session.append_edges("edges", &batch).unwrap();
    session.seal_and_publish().unwrap();
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    edges: Vec<Edge>,
}

fn build() -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    let edges = graph();
    construct(&project, &edges);
    Built {
        _root: root,
        project,
        edges,
    }
}

fn open(project: &Path) -> GraphForge {
    GraphForge::new(Some(project.to_str().expect("utf-8 path"))).unwrap()
}

fn index_files(project: &Path) -> Vec<graphforge_storage::GraphFileEntry> {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .into_iter()
        .filter(|file| file.relative_path.starts_with("indexes/edge-properties/"))
        .collect()
}

/// Which way a pattern's relationship points from its first node.
#[derive(Clone, Copy, Debug)]
enum Direction {
    Out,
    In,
}

impl Direction {
    fn pattern(self, hops: &str) -> String {
        match self {
            Self::Out => format!("-[r:LINK*{hops}]->"),
            Self::In => format!("<-[r:LINK*{hops}]-"),
        }
    }
}

/// The search: a pinned source and destination, a window on the timestamp,
/// and the hops. `observe` adds an aggregate that counts trails, which forces
/// trail enumeration while the answer under test stays the same column.
struct Search {
    direction: Direction,
    hops: &'static str,
    observe: bool,
}

impl Search {
    fn query(&self) -> String {
        format!(
            "MATCH (s:Entity {{ident: $s}}){}(d:Entity {{ident: $d}}) \
             WHERE ALL(e IN r WHERE $lo < e.timestamp AND e.timestamp < $hi) \
             RETURN coalesce(min(size(r)), -1) AS n{}",
            self.direction.pattern(self.hops),
            if self.observe {
                ", count(*) AS trails"
            } else {
                ""
            }
        )
    }

    /// The same search with its parameters written into the text, which
    /// `EXPLAIN` needs.
    fn literal_query(&self, s: usize, d: usize, lo: i64, hi: i64) -> String {
        self.query()
            .replace("$s", &s.to_string())
            .replace("$d", &d.to_string())
            .replace("$lo", &lo.to_string())
            .replace("$hi", &hi.to_string())
    }

    fn range(&self) -> (usize, Option<usize>) {
        let (min, max) = self.hops.split_once("..").expect("a range");
        (min.parse().unwrap(), max.parse().ok())
    }

    /// The shortest number of crossed relationships from `s` to `d` over the
    /// relationships whose timestamp lies strictly inside `(lo, hi)`, or -1.
    /// A breadth-first search over the edge list, independent of the engine.
    /// With at most one relationship required the shortest trail is a shortest
    /// path; from `s` back to `s` it is the shortest cycle.
    fn expected(&self, edges: &[Edge], s: usize, d: usize, lo: i64, hi: i64) -> i64 {
        let (min, max) = self.range();
        let mut adjacent = vec![Vec::new(); NODES];
        for edge in edges {
            if !edge.timestamp.is_some_and(|t| lo < t && t < hi) {
                continue;
            }
            match self.direction {
                Direction::Out => adjacent[edge.source].push(edge.target),
                Direction::In => adjacent[edge.target].push(edge.source),
            }
        }
        let mut depth: Vec<Option<usize>> = vec![None; NODES];
        if min == 0 {
            depth[s] = Some(0);
        }
        let mut queue = std::collections::VecDeque::from([(s, 0_usize)]);
        while let Some((node, at)) = queue.pop_front() {
            if max.is_some_and(|max| at >= max) {
                continue;
            }
            for &neighbor in &adjacent[node] {
                if depth[neighbor].is_none() {
                    depth[neighbor] = Some(at + 1);
                    queue.push_back((neighbor, at + 1));
                }
            }
        }
        depth[d].map_or(-1, |depth| depth as i64)
    }
}

fn answer(forge: &GraphForge, search: &Search, s: usize, d: usize, lo: i64, hi: i64) -> i64 {
    let params = HashMap::from([
        ("s".to_owned(), IrLiteral::Int(s as i64)),
        ("d".to_owned(), IrLiteral::Int(d as i64)),
        ("lo".to_owned(), IrLiteral::Int(lo)),
        ("hi".to_owned(), IrLiteral::Int(hi)),
    ]);
    let batches = forge
        .execute_with_params(&search.query(), &params)
        .unwrap_or_else(|e| panic!("{}: {e}", search.query()))
        .batches;
    assert_eq!(batches.len(), 1, "{}", search.query());
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

const WINDOWS: [(i64, i64); 3] = [(-1, 100), (20, 80), (-1, 35)];

/// The pairs of nodes and windows a pass asks about: every one for `every` of
/// 1, otherwise every `every`-th in a fixed order that starts with node 0.
fn asked(every: usize) -> impl Iterator<Item = (usize, usize, i64, i64)> {
    WINDOWS
        .into_iter()
        .flat_map(|(lo, hi)| (0..NODES).flat_map(move |s| (0..NODES).map(move |d| (s, d, lo, hi))))
        .enumerate()
        .filter_map(move |(index, ask)| (index % every == 0).then_some(ask))
}

/// The engine's answers, in the order of [`asked`].
fn answers(forge: &GraphForge, search: &Search, every: usize) -> Vec<i64> {
    asked(every)
        .map(|(s, d, lo, hi)| answer(forge, search, s, d, lo, hi))
        .collect()
}

/// The breadth-first oracle's answers, in the order of [`asked`].
fn expected(edges: &[Edge], search: &Search, every: usize) -> Vec<i64> {
    asked(every)
        .map(|(s, d, lo, hi)| search.expected(edges, s, d, lo, hi))
        .collect()
}

fn plan_of(forge: &GraphForge, search: &Search) -> String {
    forge
        .explain_stage(
            &search.literal_query(0, 5, 20, 80),
            ExplainStage::PhysicalPlan,
        )
        .expect("plan")
}

const SEARCHES: [(Direction, &str); 6] = [
    (Direction::Out, "1.."),
    (Direction::Out, "0.."),
    (Direction::In, "1.."),
    (Direction::Out, "1..5"),
    (Direction::Out, "0..4"),
    (Direction::In, "1..3"),
];

fn searches(observe: bool) -> impl Iterator<Item = Search> {
    SEARCHES.into_iter().map(move |(direction, hops)| Search {
        direction,
        hops,
        observe,
    })
}

/// A write moves the timestamps of the relationships that leave node 0 out of
/// every window (and back, with the opposite sign), so it changes the answers.
const MOVE: i64 = 1000;

/// The edge list after `SET e.timestamp = e.timestamp + delta` on every
/// relationship that leaves node 0.
fn shifted(edges: &[Edge], delta: i64) -> Vec<Edge> {
    edges
        .iter()
        .map(|edge| Edge {
            timestamp: (edge.source == 0)
                .then_some(edge.timestamp.map(|t| t + delta))
                .unwrap_or(edge.timestamp),
            ..*edge
        })
        .collect()
}

fn shift(forge: &GraphForge, delta: i64) {
    forge
        .execute(&format!(
            "MATCH (a:Entity {{ident: 0}})-[e:LINK]->(b) SET e.timestamp = e.timestamp + {delta}"
        ))
        .unwrap();
}

#[test]
fn the_search_is_planned_as_reachability_with_the_predicate_pushed() {
    let built = build();
    let forge = open(&built.project);
    for search in searches(false) {
        let plan = plan_of(&forge, &search);
        assert!(plan.contains("mode=reachability"), "{plan}");
        assert!(plan.contains("edge_filter="), "{plan}");
    }
    // Counting trails observes them, so the same predicate stays on trails.
    for search in searches(true) {
        let plan = plan_of(&forge, &search);
        assert!(!plan.contains("mode=reachability"), "{plan}");
        assert!(!plan.contains("edge_filter="), "{plan}");
    }
}

/// Reachability over the subgraph of passing relationships answers as a
/// breadth-first search of that subgraph does, as trail enumeration does, and
/// the same whether the relationships' timestamps come from the index, from
/// the property overlay after a write has made the index stale, or from a
/// rebuilt index.
#[test]
fn the_answers_do_not_depend_on_where_the_timestamps_are_read_from() {
    let built = build();
    assert!(
        index_files(&built.project)
            .iter()
            .any(|file| file.relative_path.ends_with(".epx")),
        "the constructed generation declares the index"
    );
    let forge = open(&built.project);

    // The first pass asks about every third pair and checks it against the
    // oracle; later passes ask about every ninth, which keeps the run short.
    // Nine is chosen so that the write below changes the answer of every
    // search among the pairs asked (asserted).
    const FIRST: usize = 3;
    const SAMPLE: usize = 9;

    let mut baseline = Vec::new();
    for search in searches(false) {
        let indexed = answers(&forge, &search, FIRST);
        assert_eq!(
            indexed,
            expected(&built.edges, &search, FIRST),
            "{:?} {} against a breadth-first search",
            search.direction,
            search.hops
        );
        baseline.push(
            indexed
                .into_iter()
                .step_by(SAMPLE / FIRST)
                .collect::<Vec<_>>(),
        );
    }
    // The answers are not all alike: some windows reach, some do not.
    let flat: Vec<i64> = baseline.iter().flatten().copied().collect();
    assert!(
        flat.contains(&-1) && flat.iter().any(|n| *n > 1),
        "{flat:?}"
    );

    // Trail enumeration of the same bounded searches.
    for (search, reachable) in searches(true).zip(&baseline) {
        if search.range().1.is_some() {
            assert_eq!(
                &answers(&forge, &search, SAMPLE),
                reachable,
                "{}",
                search.hops
            );
        }
    }

    // A write makes the index stale, so the overlay answers. Moving the
    // timestamps out of every window changes the answers; moving them back
    // restores the data.
    shift(&forge, MOVE);
    let moved = shifted(&built.edges, MOVE);
    for (search, before) in searches(false).zip(&baseline) {
        let after = expected(&moved, &search, SAMPLE);
        assert_ne!(
            &after, before,
            "the write changes some {} answer",
            search.hops
        );
        assert_eq!(
            answers(&forge, &search, SAMPLE),
            after,
            "{:?} {}",
            search.direction,
            search.hops
        );
    }
    shift(&forge, -MOVE);
    for (search, indexed) in searches(false).zip(&baseline) {
        assert_eq!(
            &answers(&forge, &search, SAMPLE),
            indexed,
            "{}",
            search.hops
        );
    }

    // A rebuilt index describes the data again.
    let report = forge.rebuild_property_indexes(None).unwrap();
    assert!(report.edge_properties.indexes >= 1, "{report:?}");
    for (search, indexed) in searches(false).zip(&baseline) {
        assert_eq!(
            &answers(&forge, &search, SAMPLE),
            indexed,
            "{}",
            search.hops
        );
    }
    drop(forge);

    // And a fresh session finds the rebuilt index.
    let forge = open(&built.project);
    for (search, indexed) in searches(false).zip(&baseline) {
        assert_eq!(
            &answers(&forge, &search, SAMPLE),
            indexed,
            "{}",
            search.hops
        );
    }
}

/// A search with a predicate reads the relationships' timestamps through the
/// index. A manifest that fails its checksum is refused by the lookup that
/// reads it, so the refusal shows the lookup was made; a search that reads no
/// property is unaffected.
#[test]
fn the_predicate_reads_timestamps_from_the_index() {
    let built = build();
    let entry = index_files(&built.project)
        .into_iter()
        .find(|file| file.relative_path.ends_with(".epx"))
        .expect("the generation declares a manifest");
    let generation = graphforge_storage::resolve_project_generation(&built.project).unwrap();
    let object =
        graphforge_storage::graph_object_path(generation.container_root(), &entry.content_sha256)
            .unwrap();
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

    let forge = open(&built.project);
    let search = Search {
        direction: Direction::Out,
        hops: "1..",
        observe: false,
    };
    let params = HashMap::from([
        ("s".to_owned(), IrLiteral::Int(0)),
        ("d".to_owned(), IrLiteral::Int(5)),
        ("lo".to_owned(), IrLiteral::Int(20)),
        ("hi".to_owned(), IrLiteral::Int(80)),
    ]);
    let error = forge
        .execute_with_params(&search.query(), &params)
        .expect_err("a corrupted index is refused, not bypassed");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");

    // A search that reads no relationship property never opens the index.
    let batches = forge
        .execute(
            "MATCH (s:Entity {ident: 0})-[r:LINK*1..]->(d:Entity {ident: 5}) \
             RETURN coalesce(min(size(r)), -1) AS n",
        )
        .expect("no property is read")
        .batches;
    let n = batches[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let stamped: Vec<Edge> = built
        .edges
        .iter()
        .map(|edge| Edge {
            timestamp: Some(0),
            ..*edge
        })
        .collect();
    assert_eq!(n, search.expected(&stamped, 0, 5, -1, 1));
}

/// A traversal already has edge ids, so an indexed per-edge filter reads its
/// demanded properties without resolving those edges' UUIDs. The oracle also
/// covers NULL property values, which the `ALL` predicate excludes.
#[test]
fn a_per_edge_filter_reads_indexed_properties_by_edge_id() {
    let built = build();
    let forge = open(&built.project);
    let search = Search {
        direction: Direction::Out,
        hops: "1..4",
        observe: false,
    };
    let expected = search.expected(&built.edges, 0, 7, 20, 80);
    let query = search.query().replace("ident: $s", "node_uuid: $s");
    let params = HashMap::from([
        ("s".to_owned(), IrLiteral::Uuid(*node_uuid(0).as_bytes())),
        ("d".to_owned(), IrLiteral::Int(7)),
        ("lo".to_owned(), IrLiteral::Int(20)),
        ("hi".to_owned(), IrLiteral::Int(80)),
    ]);

    // Warm inventory and adjacency admission before attributing the query.
    assert_eq!(
        answer(&forge, &search, 0, 7, 20, 80),
        expected,
        "indexed lookup must preserve reachability results"
    );
    let _capture = CaptureScope::install();
    io_stats::reset();
    let result = forge
        .execute_with_params(&query, &params)
        .expect("the filtered traversal executes");
    assert_eq!(result.batches.len(), 1);
    assert_eq!(
        result.batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected,
        "NULL and absent property values still exclude an edge"
    );

    let io = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(io.edge_full_reads, 0, "no edge UUIDs were read");
    assert_eq!(io.edge_filtered_reads, 0, "no edge UUIDs were read");
    assert_eq!(
        io.property_uuid_selections, 0,
        "no UUID-keyed property lookup"
    );
    assert_eq!(
        io.property_uuid_rows_decoded, 0,
        "no UUID rows were decoded"
    );
    assert_eq!(
        io.property_uuid_pages_selected, 0,
        "no UUID pages were read"
    );
}
