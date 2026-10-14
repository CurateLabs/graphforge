//! A node property that only the final projection reads is read after the sort
//! and the limit, for the rows the limit kept (#1984, the LDBC SNB Interactive
//! IC2 and IC9 shapes).
//!
//! Plan-shape tests read the physical EXPLAIN of a durable project, where a node
//! property route is a `PropertyOverlayExec`. Counter tests read the storage I/O
//! statistics, which count the node UUIDs a query asked the property store for.
//! Result tests run a query matrix against the durable project and an in-memory
//! graph built from the same data, and compare with the rows the data defines:
//! deferring a read must never change a row, a NULL, a count or an order.

use std::cmp::Reverse;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::io_stats;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// One fixture node. `None` is a NULL property.
struct Node {
    label: &'static str,
    id: i64,
    first_name: Option<String>,
    last_name: Option<String>,
    content: Option<String>,
    image_file: Option<String>,
    creation_date: Option<i64>,
    name: Option<String>,
}

impl Node {
    fn new(label: &'static str, id: i64) -> Self {
        Self {
            label,
            id,
            first_name: None,
            last_name: None,
            content: None,
            image_file: None,
            creation_date: None,
            name: None,
        }
    }
}

/// One fixture edge between node indexes. `work_from` is only on `WORK_AT`.
struct Edge {
    ty: &'static str,
    source: usize,
    target: usize,
    work_from: Option<i64>,
}

const PERSONS: [(i64, &str, &str); 4] = [
    (1, "ann", "aa"),
    (2, "bob", "bb"),
    (3, "cat", "cc"),
    (4, "dan", "dd"),
];
const FIRST_COMPANY: usize = 4;
const FIRST_COUNTRY: usize = 6;
const FIRST_MESSAGE: usize = 8;
const MESSAGES: usize = 30;

/// Message `i` is a Post when `i` is even and a Comment otherwise. A Post whose
/// index divides by four is an image post: no `content`, an `imageFile`.
/// Creation dates repeat in runs of three, so ordering by date alone ties, and
/// the author cycles through persons 2, 3 and 4.
fn message(i: usize) -> Node {
    let post = i.is_multiple_of(2);
    let image = i.is_multiple_of(4);
    let mut node = Node::new(if post { "Post" } else { "Comment" }, 1000 + i as i64);
    node.creation_date = Some(100 + (i / 3) as i64);
    if post && image {
        node.image_file = Some(format!("img{i}"));
    } else {
        node.content = Some(format!("m{i}"));
    }
    node
}

fn nodes() -> Vec<Node> {
    let mut nodes = Vec::new();
    for (id, first, last) in PERSONS {
        let mut person = Node::new("Person", id);
        person.first_name = Some(first.to_owned());
        person.last_name = Some(last.to_owned());
        nodes.push(person);
    }
    for (id, name) in [(20, "acme"), (21, "bolt")] {
        let mut company = Node::new("Company", id);
        company.name = Some(name.to_owned());
        nodes.push(company);
    }
    for (id, name) in [(30, "Laos"), (31, "Peru")] {
        let mut country = Node::new("Country", id);
        country.name = Some(name.to_owned());
        nodes.push(country);
    }
    nodes.extend((0..MESSAGES).map(message));
    nodes
}

fn edges() -> Vec<Edge> {
    let mut edges = Vec::new();
    for (source, target) in [(0, 1), (0, 2), (1, 3)] {
        edges.push(Edge {
            ty: "KNOWS",
            source,
            target,
            work_from: None,
        });
    }
    for (person, company, year) in [(1, 4, 2005), (2, 4, 2008), (3, 5, 2005), (1, 5, 2010)] {
        edges.push(Edge {
            ty: "WORK_AT",
            source: person,
            target: company,
            work_from: Some(year),
        });
    }
    // Both companies are in Laos; Peru has none.
    for company in [FIRST_COMPANY, FIRST_COMPANY + 1] {
        edges.push(Edge {
            ty: "IS_LOCATED_IN",
            source: company,
            target: FIRST_COUNTRY,
            work_from: None,
        });
    }
    for i in 0..MESSAGES {
        edges.push(Edge {
            ty: "HAS_CREATOR",
            source: FIRST_MESSAGE + i,
            target: 1 + i % 3,
            work_from: None,
        });
    }
    edges
}

fn quoted(text: &str) -> String {
    format!("'{text}'")
}

/// The same data as [`nodes`] and [`edges`], as one Cypher statement.
fn memory_script() -> String {
    let mut parts = Vec::new();
    for (index, node) in nodes().iter().enumerate() {
        let mut properties = vec![format!("id: {}", node.id)];
        for (key, value) in [
            ("firstName", &node.first_name),
            ("lastName", &node.last_name),
            ("content", &node.content),
            ("imageFile", &node.image_file),
            ("name", &node.name),
        ] {
            if let Some(value) = value {
                properties.push(format!("{key}: {}", quoted(value)));
            }
        }
        if let Some(date) = node.creation_date {
            properties.push(format!("creationDate: {date}"));
        }
        parts.push(format!(
            "(n{index}:{} {{{}}})",
            node.label,
            properties.join(", ")
        ));
    }
    for edge in edges() {
        let properties = edge
            .work_from
            .map(|year| format!(" {{workFrom: {year}}}"))
            .unwrap_or_default();
        parts.push(format!(
            "(n{})-[:{}{properties}]->(n{})",
            edge.source, edge.ty, edge.target
        ));
    }
    format!("CREATE {}", parts.join(", "))
}

fn uuid(tag: u128, index: usize) -> Uuid {
    Uuid::from_u128((tag << 120) | (index as u128 + 1))
}

/// A published durable project holding the fixture, reopened read-only.
struct Durable {
    graph: GraphForge,
    // Keeps the project directory alive for the test's duration.
    _dir: tempfile::TempDir,
}

fn durable_with(options: impl Fn() -> GraphForgeOptions) -> Durable {
    let dir = tempfile::tempdir().expect("tempdir");
    let project = dir.path().join("project");
    let path = project.to_str().expect("utf-8 path");
    let forge = GraphForge::new(Some(path)).expect("create project");
    let mut construction = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .expect("begin construction");

    let nodes = nodes();
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    for (name, ty) in [
        ("id", DataType::Int64),
        ("firstName", DataType::Utf8),
        ("lastName", DataType::Utf8),
        ("content", DataType::Utf8),
        ("imageFile", DataType::Utf8),
        ("creationDate", DataType::Int64),
        ("name", DataType::Utf8),
    ] {
        fields.push(Arc::new(Field::new(name, ty, true)));
    }
    let mut node_ids = FixedSizeBinaryBuilder::new(16);
    for index in 0..nodes.len() {
        node_ids
            .append_value(uuid(1, index).as_bytes())
            .expect("node uuid");
    }
    let text = |get: fn(&Node) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from_iter(nodes.iter().map(get)))
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(node_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(nodes.iter().map(|n| n.label))),
            Arc::new(Int64Array::from_iter(nodes.iter().map(|n| Some(n.id)))),
            text(|n| n.first_name.as_deref()),
            text(|n| n.last_name.as_deref()),
            text(|n| n.content.as_deref()),
            text(|n| n.image_file.as_deref()),
            Arc::new(Int64Array::from_iter(nodes.iter().map(|n| n.creation_date))),
            text(|n| n.name.as_deref()),
        ],
    )
    .expect("node batch");
    construction.append_nodes("nodes", &batch).expect("nodes");

    let edges = edges();
    let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("workFrom", DataType::Int64, true)));
    let mut edge_ids = FixedSizeBinaryBuilder::new(16);
    let mut sources = FixedSizeBinaryBuilder::new(16);
    let mut targets = FixedSizeBinaryBuilder::new(16);
    for (index, edge) in edges.iter().enumerate() {
        edge_ids
            .append_value(uuid(2, index).as_bytes())
            .expect("edge uuid");
        sources
            .append_value(uuid(1, edge.source).as_bytes())
            .expect("source uuid");
        targets
            .append_value(uuid(1, edge.target).as_bytes())
            .expect("target uuid");
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(edge_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(edges.iter().map(|e| e.ty))),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(Int64Array::from_iter(edges.iter().map(|e| e.work_from))),
        ],
    )
    .expect("edge batch");
    construction.append_edges("edges", &batch).expect("edges");
    construction.seal_and_publish().expect("publish");
    drop(forge);

    Durable {
        graph: GraphForge::new_with_options(Some(path), options()).expect("reopen project"),
        _dir: dir,
    }
}

fn durable() -> Durable {
    durable_with(GraphForgeOptions::default)
}

/// A durable project whose queries run on the calling thread, where the I/O
/// capture of a counter test is installed.
fn durable_single_partition() -> Durable {
    durable_with(|| GraphForgeOptions {
        resource: ExecutionResourcePolicy {
            target_partitions: Some(1),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn in_memory() -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory instance");
    graph.execute(&memory_script()).expect("load fixture");
    graph
}

fn try_rows(graph: &GraphForge, query: &str) -> Result<Vec<Vec<String>>, String> {
    let result = graph.execute(query).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "null".to_owned()
                        } else {
                            array_value_to_string(column, row).expect("render")
                        }
                    })
                    .collect(),
            );
        }
    }
    Ok(rows)
}

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    try_rows(graph, query).unwrap_or_else(|error| panic!("{query}: {error}"))
}

fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

// ---------------------------------------------------------------------------
// What the data defines: references computed from `nodes()` and `edges()`.
// ---------------------------------------------------------------------------

fn message_id(i: usize) -> i64 {
    message(i).id
}

fn date(i: usize) -> i64 {
    message(i).creation_date.expect("every message is dated")
}

/// What a message shows: its content, else its image file, else NULL.
fn shown(i: usize) -> String {
    let node = message(i);
    node.content
        .or(node.image_file)
        .unwrap_or_else(|| "null".to_owned())
}

fn text(value: Option<String>) -> String {
    value.unwrap_or_else(|| "null".to_owned())
}

fn author(i: usize) -> usize {
    edges()
        .iter()
        .find(|edge| edge.ty == "HAS_CREATOR" && edge.source == FIRST_MESSAGE + i)
        .expect("every message has a creator")
        .target
}

/// The persons within `hops` KNOWS steps of the first person, in either
/// direction, without that person.
fn friends_of_first(hops: usize) -> Vec<usize> {
    let knows: Vec<_> = edges().into_iter().filter(|e| e.ty == "KNOWS").collect();
    let mut seen = vec![0];
    let mut frontier = vec![0];
    for _ in 0..hops {
        let mut next = Vec::new();
        for edge in &knows {
            for &at in &frontier {
                let other = if edge.source == at {
                    edge.target
                } else if edge.target == at {
                    edge.source
                } else {
                    continue;
                };
                if !seen.contains(&other) {
                    seen.push(other);
                    next.push(other);
                }
            }
        }
        frontier = next;
    }
    seen.remove(0);
    seen.sort_unstable();
    seen
}

/// The `(creator, message)` pairs of `creators` whose date passes `keep`, in
/// the order of `ORDER BY date DESC, id ASC`.
fn messages_of(creators: &[usize], keep: impl Fn(i64) -> bool) -> Vec<(usize, usize)> {
    let mut rows: Vec<_> = (0..MESSAGES)
        .filter(|&i| creators.contains(&author(i)) && keep(date(i)))
        .map(|i| (author(i), i))
        .collect();
    rows.sort_by_key(|&(_, i)| (Reverse(date(i)), message_id(i)));
    rows
}

fn friend_message_row((friend, i): (usize, usize)) -> Vec<String> {
    let (id, first, last) = PERSONS[friend];
    vec![
        id.to_string(),
        first.to_owned(),
        last.to_owned(),
        message_id(i).to_string(),
        shown(i),
        date(i).to_string(),
    ]
}

const FRIEND_MESSAGE_COLUMNS: &str = "RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS postOrCommentId, coalesce(message.content, message.imageFile) AS postOrCommentContent, message.creationDate AS postOrCommentCreationDate ORDER BY postOrCommentCreationDate DESC";

/// The IC2 shape: the messages of a person's friends, newest first.
fn friends_messages(max_date: i64, tail: &str) -> String {
    format!(
        "MATCH (:Person {{id: 1}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message) WHERE (message:Post OR message:Comment) AND message.creationDate <= {max_date} {FRIEND_MESSAGE_COLUMNS}, toInteger(postOrCommentId) ASC {tail}"
    )
}

/// The IC9 shape: friends and friends of friends gathered into one list, then
/// each one's messages.
fn network_messages(max_date: i64, tail: &str) -> String {
    format!(
        "MATCH (root:Person {{id: 1}})-[:KNOWS*1..2]-(friend:Person) WHERE NOT friend = root WITH collect(DISTINCT friend) AS friends UNWIND friends AS friend MATCH (friend)<-[:HAS_CREATOR]-(message) WHERE (message:Post OR message:Comment) AND message.creationDate < {max_date} {FRIEND_MESSAGE_COLUMNS}, postOrCommentId ASC {tail}"
    )
}

/// The IC11 shape: where a person's network works, in a country.
fn network_jobs(before_year: i64, tail: &str) -> String {
    format!(
        "MATCH (person:Person {{id: 1}})-[:KNOWS*1..2]-(friend:Person) WHERE NOT(person = friend) WITH DISTINCT friend MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {{name: 'Laos'}}) WHERE workAt.workFrom < {before_year} RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, company.name AS organizationName, workAt.workFrom AS organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, toInteger(personId) ASC, organizationName DESC {tail}"
    )
}

fn network_jobs_reference(before_year: i64) -> Vec<Vec<String>> {
    let nodes = nodes();
    let edges = edges();
    let friends = friends_of_first(2);
    let mut jobs = Vec::new();
    for work in edges
        .iter()
        .filter(|e| e.ty == "WORK_AT" && friends.contains(&e.source))
    {
        let year = work.work_from.expect("WORK_AT has a year");
        let in_laos = edges.iter().any(|e| {
            e.ty == "IS_LOCATED_IN"
                && e.source == work.target
                && nodes[e.target].name.as_deref() == Some("Laos")
        });
        if year < before_year && in_laos {
            let (id, first, last) = PERSONS[work.source];
            let company = nodes[work.target].name.clone().expect("company name");
            jobs.push((year, id, company, [first.to_owned(), last.to_owned()]));
        }
    }
    jobs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(b.2.cmp(&a.2)));
    jobs.into_iter()
        .map(|(year, id, company, [first, last])| {
            vec![id.to_string(), first, last, company, year.to_string()]
        })
        .collect()
}

/// The durable project on several partitions and the in-memory graph of the same
/// data. A query must return the same rows on both.
struct Engines {
    durable: Durable,
    memory: GraphForge,
}

impl Engines {
    fn new() -> Self {
        Self {
            durable: durable(),
            memory: in_memory(),
        }
    }

    fn rows(&self, query: &str) -> Vec<Vec<String>> {
        let durable = rows(&self.durable.graph, query);
        let memory = rows(&self.memory, query);
        assert_eq!(
            durable, memory,
            "durable and in-memory rows differ: {query}"
        );
        durable
    }

    /// The rows of a query with no total order (a UNION), sorted: both
    /// engines must return the same multiset.
    fn sorted_rows(&self, query: &str) -> Vec<Vec<String>> {
        let mut durable = rows(&self.durable.graph, query);
        let mut memory = rows(&self.memory, query);
        durable.sort();
        memory.sort();
        assert_eq!(
            durable, memory,
            "durable and in-memory rows differ: {query}"
        );
        durable
    }
}

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// How many property overlays a physical plan reads below the `TopK` sort that
/// keeps `fetch` rows, and how many it reads above it.
struct OverlayShape {
    below_sort: usize,
    above_sort: usize,
}

fn overlays_around_sort(plan: &str, fetch: usize) -> OverlayShape {
    let lines: Vec<&str> = plan.lines().collect();
    let marker = format!("SortExec: TopK(fetch={fetch})");
    let sort = lines
        .iter()
        .position(|line| line.contains(&marker))
        .unwrap_or_else(|| panic!("no `{marker}` in\n{plan}"));
    let level = indent(lines[sort]);
    // The sort's inputs are the lines under it that sit deeper.
    let end = lines[sort + 1..]
        .iter()
        .position(|line| indent(line) <= level)
        .map_or(lines.len(), |offset| sort + 1 + offset);
    let mut shape = OverlayShape {
        below_sort: 0,
        above_sort: 0,
    };
    for (at, line) in lines.iter().enumerate() {
        if line.contains("PropertyOverlayExec") {
            if at > sort && at < end {
                shape.below_sort += 1;
            } else {
                shape.above_sort += 1;
            }
        }
    }
    shape
}

fn rows_and_requested(graph: &GraphForge, query: &str) -> (Vec<Vec<String>>, u64) {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let result = rows(graph, query);
    let requested = io_stats::snapshot()
        .expect("the I/O capture is installed")
        .property_rows_requested;
    (result, requested)
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

#[test]
fn return_only_properties_are_read_above_the_limit() {
    let _serial = serial();
    let single = durable_single_partition();

    let plan = physical_plan(&single.graph, &friends_messages(200, "LIMIT 5"));
    let shape = overlays_around_sort(&plan, 5);
    assert!(
        shape.above_sort >= 2,
        "the friend names and the message text are read after the sort:\n{plan}"
    );
    assert!(
        shape.below_sort >= 1,
        "the sort keys are still read before the sort:\n{plan}"
    );
    assert!(
        plan.contains("BoundedWindowAggExec") && plan.contains("__gf_late_row"),
        "the kept rows are numbered so the join cannot reorder them:\n{plan}"
    );

    // With SKIP the sort keeps both the skipped and the returned rows.
    let plan = physical_plan(&single.graph, &friends_messages(200, "SKIP 3 LIMIT 4"));
    let shape = overlays_around_sort(&plan, 7);
    assert!(shape.above_sort >= 2, "SKIP and LIMIT:\n{plan}");

    // A property the sort reads is read before it. The friend still waits.
    let sorted_by_text = format!(
        "{} , postOrCommentContent ASC LIMIT 5",
        friends_messages(200, "").trim_end()
    );
    let plan = physical_plan(&single.graph, &sorted_by_text);
    let shape = overlays_around_sort(&plan, 5);
    assert_eq!(
        shape.above_sort, 1,
        "only the friend is read after the sort when the sort reads the text:\n{plan}"
    );

    // Nothing is deferred without a limit, or when DISTINCT or an aggregate
    // sits between the scan and the limit.
    for (query, why) in [
        (friends_messages(200, ""), "no limit"),
        (
            "MATCH (m:Post) RETURN DISTINCT m.content AS c ORDER BY c LIMIT 5".to_owned(),
            "DISTINCT",
        ),
        (
            "MATCH (m:Post) RETURN m.creationDate AS d, count(m.content) AS n ORDER BY d LIMIT 5"
                .to_owned(),
            "aggregate",
        ),
    ] {
        let plan = physical_plan(&single.graph, &query);
        assert!(
            !plan.contains("__gf_late_row"),
            "{why} must not defer a read:\n{plan}"
        );
    }

    // The IC9 shape defers the message text. The friends arrive as collected
    // nodes that hold their properties whole, so there is no friend read to
    // defer.
    let plan = physical_plan(&single.graph, &network_messages(200, "LIMIT 5"));
    let shape = overlays_around_sort(&plan, 5);
    assert_eq!(
        shape.above_sort, 1,
        "IC9 shape: only the message text is read after the sort:\n{plan}"
    );
    assert!(
        shape.below_sort >= 2,
        "IC9 shape: the message keys and the friend collection are read before the sort:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

#[test]
fn deferred_read_requests_exactly_the_limit_rows() {
    let _serial = serial();
    let single = durable_single_partition();
    let sort = "ORDER BY date DESC, toInteger(id) ASC";
    let from = "MATCH (:Person {id: 1})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message) WHERE (message:Post OR message:Comment) AND message.creationDate <= 200";
    for n in [3usize, 7] {
        // The same query without the deferred column reads only the keys; the
        // difference is what the deferred column's read asked for.
        let (_, keys) = rows_and_requested(
            &single.graph,
            &format!(
                "{from} RETURN message.id AS id, message.creationDate AS date {sort} LIMIT {n}"
            ),
        );
        let (kept, with_text) = rows_and_requested(
            &single.graph,
            &format!(
                "{from} RETURN message.id AS id, message.creationDate AS date, coalesce(message.content, message.imageFile) AS text {sort} LIMIT {n}"
            ),
        );
        assert_eq!(kept.len(), n, "LIMIT {n} keeps {n} rows");
        assert_eq!(
            with_text,
            keys + n as u64,
            "the message text is read for exactly the {n} kept rows"
        );

        // A friend's names are read for the distinct friends of the kept rows.
        let (kept, with_names) = rows_and_requested(
            &single.graph,
            &format!(
                "{from} RETURN message.id AS id, message.creationDate AS date, friend.firstName AS name {sort} LIMIT {n}"
            ),
        );
        let friends: std::collections::BTreeSet<_> =
            kept.iter().map(|row| row[2].clone()).collect();
        assert_eq!(
            with_names,
            keys + friends.len() as u64,
            "the friend names are read for the {} friends of the {n} kept rows",
            friends.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[test]
fn ic_shapes_return_the_reference_rows() {
    let _serial = serial();
    let engines = Engines::new();
    let single = durable_single_partition();

    // IC2: direct friends, newest first; the date cut keeps part of the data.
    let ic2: Vec<Vec<String>> = messages_of(&friends_of_first(1), |d| d <= 107)
        .into_iter()
        .map(friend_message_row)
        .collect();
    assert!(ic2.len() > 10, "the IC2 reference is not trivial");
    for (tail, skip, limit) in [
        ("LIMIT 5", 0, 5),
        ("LIMIT 20", 0, 20),
        ("SKIP 3 LIMIT 4", 3, 4),
        ("SKIP 8 LIMIT 100", 8, 100),
        ("SKIP 100 LIMIT 5", 100, 5),
        ("LIMIT 0", 0, 0),
        ("LIMIT 1000", 0, 1000),
    ] {
        let expected: Vec<_> = ic2.iter().skip(skip).take(limit).cloned().collect();
        let query = friends_messages(107, tail);
        assert_eq!(engines.rows(&query), expected, "IC2 {tail}");
        assert_eq!(
            rows(&single.graph, &query),
            expected,
            "IC2 {tail}, one partition"
        );
    }

    // IC9: friends and friends of friends, a strict date bound.
    let ic9: Vec<Vec<String>> = messages_of(&friends_of_first(2), |d| d < 108)
        .into_iter()
        .map(friend_message_row)
        .collect();
    assert!(ic9.len() > ic2.len(), "IC9 reaches more people than IC2");
    for (tail, skip, limit) in [
        ("LIMIT 5", 0, 5),
        ("LIMIT 20", 0, 20),
        ("SKIP 6 LIMIT 7", 6, 7),
    ] {
        let expected: Vec<_> = ic9.iter().skip(skip).take(limit).cloned().collect();
        let query = network_messages(108, tail);
        assert_eq!(engines.rows(&query), expected, "IC9 {tail}");
        assert_eq!(
            rows(&single.graph, &query),
            expected,
            "IC9 {tail}, one partition"
        );
    }

    // IC11: ordered by the job's year, the person and the company.
    let jobs = network_jobs_reference(2011);
    assert_eq!(jobs.len(), 4, "every job in Laos before 2011");
    for (tail, limit) in [("LIMIT 3", 3), ("LIMIT 10", 10)] {
        let expected: Vec<_> = jobs.iter().take(limit).cloned().collect();
        let query = network_jobs(2011, tail);
        assert_eq!(engines.rows(&query), expected, "IC11 {tail}");
        assert_eq!(
            rows(&single.graph, &query),
            expected,
            "IC11 {tail}, one partition"
        );
    }
    let before_2010 = network_jobs_reference(2010);
    assert_eq!(before_2010.len(), 3, "the year bound drops one job");
    assert_eq!(engines.rows(&network_jobs(2010, "LIMIT 10")), before_2010);
}

#[test]
fn query_shapes_around_a_deferred_read_return_the_reference_rows() {
    let _serial = serial();
    let engines = Engines::new();
    let posts: Vec<usize> = (0..MESSAGES).filter(|i| i.is_multiple_of(2)).collect();
    let by_date_then_id = |mut ids: Vec<usize>| {
        ids.sort_by_key(|&i| (Reverse(date(i)), message_id(i)));
        ids
    };

    // ORDER BY a property the query does not return; NULL deferred values.
    let expected: Vec<_> = by_date_then_id(posts.clone())
        .into_iter()
        .take(5)
        .map(|i| {
            let node = message(i);
            vec![
                node.id.to_string(),
                text(node.content),
                text(node.image_file),
            ]
        })
        .collect();
    assert!(
        expected.iter().any(|row| row[1] == "null"),
        "the reference holds NULL deferred values"
    );
    assert_eq!(
        engines.rows("MATCH (m:Post) RETURN m.id AS id, m.content AS content, m.imageFile AS image ORDER BY m.creationDate DESC, m.id ASC LIMIT 5"),
        expected,
        "ORDER BY a property the query does not return"
    );

    // A property read by WHERE and by RETURN, and one read by RETURN only.
    let mut with_content: Vec<usize> = posts
        .iter()
        .copied()
        .filter(|&i| message(i).content.is_some() && date(i) >= 103)
        .collect();
    with_content.sort_by_key(|&i| Reverse(message_id(i)));
    let expected: Vec<_> = with_content
        .into_iter()
        .take(4)
        .map(|i| {
            let node = message(i);
            vec![
                date(i).to_string(),
                text(node.content),
                text(node.image_file),
                node.id.to_string(),
            ]
        })
        .collect();
    assert_eq!(
        engines.rows("MATCH (m:Post) WHERE m.content IS NOT NULL AND m.creationDate >= 103 RETURN m.creationDate AS date, m.content AS content, m.imageFile AS image, m.id AS id ORDER BY m.id DESC LIMIT 4"),
        expected,
        "a property used by WHERE and RETURN"
    );

    // DISTINCT sits before the limit: the limit keeps distinct rows.
    let mut dates: Vec<i64> = posts.iter().map(|&i| date(i)).collect();
    dates.sort_unstable_by_key(|&d| Reverse(d));
    dates.dedup();
    let expected: Vec<_> = dates.iter().take(4).map(|d| vec![d.to_string()]).collect();
    assert_eq!(
        engines.rows(
            "MATCH (m:Post) RETURN DISTINCT m.creationDate AS date ORDER BY date DESC LIMIT 4"
        ),
        expected,
        "DISTINCT before LIMIT"
    );
    let mut pairs: Vec<(i64, Option<String>)> = posts
        .iter()
        .map(|&i| (date(i), message(i).content))
        .collect();
    pairs.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.is_none().cmp(&b.1.is_none()))
            .then(a.1.cmp(&b.1))
    });
    pairs.dedup();
    let expected: Vec<_> = pairs
        .into_iter()
        .take(6)
        .map(|(d, c)| vec![d.to_string(), text(c)])
        .collect();
    assert_eq!(
        engines.rows("MATCH (m:Post) RETURN DISTINCT m.creationDate AS date, m.content AS content ORDER BY date DESC, content ASC LIMIT 6"),
        expected,
        "DISTINCT over two columns"
    );

    // An aggregate groups by a property and a second one feeds the sort.
    let mut totals: Vec<(String, usize, i64)> = PERSONS
        .iter()
        .enumerate()
        .filter_map(|(index, (_, first, _))| {
            let theirs: Vec<_> = (0..MESSAGES).filter(|&i| author(i) == index).collect();
            (!theirs.is_empty()).then(|| {
                (
                    (*first).to_owned(),
                    theirs.len(),
                    theirs.iter().map(|&i| date(i)).sum(),
                )
            })
        })
        .collect();
    totals.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
    let expected: Vec<_> = totals
        .into_iter()
        .take(2)
        .map(|(name, count, sum)| vec![name, count.to_string(), sum.to_string()])
        .collect();
    assert_eq!(
        engines.rows("MATCH (p:Person)<-[:HAS_CREATOR]-(m) RETURN p.firstName AS name, count(m) AS n, sum(m.creationDate) AS total ORDER BY total DESC, name ASC LIMIT 2"),
        expected,
        "aggregate with ORDER BY and LIMIT"
    );

    // UNION ALL of two limited branches.
    let mut expected: Vec<Vec<String>> = Vec::new();
    expected.extend(
        posts
            .iter()
            .copied()
            .filter(|&i| message(i).content.is_some())
            .take(2)
            .map(|i| vec![message_id(i).to_string(), shown(i)]),
    );
    expected.extend(
        (0..MESSAGES)
            .filter(|i| !i.is_multiple_of(2))
            .take(2)
            .map(|i| vec![message_id(i).to_string(), shown(i)]),
    );
    expected.sort();
    let branches = "MATCH (m:Post) WHERE m.content IS NOT NULL WITH m ORDER BY m.id ASC LIMIT 2 RETURN m.id AS id, m.content AS text UNION ALL MATCH (m:Comment) WITH m ORDER BY m.id ASC LIMIT 2 RETURN m.id AS id, m.content AS text";
    assert_eq!(
        engines.sorted_rows(branches),
        expected,
        "UNION ALL of limited branches"
    );

    // A variable bound once and matched again: directly, and through WITH.
    let friends_of_ann = [1, 2];
    let expected: Vec<_> = by_date_then_id(
        posts
            .iter()
            .copied()
            .filter(|&i| friends_of_ann.contains(&author(i)))
            .collect(),
    )
    .into_iter()
    .take(4)
    .map(|i| {
        vec![
            PERSONS[author(i)].1.to_owned(),
            message_id(i).to_string(),
            text(message(i).content),
        ]
    })
    .collect();
    assert_eq!(
        engines.rows("MATCH (:Person {id: 1})-[:KNOWS]->(f:Person) MATCH (f)<-[:HAS_CREATOR]-(m:Post) RETURN f.firstName AS name, m.id AS id, m.content AS content ORDER BY m.creationDate DESC, m.id ASC LIMIT 4"),
        expected,
        "a variable matched again"
    );
    let mut theirs: Vec<usize> = posts.iter().copied().filter(|&i| author(i) <= 2).collect();
    theirs.sort_by_key(|&i| Reverse(message_id(i)));
    let expected: Vec<_> = theirs
        .into_iter()
        .take(4)
        .map(|i| {
            let (_, first, last) = PERSONS[author(i)];
            vec![
                first.to_owned(),
                last.to_owned(),
                message_id(i).to_string(),
                text(message(i).content),
            ]
        })
        .collect();
    assert_eq!(
        engines.rows("MATCH (f:Person) WHERE f.id <= 3 WITH f MATCH (f)<-[:HAS_CREATOR]-(m:Post) RETURN f.firstName AS name, f.lastName AS last, m.id AS id, m.content AS content ORDER BY m.id DESC LIMIT 4"),
        expected,
        "a variable matched again after WITH"
    );

    // An alias computed from a property orders the rows.
    let mut by_bucket = posts.clone();
    by_bucket.sort_by_key(|&i| (Reverse(date(i) % 3), message_id(i)));
    let expected: Vec<_> = by_bucket
        .into_iter()
        .take(5)
        .map(|i| {
            vec![
                message_id(i).to_string(),
                text(message(i).content),
                (date(i) % 3).to_string(),
            ]
        })
        .collect();
    assert_eq!(
        engines.rows("MATCH (m:Post) RETURN m.id AS id, m.content AS content, m.creationDate % 3 AS bucket ORDER BY bucket DESC, id ASC LIMIT 5"),
        expected,
        "ORDER BY an alias computed from a property"
    );

    // OPTIONAL MATCH keeps a row for a person without a match, with NULLs.
    let mut expected: Vec<Vec<String>> = Vec::new();
    for (index, (_, first, _)) in PERSONS.iter().enumerate() {
        let mut theirs: Vec<usize> = posts
            .iter()
            .copied()
            .filter(|&i| author(i) == index && date(i) > 108)
            .collect();
        theirs.sort_by_key(|&i| message_id(i));
        if theirs.is_empty() {
            expected.push(vec![(*first).to_owned(), "null".into(), "null".into()]);
        }
        for i in theirs {
            let node = message(i);
            expected.push(vec![
                (*first).to_owned(),
                text(node.content),
                text(node.image_file),
            ]);
        }
    }
    assert!(
        expected
            .iter()
            .any(|row| row[1] != "null" || row[2] != "null"),
        "the reference matches one post"
    );
    assert_eq!(
        engines.rows("MATCH (p:Person) OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate > 108 RETURN p.firstName AS name, m.content AS content, m.imageFile AS image ORDER BY p.id ASC, m.id ASC LIMIT 6"),
        expected,
        "OPTIONAL MATCH nulls"
    );
}

// ---------------------------------------------------------------------------
// Ties and the order of the kept rows
// ---------------------------------------------------------------------------

/// A top-`n` by date alone leaves ties: the rows after the cutoff date are
/// determined, the rows on it are any of the tied messages.
fn assert_tied_top(rows: &[Vec<String>], n: usize, context: &str) {
    let all = messages_of(&friends_of_first(1), |_| true);
    let cutoff = date(all[n - 1].1);
    let above: Vec<_> = all.iter().filter(|&&(_, i)| date(i) > cutoff).collect();
    let on_cutoff: Vec<_> = all.iter().filter(|&&(_, i)| date(i) == cutoff).collect();
    assert_eq!(rows.len(), n, "{context}: {rows:?}");
    // [id, text, date], and each row is the message it names.
    for row in rows {
        let i = (row[0].parse::<i64>().expect("id") - 1000) as usize;
        assert_eq!(
            row,
            &vec![message_id(i).to_string(), shown(i), date(i).to_string()],
            "{context}: a row mixes two messages"
        );
    }
    let dates: Vec<i64> = rows.iter().map(|r| r[2].parse().expect("date")).collect();
    assert!(
        dates.windows(2).all(|w| w[0] >= w[1]),
        "{context}: not ordered by date: {rows:?}"
    );
    let ids: std::collections::BTreeSet<_> = rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids.len(), n, "{context}: a message repeats: {rows:?}");
    for &&(_, i) in &above {
        assert!(
            ids.contains(&message_id(i).to_string()),
            "{context}: lost {i}"
        );
    }
    for row in &rows[above.len()..] {
        assert!(
            on_cutoff
                .iter()
                .any(|&&(_, i)| message_id(i).to_string() == row[0]),
            "{context}: {row:?} is not a tied message"
        );
    }
}

#[test]
fn tied_sort_keys_keep_their_rows_and_their_order() {
    let _serial = serial();
    let engines = Engines::new();
    let single = durable_single_partition();
    let from = "MATCH (:Person {id: 1})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message) WHERE (message:Post OR message:Comment)";
    let tied = |n: usize| {
        format!(
            "{from} RETURN message.id AS id, coalesce(message.content, message.imageFile) AS text, message.creationDate AS date ORDER BY date DESC LIMIT {n}"
        )
    };
    let keys = |n: usize| {
        format!(
            "{from} RETURN message.id AS id, message.creationDate AS date ORDER BY date DESC LIMIT {n}"
        )
    };

    // Two friend messages share each date, so every limit below cuts a tie
    // group or ends on one. Repeat to catch an order that depends on timing.
    for n in [3, 5, 6, 9, 12] {
        for run in 0..4 {
            let context = format!("LIMIT {n}, run {run}");
            assert_tied_top(&rows(&engines.durable.graph, &tied(n)), n, &context);
        }
        // The in-memory engine breaks a tie its own way, so it is held to
        // the same invariants, not to the durable engine's rows.
        assert_tied_top(
            &rows(&engines.memory, &tied(n)),
            n,
            &format!("LIMIT {n}, in memory"),
        );

        // The deferred read follows the sort: on one partition the rows come
        // in the very order the same sort gives without the deferred column.
        let with_text = rows(&single.graph, &tied(n));
        let sort_only = rows(&single.graph, &keys(n));
        assert_eq!(
            with_text.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            sort_only.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            "LIMIT {n}: the deferred read reordered the kept rows"
        );
        assert_tied_top(&with_text, n, &format!("LIMIT {n}, one partition"));
    }

    // A SKIP moves the window over the ties; the deferred read keeps the rows
    // of the sort it follows.
    for (skip, limit) in [(0, 3), (3, 3), (2, 4), (5, 1)] {
        let page = |returned: &str| {
            rows(
                &single.graph,
                &format!(
                    "{from} RETURN message.id AS id, message.creationDate AS date{returned} ORDER BY date DESC SKIP {skip} LIMIT {limit}"
                ),
            )
        };
        let sort_only = page("");
        let deferred = page(", coalesce(message.content, message.imageFile) AS text");
        assert_eq!(deferred.len(), limit, "SKIP {skip} LIMIT {limit}");
        assert_eq!(
            deferred.iter().map(|r| r[..2].to_vec()).collect::<Vec<_>>(),
            sort_only,
            "SKIP {skip} LIMIT {limit}: the deferred read changed the page"
        );
        for row in &deferred {
            let i = (row[0].parse::<i64>().expect("id") - 1000) as usize;
            assert_eq!(
                row[2],
                shown(i),
                "SKIP {skip} LIMIT {limit}: text of {}",
                row[0]
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The projection runs for the kept rows
// ---------------------------------------------------------------------------

#[test]
fn projection_functions_run_for_the_kept_rows() {
    let _serial = serial();
    let engines = Engines::new();
    let single = durable_single_partition();

    // rand() is not deterministic, but it still yields one value per returned
    // row and a new one per run.
    let query = "MATCH (m:Post) WHERE m.content IS NOT NULL RETURN m.id AS id, m.creationDate AS date, rand() AS r ORDER BY id ASC LIMIT 3";
    let mut seen = std::collections::BTreeSet::new();
    for graph in [&engines.durable.graph, &single.graph, &engines.memory] {
        for _ in 0..2 {
            let result = rows(graph, query);
            assert_eq!(
                result.iter().map(|r| r[..2].to_vec()).collect::<Vec<_>>(),
                [1002i64, 1006, 1010]
                    .iter()
                    .map(|&id| {
                        let i = (id - 1000) as usize;
                        vec![id.to_string(), date(i).to_string()]
                    })
                    .collect::<Vec<_>>(),
                "rand() projection rows"
            );
            for row in &result {
                let r: f64 = row[2].parse().expect("rand() is a float");
                assert!((0.0..1.0).contains(&r), "rand() is in [0, 1): {r}");
                seen.insert(row[2].clone());
            }
        }
    }
    assert_eq!(
        seen.len(),
        3 * 3 * 2,
        "every returned row gets its own rand()"
    );

    // A projection that fails for a row only the limit removes. Posts with
    // content: 1002 has date 100 and divides by zero; the next ones do not.
    let divide = |order: &str, limit: usize| {
        format!(
            "MATCH (m:Post) WHERE m.content IS NOT NULL RETURN m.id AS id, 10 / (m.creationDate - 100) AS q ORDER BY id {order} LIMIT {limit}"
        )
    };
    let expected: Vec<Vec<String>> = [1026i64, 1022, 1018]
        .iter()
        .map(|&id| {
            let i = (id - 1000) as usize;
            vec![id.to_string(), (10 / (date(i) - 100)).to_string()]
        })
        .collect();
    for graph in [&engines.durable.graph, &single.graph, &engines.memory] {
        // The failing row is past the limit: it is never returned.
        assert_eq!(rows(graph, &divide("DESC", 3)), expected);
        // The failing row is among the kept rows: the query fails.
        for query in [divide("DESC", 7), divide("ASC", 3)] {
            let error = try_rows(graph, &query).expect_err("the failing row is returned");
            assert!(error.contains("ivide by zero"), "{query}: {error}");
        }
    }
}
