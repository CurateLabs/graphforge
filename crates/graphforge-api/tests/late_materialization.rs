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

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = graph
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
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
    rows
}

fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

