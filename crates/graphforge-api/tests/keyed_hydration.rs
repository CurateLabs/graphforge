//! A node's properties are hydrated by key after a sort and `LIMIT`: only the
//! rows the limit kept are looked up in the property route, instead of joining
//! them against every row of the route (#1982).
//!
//! The statements expand from twenty limited nodes and return a property of the
//! nodes they reach. The route holds many pages, so a read that scales with it
//! shows up as bytes and as pages. The gate is deterministic: it counts the
//! storage layer's page and byte reads, never time. The same statement with an
//! unbounded left side (`WITH DISTINCT`) plans as the hash join the lookup
//! replaces, and results must equal it, row for row, after mutations that
//! tombstone, replace and remove property rows.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::io_stats;

const WRITE_WINDOW: usize = 4 * 1024;
const PADDING_BYTES: usize = 64;
/// Rows of one point-access page of a property route.
const PAGE_ROWS: u64 = 2_048;
/// Pages of the property route: far more than the limited rows can touch.
const NODES: usize = 32 * PAGE_ROWS as usize;
/// Nodes with outgoing edges; the others are the unrelated route.
const LINKED_NODES: usize = 4 * 1024;
/// Nodes whose index is this modulo 1,000 have no `ident`.
const MISSING_IDENT: usize = 9;
/// The rows the limit keeps.
const LIMIT: u64 = 20;

/// The `ident` of a node, which some nodes lack.
fn ident(index: usize) -> Option<i64> {
    (index % 1000 != MISSING_IDENT).then_some(index as i64)
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

fn construct(dir: &Path, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
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
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_option(ident(node));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = LINKED_NODES.min(nodes) * 2;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / 2;
            let target = (source + edge % 2 + 1) % nodes;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets.append_value(node_uuid(target).as_bytes()).unwrap();
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

/// Expands from the twenty nodes with the smallest `ident`, then hydrates a
/// property of the nodes reached: a bounded side joined to the route.
const KEYED: &str = "MATCH (m:Entity) WITH m ORDER BY m.ident ASC LIMIT 20 \
                     MATCH (m)-[:LINK]->(c:Entity) RETURN c.padding AS p";
/// The same rows, but `DISTINCT` leaves the left side unbounded in the plan,
/// so the hydration is the hash join against the whole route.
const CONTROL: &str = "MATCH (m:Entity) WITH m ORDER BY m.ident ASC LIMIT 20 \
                       WITH DISTINCT m MATCH (m)-[:LINK]->(c:Entity) RETURN c.padding AS p";
/// The same expansion with no property hydrated at all.
const BASE: &str = "MATCH (m:Entity) WITH m ORDER BY m.ident ASC LIMIT 20 \
                    MATCH (m)-[:LINK]->(c:Entity) RETURN count(c) AS n";

/// What one statement returned and read.
struct Run {
    batches: Vec<RecordBatch>,
    read_bytes: u64,
    io: io_stats::IoSnapshot,
}

fn run(forge: &GraphForge, query: &str) -> Run {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge.execute(query).expect("statement executes");
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    Run {
        batches: result.batches,
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        io: io_stats::snapshot().expect("requested I/O statistics"),
    }
}

impl Run {
    /// The text column `name`, flattened across batches.
    fn text(&self, name: &str) -> Vec<Option<String>> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = cast(batch.column_by_name(name).expect("column"), &DataType::Utf8)
                .expect("text column");
            let text = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("text column");
            values.extend(text.iter().map(|value| value.map(str::to_owned)));
        }
        values
    }

    /// The integer column `name`, flattened across batches.
    fn integers(&self, name: &str) -> Vec<Option<i64>> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = cast(
                batch.column_by_name(name).expect("column"),
                &DataType::Int64,
            )
            .expect("integer column");
            let ints = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("integer column");
            values.extend(ints.iter());
        }
        values
    }

    /// The returned `p` values, sorted: the statements do not order their rows.
    fn padding(&self) -> Vec<Option<String>> {
        let mut rows = self.text("p");
        rows.sort();
        rows
    }

    /// The returned `(i, p)` pairs, sorted for the same reason.
    fn pairs(&self) -> Pairs {
        let mut rows: Pairs = self.integers("i").into_iter().zip(self.text("p")).collect();
        rows.sort();
        rows
    }
}

type Pairs = Vec<(Option<i64>, Option<String>)>;

fn plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("physical plan")
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

fn build(nodes: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, nodes);
    Built {
        _root: root,
        project,
    }
}

/// The bytes of the property fragments a project's route spans.
fn property_bytes(project: &Path) -> u64 {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .map(|file| file.byte_length)
        .sum()
}

/// Each limited node has two `LINK` edges, so the limit reaches at most this
/// many property rows, however large the route is.
const LOOKED_UP: u64 = 2 * LIMIT;

#[test]
fn limited_rows_are_looked_up_not_joined() {
    let _serial = serial();
    let built = build(NODES);
    let route_bytes = property_bytes(&built.project);
    for partitions in [1usize, 4] {
        let forge = open(&built.project, partitions);
        let keyed_plan = plan(&forge, KEYED);
        let control_plan = plan(&forge, CONTROL);
        assert!(
            keyed_plan.contains("KeyedPropertyLookupExec"),
            "{partitions} partitions: the limited side is looked up by key\n{keyed_plan}"
        );
        assert!(
            !control_plan.contains("KeyedPropertyLookupExec")
                && control_plan.contains("HashJoinExec"),
            "{partitions} partitions: the unbounded side stays a hash join\n{control_plan}"
        );

        let base = run(&forge, BASE);
        let keyed = run(&forge, KEYED);
        let control = run(&forge, CONTROL);
        assert_eq!(
            keyed.padding(),
            control.padding(),
            "{partitions} partitions: the lookup returns the hash join's rows"
        );
        assert_eq!(
            keyed.padding().len() as u64,
            LOOKED_UP,
            "{partitions} partitions: one row per edge of the limited nodes"
        );

        let io = &keyed.io;
        assert!(
            io.property_uuid_selections >= 1,
            "{partitions} partitions: the route is read by key: {io:?}"
        );
        assert!(
            io.property_uuid_pages_selected < io.property_uuid_pages_considered,
            "{partitions} partitions: pages outside the keys are skipped: {io:?}"
        );
        assert!(
            io.property_uuid_pages_selected <= LOOKED_UP
                && io.property_uuid_rows_decoded <= LOOKED_UP * PAGE_ROWS
                && io.property_uuid_rows_decoded < NODES as u64,
            "{partitions} partitions: at most {LOOKED_UP} keys read, never the route: {io:?}"
        );
        // Bytes confirm the pages: hydrating the nodes the limit reached adds a
        // fraction of the route to what the expansion alone reads.
        assert!(
            keyed.read_bytes < base.read_bytes + route_bytes / 2,
            "{partitions} partitions: keyed {} base {} route {route_bytes} (control {})",
            keyed.read_bytes,
            base.read_bytes,
            control.read_bytes,
        );
    }
}

fn execute(forge: &GraphForge, query: &str) {
    forge.execute(query).expect("statement executes");
}

/// A node as the model holds it: a node and its optional properties.
struct Entity {
    alive: bool,
    ident: Option<i64>,
    padding: Option<String>,
}

/// The graph the statements below should leave, kept in plain Rust.
struct Model {
    nodes: Vec<Entity>,
    edges: Vec<(usize, usize)>,
}

impl Model {
    fn new(nodes: usize) -> Self {
        let linked = LINKED_NODES.min(nodes);
        Self {
            nodes: (0..nodes)
                .map(|node| Entity {
                    alive: true,
                    ident: ident(node),
                    padding: Some(padding(node)),
                })
                .collect(),
            edges: (0..linked * 2)
                .map(|edge| {
                    let source = edge / 2;
                    (source, (source + edge % 2 + 1) % nodes)
                })
                .collect(),
        }
    }

    fn with_ident(&mut self, ident: i64) -> &mut Entity {
        self.nodes
            .iter_mut()
            .find(|node| node.alive && node.ident == Some(ident))
            .expect("one node holds the ident")
    }

    fn index_of(&self, ident: i64) -> usize {
        self.nodes
            .iter()
            .position(|node| node.alive && node.ident == Some(ident))
            .expect("one node holds the ident")
    }

    fn link_new(&mut self, source_ident: i64, ident: Option<i64>, padding: Option<&str>) {
        let source = self.index_of(source_ident);
        self.nodes.push(Entity {
            alive: true,
            ident,
            padding: padding.map(str::to_owned),
        });
        self.edges.push((source, self.nodes.len() - 1));
    }

    /// What `KEYED` returns: the twenty smallest idents, then their targets.
    fn expected(&self) -> Pairs {
        let mut limited: Vec<usize> = (0..self.nodes.len())
            .filter(|&node| self.nodes[node].alive && self.nodes[node].ident.is_some())
            .collect();
        limited.sort_by_key(|&node| self.nodes[node].ident);
        limited.truncate(LIMIT as usize);
        let mut rows: Pairs = self
            .edges
            .iter()
            .filter(|(source, target)| limited.contains(source) && self.nodes[*target].alive)
            .map(|&(_, target)| (self.nodes[target].ident, self.nodes[target].padding.clone()))
            .collect();
        rows.sort();
        rows
    }
}

const PAIRS: &str = "MATCH (m:Entity) WITH m ORDER BY m.ident ASC LIMIT 20 \
                     MATCH (m)-[:LINK]->(c:Entity) RETURN c.ident AS i, c.padding AS p";
const PAIRS_CONTROL: &str = "MATCH (m:Entity) WITH m ORDER BY m.ident ASC LIMIT 20 \
                             WITH DISTINCT m MATCH (m)-[:LINK]->(c:Entity) \
                             RETURN c.ident AS i, c.padding AS p";

/// Both forms and the model agree, after the project is reopened.
fn assert_agrees(project: &Path, model: &Model, stage: &str) {
    let expected = model.expected();
    for partitions in [1usize, 4] {
        let forge = open(project, partitions);
        let keyed_plan = plan(&forge, PAIRS);
        assert!(
            keyed_plan.contains("KeyedPropertyLookupExec"),
            "{stage}, {partitions} partitions: the statement is looked up by key\n{keyed_plan}"
        );
        assert_eq!(
            run(&forge, PAIRS).pairs(),
            expected,
            "{stage}, {partitions} partitions: lookup against the model"
        );
        assert_eq!(
            run(&forge, PAIRS_CONTROL).pairs(),
            expected,
            "{stage}, {partitions} partitions: hash join against the model"
        );
    }
}

#[test]
fn lookup_matches_the_join_through_replaced_removed_and_deleted_properties() {
    let _serial = serial();
    let nodes = 600;
    let built = build(nodes);
    let mut model = Model::new(nodes);
    assert_agrees(&built.project, &model, "constructed");

    let forge = open(&built.project, 1);
    // A property replaced, then replaced again: the newest row wins.
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 5 SET n.padding = 'first'",
    );
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 5 SET n.padding = 'second'",
    );
    model.with_ident(5).padding = Some("second".to_owned());
    // One property removed, and then every property removed.
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 6 REMOVE n.padding",
    );
    model.with_ident(6).padding = None;
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 7 REMOVE n.ident, n.padding",
    );
    let seven = model.with_ident(7);
    seven.ident = None;
    seven.padding = None;
    // A limited node whose own ident moves it out of the limit.
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 15 SET n.ident = 2000000",
    );
    model.with_ident(15).ident = Some(2_000_000);
    // A target that is deleted: its row is tombstoned and its edges go.
    execute(&forge, "MATCH (n:Entity) WHERE n.ident = 8 DETACH DELETE n");
    model.with_ident(8).alive = false;
    // Targets created with every combination of properties.
    execute(
        &forge,
        "MATCH (m:Entity) WHERE m.ident = 11 \
         CREATE (m)-[:LINK]->(:Entity {ident: 1000000, padding: 'created'})",
    );
    model.link_new(11, Some(1_000_000), Some("created"));
    execute(
        &forge,
        "MATCH (m:Entity) WHERE m.ident = 12 CREATE (m)-[:LINK]->(:Entity {padding: 'no-ident'})",
    );
    model.link_new(12, None, Some("no-ident"));
    execute(
        &forge,
        "MATCH (m:Entity) WHERE m.ident = 13 CREATE (m)-[:LINK]->(:Entity)",
    );
    model.link_new(13, None, None);
    // A target outside the limited nodes' reach changes nothing they return.
    execute(
        &forge,
        "MATCH (n:Entity) WHERE n.ident = 400 SET n.padding = 'elsewhere'",
    );
    model.with_ident(400).padding = Some("elsewhere".to_owned());
    drop(forge);

    assert_agrees(&built.project, &model, "mutated");
}
