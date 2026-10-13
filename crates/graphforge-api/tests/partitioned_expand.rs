//! Expansion runs on every partition of its input (#1984).
//!
//! `ExpandExec` and `VarLenExpandExec` used to declare one output partition, so
//! a traversal ran its adjacency and destination-node reads on a single task
//! whatever `target_partitions` was, and a planner exchange above it had to fan
//! that one stream back out. They now keep the partitions of their input, so a
//! traversal over many anchors uses more than one core.
//!
//! Two properties are pinned through the public API on a random graph:
//!
//! * at four partitions the physical plan never fans a single expansion stream
//!   out through an exchange, and every expansion reads from an exchange that
//!   produces four partitions; at one partition there is no exchange at all;
//! * single-hop, two-hop, variable-length and aggregate-then-expand statements
//!   return exactly the rows the edge list implies, at one partition and at four.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions,
};
use graphforge_core::uuid::Uuid;

const WRITE_WINDOW: usize = 4 * 1024;
const NODES: usize = 6_000;
const EDGES: usize = 3 * NODES;
/// Anchors of the single-hop statement; the others anchor fewer nodes so the
/// expected multisets stay small.
const ONE_HOP_ANCHORS: i64 = 3_000;
const MULTI_HOP_ANCHORS: i64 = 300;
/// Rows per batch. Traversals publish a row estimate (#1985), and the planner
/// spreads a plan over partitions only when it estimates more than one batch of
/// rows: a plan that fits one batch cannot use a second core. The default batch
/// (8192 rows) holds this whole fixture, so the plan under test would correctly
/// stay on one partition; a smaller batch makes the fixture span many.
const BATCH_SIZE: usize = 256;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Deterministic pseudo-random sequence (64-bit LCG): the graph is the same on
/// every run and no random-number crate is needed.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from((self.0 >> 33) % u64::try_from(bound).unwrap()).unwrap()
    }
}

/// `EDGES` random `(source, target)` pairs. Parallel edges and cycles occur;
/// self-loops do not, so a path never repeats an edge and the expected rows are
/// a plain join of the edge list.
fn random_edges() -> Vec<(usize, usize)> {
    let mut rng = Lcg(0x5eed_1984);
    let mut edges = Vec::with_capacity(EDGES);
    while edges.len() < EDGES {
        let (source, target) = (rng.below(NODES), rng.below(NODES));
        if source != target {
            edges.push((source, target));
        }
    }
    edges
}

/// Writes the graph as a durable project. With `properties`, every node also
/// carries an `ident` property equal to its index; without, nodes carry only
/// their identity and label, so reading a destination needs no property join.
fn construct(dir: &Path, edges: &[(usize, usize)], properties: bool) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    if properties {
        fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    }
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
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let mut columns = vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; end - start])),
        ];
        if properties {
            columns.push(Arc::new(idents.finish()));
        }
        let batch = RecordBatch::try_new(Arc::clone(&node_schema), columns).unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..edges.len()).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges.len());
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for (edge, (source, target)) in edges[start..end].iter().enumerate() {
            ids.append_value(edge_uuid(start + edge).as_bytes())
                .unwrap();
            sources.append_value(node_uuid(*source).as_bytes()).unwrap();
            targets.append_value(node_uuid(*target).as_bytes()).unwrap();
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
                batch_size: Some(BATCH_SIZE),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    edges: Vec<(usize, usize)>,
}

fn build(properties: bool) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    let edges = random_edges();
    construct(&project, &edges, properties);
    Built {
        _root: root,
        project,
        edges,
    }
}

type Row = Vec<i64>;

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns: Vec<&Int64Array> = batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("every returned column is Int64")
            })
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|column| {
                        assert!(!column.is_null(row), "returned value is never null");
                        column.value(row)
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

fn run(forge: &GraphForge, query: &str) -> Vec<Row> {
    rows(&forge.execute(query).expect("statement executes").batches)
}

fn one_hop_query() -> String {
    format!(
        "MATCH (a:Entity)-[:LINK]->(b:Entity) WHERE a.ident < {ONE_HOP_ANCHORS} \
         RETURN a.ident AS a, b.ident AS b"
    )
}

fn two_hop_query() -> String {
    format!(
        "MATCH (a:Entity)-[:LINK]->(b:Entity)-[:LINK]->(c:Entity) \
         WHERE a.ident < {MULTI_HOP_ANCHORS} RETURN a.ident AS a, b.ident AS b, c.ident AS c"
    )
}

fn var_len_query() -> String {
    format!(
        "MATCH (a:Entity)-[:LINK*1..2]->(b:Entity) WHERE a.ident < {MULTI_HOP_ANCHORS} \
         RETURN a.ident AS a, b.ident AS b"
    )
}

/// Aggregate first, so the second hop reads a hash-partitioned aggregate output
/// rather than a round-robin split of a scan.
fn aggregate_then_hop_query() -> String {
    "MATCH (a:Entity)-[:LINK]->(b:Entity) WITH b, count(*) AS c \
     MATCH (b)-[:LINK]->(d:Entity) RETURN b.ident AS b, d.ident AS d, c"
        .to_owned()
}

fn out_edges(edges: &[(usize, usize)]) -> HashMap<usize, Vec<usize>> {
    let mut out: HashMap<usize, Vec<usize>> = HashMap::new();
    for (source, target) in edges {
        out.entry(*source).or_default().push(*target);
    }
    out
}

fn as_row(nodes: &[usize]) -> Row {
    nodes.iter().map(|node| *node as i64).collect()
}

fn expected_one_hop(edges: &[(usize, usize)]) -> Vec<Row> {
    let mut expected: Vec<Row> = edges
        .iter()
        .filter(|(source, _)| (*source as i64) < ONE_HOP_ANCHORS)
        .map(|(source, target)| as_row(&[*source, *target]))
        .collect();
    expected.sort();
    expected
}

fn expected_two_hop(edges: &[(usize, usize)]) -> Vec<Row> {
    let out = out_edges(edges);
    let mut expected = Vec::new();
    for (a, b) in edges {
        if (*a as i64) >= MULTI_HOP_ANCHORS {
            continue;
        }
        for c in out.get(b).into_iter().flatten() {
            expected.push(as_row(&[*a, *b, *c]));
        }
    }
    expected.sort();
    expected
}

fn expected_var_len(edges: &[(usize, usize)]) -> Vec<Row> {
    let out = out_edges(edges);
    let mut expected = Vec::new();
    for (a, b) in edges {
        if (*a as i64) >= MULTI_HOP_ANCHORS {
            continue;
        }
        expected.push(as_row(&[*a, *b]));
        for c in out.get(b).into_iter().flatten() {
            expected.push(as_row(&[*a, *c]));
        }
    }
    expected.sort();
    expected
}

fn expected_aggregate_then_hop(edges: &[(usize, usize)]) -> Vec<Row> {
    let mut in_degree: HashMap<usize, i64> = HashMap::new();
    for (_, target) in edges {
        *in_degree.entry(*target).or_default() += 1;
    }
    let mut expected: Vec<Row> = edges
        .iter()
        .filter_map(|(b, d)| {
            in_degree
                .get(b)
                .map(|count| vec![*b as i64, *d as i64, *count])
        })
        .collect();
    expected.sort();
    expected
}

/// The statements at one and at four partitions return the rows the edge list
/// implies, and the multi-partition run is not a trivial one.
#[test]
fn expansion_rows_are_the_same_at_every_partition_count() {
    let built = build(true);
    let cases = [
        (
            "single hop",
            one_hop_query(),
            expected_one_hop(&built.edges),
        ),
        ("two hops", two_hop_query(), expected_two_hop(&built.edges)),
        (
            "variable length",
            var_len_query(),
            expected_var_len(&built.edges),
        ),
        (
            "aggregate then hop",
            aggregate_then_hop_query(),
            expected_aggregate_then_hop(&built.edges),
        ),
    ];
    for target_partitions in [1, 4] {
        let forge = open(&built.project, target_partitions);
        for (name, query, expected) in &cases {
            assert!(
                expected.len() > 500,
                "{name}: the fixture must produce a non-trivial result, got {} rows",
                expected.len()
            );
            assert_eq!(
                &run(&forge, query),
                expected,
                "{name} at {target_partitions} partitions"
            );
        }
    }
}

fn plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("plan")
}

fn indentation(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

fn is_expansion(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("ExpandExec:") || line.starts_with("VarLenExpandExec:")
}

/// Every operator below `lines[index]`: the lines that follow it with a deeper
/// indentation.
fn subtree<'a>(lines: &[&'a str], index: usize) -> Vec<&'a str> {
    let depth = indentation(lines[index]);
    lines[index + 1..]
        .iter()
        .copied()
        .take_while(|line| indentation(line) > depth)
        .collect()
}

/// The operator an exchange feeds when only projections and filters sit between
/// them: those pass a stream through, so an expansion found this way is the
/// single stream the exchange fans out. A join, an aggregate or a coalesce ends
/// the walk, because the stream above it is that operator's, not the
/// expansion's.
fn streamed_through(lines: &[&str], index: usize) -> bool {
    let depth = indentation(lines[index]);
    lines[index + 1..]
        .iter()
        .copied()
        .take_while(|line| indentation(line) > depth)
        .map(str::trim_start)
        .find(|line| !line.starts_with("ProjectionExec:") && !line.starts_with("FilterExec:"))
        .is_some_and(is_expansion)
}

/// An exchange whose output has four partitions.
fn is_four_way_exchange(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("RepartitionExec:")
        && (line.contains("RoundRobinBatch(4)") || line.contains(", 4)"))
}

/// At four partitions no exchange fans a single expansion stream out (the plan
/// shape that kept a traversal on one task), and every expansion reads from a
/// four-way exchange; at one partition the plan has no exchange.
#[test]
fn expansion_keeps_the_partitions_of_its_input() {
    let built = build(true);
    let queries = [
        one_hop_query(),
        two_hop_query(),
        var_len_query(),
        aggregate_then_hop_query(),
    ];
    for query in &queries {
        let single = plan(&open(&built.project, 1), query);
        assert!(
            single.lines().any(is_expansion),
            "no expansion in the plan:\n{single}"
        );
        assert!(
            !single.contains("RepartitionExec"),
            "one partition needs no exchange:\n{single}"
        );

        let partitioned = plan(&open(&built.project, 4), query);
        let lines: Vec<&str> = partitioned.lines().collect();
        assert!(
            lines.iter().copied().any(is_expansion),
            "no expansion in the plan:\n{partitioned}"
        );
        for (index, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("RepartitionExec:")
                && line.contains("input_partitions=1")
            {
                assert!(
                    !streamed_through(&lines, index),
                    "an exchange fans one expansion stream out:\n{partitioned}"
                );
            }
            if is_expansion(line) {
                assert!(
                    subtree(&lines, index).into_iter().any(is_four_way_exchange),
                    "{} must read from a four-way exchange:\n{partitioned}",
                    line.trim()
                );
            }
        }
    }
}

/// A limited traversal counts its demand and takes the first rows across every
/// row the hop emits, so it stays one pull stream: a demanded hop reads an
/// input the planner partitioned (here the aggregate feeding the second hop)
/// through one stream, and the limit returns that many real destinations.
#[test]
fn limited_expansion_reads_its_input_as_one_stream() {
    const LIMIT: usize = 25;
    let built = build(false);
    let query = format!(
        "MATCH (a:Entity)-[:LINK]->(b:Entity) WITH b, count(*) AS c \
         MATCH (b)-[:LINK]->(d:Entity) RETURN d.node_uuid AS id LIMIT {LIMIT}"
    );
    let forge = open(&built.project, 4);

    let physical = plan(&forge, &query);
    let lines: Vec<&str> = physical.lines().collect();
    let guarded: Vec<usize> = (0..lines.len())
        .filter(|&index| is_expansion(lines[index]) && lines[index].contains("cancel=guarded"))
        .collect();
    assert!(
        !guarded.is_empty(),
        "the limit must plan a demand-guarded expansion:\n{physical}"
    );
    for index in guarded {
        let input = lines.get(index + 1).map(|line| line.trim_start());
        assert!(
            input.is_some_and(|line| line.starts_with("CoalescePartitionsExec")),
            "a demanded expansion must read one stream:\n{physical}"
        );
    }

    let mut indegree: HashMap<usize, usize> = HashMap::new();
    for &(_, to) in &built.edges {
        *indegree.entry(to).or_default() += 1;
    }
    let reachable: std::collections::HashSet<Vec<u8>> = built
        .edges
        .iter()
        .filter(|(from, _)| indegree.contains_key(from))
        .map(|&(_, to)| node_uuid(to).as_bytes().to_vec())
        .collect();
    let batches = forge.execute(&query).expect("statement executes").batches;
    let mut returned = 0;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("node_uuid is a fixed-size binary column");
        for row in 0..ids.len() {
            assert!(
                reachable.contains(ids.value(row)),
                "row {row} is not the end of a two-hop path"
            );
            returned += 1;
        }
    }
    assert_eq!(returned, LIMIT);
}
