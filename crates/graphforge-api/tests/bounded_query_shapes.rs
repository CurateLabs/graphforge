//! Bounded-query cost by query shape at two graph sizes (#1388).
//!
//! Planning used to open every node and edge object of a table for a footer
//! row count, and that open admitted each object whole (exact length and
//! XXH64 on first touch, #1716), so every query paid for the whole node table
//! before reading a row. Construction shards are named by their contiguous
//! surrogate range, so the count now comes from the authenticated inventory
//! name and planning opens nothing (`ParquetFragment::for_path`).
//!
//! Five shapes run against compact (V2) projects as construction published
//! them, at 16,384 and 131,072 nodes (8x) with fan-out 16, so the larger
//! fixture spans two CSR shards (2,097,152 edges over a 1,048,576-edge shard
//! cap). Each shape either meets the structural bound or is recorded here as
//! unbounded by design:
//!
//! | shape | bound (from the manifest) | status |
//! | --- | --- | --- |
//! | ordered one-hop `LIMIT 1000` | 1 CSR shard + residual | bounded |
//! | ordered two-hop `LIMIT 1000` | 2 CSR shards + residual | bounded |
//! | ordered one-hop `LIMIT 10` | 1 CSR shard + residual | bounded |
//! | lookup by `node_uuid` | every node object | **unbounded by design**: its reads grow with the node table (measured 18,165 B at 16,384 nodes, 125,887 B at 131,072), bounded here at the declared node bytes so a regression to edge bytes still fails |
//! | one-hop with a property projection | 1 CSR shard + every node object + property fragments | **unbounded by design**: the projection leaves the ordered fast path for the generic expand, which reads the node table and the property fragments; bounded here at node + property + one shard bytes |
//!
//! What a query reads is the lifecycle attribution (application read bytes),
//! never wall time, which a shared runner cannot hold steady; wall time is
//! printed. Open is bounded at 64 bytes a node plus slack as in
//! `open_reads_control_bytes_not_payload_bytes`, plus the declared property
//! bytes: property fragments are still authenticated in full when a project
//! opens (#1716, "not in this PR"), a term this change does not touch.
//!
//! Criterion 4 (same-inode, same-length flip refused) for the path this change
//! touches: a flipped node object is refused by the generic scan on first
//! touch, and `count(n)` over it is refused too: the name-derived row count
//! is never substituted for an answer. It is reported as an inexact statistic
//! so no optimizer may ever do so; today none does (the pipeline does not run
//! DataFusion's statistics-based aggregate rewrite), so reporting it exact
//! does not change this test's outcome. The inexact precision is defense in
//! depth, pinned by `parquet_scan::tests::name_derived_counts_are_reported_as_inexact_statistics`.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
    IrLiteral, LifecycleIoCapture, lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::{GraphFilesInventory, graph_object_path, resolve_project_generation};

const SMALL_NODES: usize = 1 << 14;
const LARGE_NODES: usize = 1 << 17;
const FAN_OUT: usize = 16;
const LIMIT: usize = 1_000;
const WRITE_WINDOW: usize = 32 * 1024;
const CSR_SHARD_EDGES: usize = 1_048_576;

const OPEN_CONTROL_BYTES_PER_NODE: u64 = 64;
const CONTROL_SLACK_BYTES: u64 = 64 * 1024;
const EXECUTION_RESIDUAL_BYTES: u64 = 16 * 1024;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn node_name(index: usize) -> String {
    format!("entity-{index}")
}

/// Ring graph through the construction session: node `s` links to the next
/// `fan` nodes, every node carries a `name` property.
fn build(dir: &Path, nodes: usize, fan_out: usize) {
    assert!(nodes > 2 * fan_out, "the ring must not wrap onto itself");
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("name", DataType::Utf8, true)));
    let node_schema = Arc::new(Schema::new(fields));
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(nodes);
        let mut identities = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for node in start..end {
            identities.append_value(node_uuid(node).as_bytes()).unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(StringArray::from(
                    (start..end).map(node_name).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = nodes * fan_out;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            let offset = edge % fan_out + 1;
            identities.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + offset) % nodes).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
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
    // No trailing `index_adjacency`: that republishes as an expanded (V1)
    // generation. This is the compact generation construction published.
    session.seal_and_publish().unwrap();
}

/// What the layout declares, read from the manifest without admitting a byte.
struct Layout {
    node_bytes: u64,
    edge_bytes: u64,
    property_bytes: u64,
    largest_shard_bytes: u64,
    shards: u64,
    node_objects: Vec<(String, String, u64)>,
}

fn layout(inventory: &GraphFilesInventory) -> Layout {
    let mut by_class = BTreeMap::<&str, u64>::new();
    let mut largest_shard_bytes = 0;
    let mut shards = 0;
    let mut node_objects = Vec::new();
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        if path.contains(".csr.shards-") && path.ends_with(".csr") {
            shards += 1;
            largest_shard_bytes = largest_shard_bytes.max(file.byte_length);
        } else if path.starts_with("topology/nodes/") {
            *by_class.entry("nodes").or_default() += file.byte_length;
            node_objects.push((
                path.to_owned(),
                file.content_sha256.clone(),
                file.byte_length,
            ));
        } else if path.starts_with("topology/edges/") {
            *by_class.entry("edges").or_default() += file.byte_length;
        } else if path.starts_with("properties/") {
            *by_class.entry("properties").or_default() += file.byte_length;
        }
    }
    node_objects.sort();
    Layout {
        node_bytes: by_class["nodes"],
        edge_bytes: by_class["edges"],
        property_bytes: by_class.get("properties").copied().unwrap_or(0),
        largest_shard_bytes,
        shards,
        node_objects,
    }
}

#[derive(Debug)]
struct Measured {
    open_read: u64,
    execution_read: u64,
    open_wall: Duration,
    execution_wall: Duration,
    rows: usize,
    first_id: Option<Vec<u8>>,
    names: Vec<String>,
}

fn measure(path: &Path, query: &str, params: &HashMap<String, IrLiteral>) -> Measured {
    let _capture = LifecycleIoCapture::install();
    let before_open = lifecycle_io_snapshot().expect("requested observation");
    let started = Instant::now();
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    let open_wall = started.elapsed();
    let after_open = lifecycle_io_snapshot().expect("requested observation");
    let open = after_open.since(&before_open).expect("open attribution");
    open.validate_for_qualification().expect("open reconciles");

    let started = Instant::now();
    let result = forge.execute_with_params(query, params).unwrap();
    let execution_wall = started.elapsed();
    let execution = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&after_open)
        .expect("execution attribution");
    execution
        .validate_for_qualification()
        .expect("execution reconciles");

    let mut rows = 0;
    let mut first_id = None;
    let mut names = Vec::new();
    for batch in &result.batches {
        rows += batch.num_rows();
        if let Some(column) = batch.column_by_name("id")
            && first_id.is_none()
            && batch.num_rows() > 0
        {
            let column = column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("node_uuid is FixedSizeBinary");
            first_id = Some(column.value(0).to_vec());
        }
        if let Some(column) = batch.column_by_name("name") {
            let column = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name is Utf8");
            names.extend((0..batch.num_rows()).map(|row| column.value(row).to_owned()));
        }
    }
    Measured {
        open_read: open.totals.read_bytes,
        execution_read: execution.totals.read_bytes,
        open_wall,
        execution_wall,
        rows,
        first_id,
        names,
    }
}

struct Shape {
    name: &'static str,
    text: &'static str,
    /// Structural bound from the manifest: `None` marks a shape that is
    /// unbounded by design, bounded at the whole class it must scan.
    bound: fn(&Layout) -> (u64, bool),
    rows: fn(usize) -> usize,
}

const SHAPES: [Shape; 5] = [
    Shape {
        name: "ordered one-hop LIMIT 1000",
        text: "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
        bound: |layout| (layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES, true),
        rows: |_| LIMIT,
    },
    Shape {
        name: "ordered two-hop LIMIT 1000",
        text: "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000",
        bound: |layout| {
            (
                2 * layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES,
                true,
            )
        },
        rows: |_| LIMIT,
    },
    Shape {
        name: "ordered one-hop LIMIT 10",
        text: "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 10",
        bound: |layout| (layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES, true),
        rows: |_| 10,
    },
    Shape {
        name: "lookup by node_uuid",
        text: "MATCH (n) WHERE n.node_uuid = $uuid RETURN n.node_uuid AS id",
        bound: |layout| (layout.node_bytes + EXECUTION_RESIDUAL_BYTES, false),
        rows: |_| 1,
    },
    Shape {
        name: "one-hop with a property projection",
        text: "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id, b.name AS name ORDER BY id LIMIT 1000",
        bound: |layout| {
            (
                layout.node_bytes
                    + layout.property_bytes
                    + layout.largest_shard_bytes
                    + EXECUTION_RESIDUAL_BYTES,
                false,
            )
        },
        rows: |_| LIMIT,
    },
];

struct Size {
    nodes: usize,
    layout: Layout,
    results: Vec<Measured>,
}

fn run_size(nodes: usize) -> Size {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    let built = Instant::now();
    build(&path, nodes, FAN_OUT);
    let build_wall = built.elapsed();
    let inventory = resolve_project_generation(&path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let layout = layout(&inventory);
    eprintln!(
        "nodes={nodes} edges={} node_objects={} node_bytes={} edge_bytes={} property_bytes={} \
         shards={} largest_shard_bytes={} build_s={:.1}",
        nodes * FAN_OUT,
        layout.node_objects.len(),
        layout.node_bytes,
        layout.edge_bytes,
        layout.property_bytes,
        layout.shards,
        layout.largest_shard_bytes,
        build_wall.as_secs_f64()
    );
    let lookup = HashMap::from([(
        "uuid".to_owned(),
        IrLiteral::Uuid(*node_uuid(nodes / 2).as_bytes()),
    )]);
    let results = SHAPES
        .iter()
        .map(|shape| {
            let measured = measure(&path, shape.text, &lookup);
            let (bound, bounded) = (shape.bound)(&layout);
            eprintln!(
                "  {}: open_read={} exec_read={} bound={bound} ({}) rows={} open_ms={:.1} exec_ms={:.1}",
                shape.name,
                measured.open_read,
                measured.execution_read,
                if bounded { "bounded" } else { "unbounded by design" },
                measured.rows,
                measured.open_wall.as_secs_f64() * 1e3,
                measured.execution_wall.as_secs_f64() * 1e3,
            );
            measured
        })
        .collect();
    Size {
        nodes,
        layout,
        results,
    }
}

#[test]
fn query_shapes_cost_their_result_not_their_graph_across_an_8x_node_range() {
    let small = run_size(SMALL_NODES);
    let large = run_size(LARGE_NODES);
    assert_eq!(LARGE_NODES, 8 * SMALL_NODES);
    assert!(
        LARGE_NODES * FAN_OUT > CSR_SHARD_EDGES,
        "the larger fixture must span more than one CSR shard"
    );
    // Shards are per index direction, so the count is not one per shard cap;
    // the larger fixture must split at least one index across the cap.
    assert!(
        large.layout.shards > small.layout.shards,
        "the larger fixture declares {} CSR shard(s) against {} for the smaller",
        large.layout.shards,
        small.layout.shards
    );

    for size in [&small, &large] {
        let nodes = size.nodes;
        let open_bound = OPEN_CONTROL_BYTES_PER_NODE * nodes as u64
            + CONTROL_SLACK_BYTES
            + size.layout.property_bytes;
        for (shape, measured) in SHAPES.iter().zip(&size.results) {
            let name = shape.name;
            assert_eq!(
                measured.rows,
                (shape.rows)(nodes),
                "{name} nodes={nodes}: rows"
            );
            // The ordered shapes start at the smallest identity; the lookup
            // finds the one it asked for.
            let expected_first = if name.starts_with("lookup") {
                node_uuid(nodes / 2)
            } else {
                node_uuid(0)
            };
            assert_eq!(
                measured.first_id.as_deref(),
                Some(expected_first.as_bytes().as_slice()),
                "{name} nodes={nodes}: first id"
            );
            if name.contains("property") {
                assert_eq!(measured.names.len(), LIMIT, "{name}: names");
                assert_eq!(measured.names[0], node_name(0), "{name}: first name");
            }
            assert!(
                measured.open_read <= open_bound,
                "{name} nodes={nodes}: open read {} against a control bound of {open_bound}",
                measured.open_read
            );
            let (bound, _) = (shape.bound)(&size.layout);
            assert!(
                measured.execution_read <= bound,
                "{name} nodes={nodes}: execution read {} bytes against a structural bound of {bound}",
                measured.execution_read
            );
            // The bound must be tighter than the work it forbids.
            assert!(
                bound < size.layout.edge_bytes,
                "{name} nodes={nodes}: the bound {bound} admits a full edge scan ({} edge bytes)",
                size.layout.edge_bytes
            );
        }
    }

    // Across sizes the per-size bounds above are the statement: 8x nodes and
    // 2x the edges one shard holds leave the bounded shapes at `hops` shards
    // (the two-hop pays one shard when both hops fall in it, two otherwise)
    // and nothing proportional to the node table.
}

/// Same-inode, same-length flip of one byte in a node object.
fn flip(object: &Path, offset: u64) {
    let metadata = std::fs::metadata(object).unwrap();
    let length = metadata.len();
    // Content-store objects are installed read-only; the flip mutates the
    // inode in place and restores the mode so nothing but the byte changed.
    let mode = metadata.permissions();
    let mut writable = mode.clone();
    writable.set_readonly(false);
    std::fs::set_permissions(object, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(object)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
    assert_eq!(file.metadata().unwrap().len(), length);
    drop(file);
    std::fs::set_permissions(object, mode).unwrap();
}

#[test]
fn a_flipped_node_object_is_refused_by_the_scan_that_touches_it_and_by_count() {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    build(&path, 1 << 12, 4);
    let generation = resolve_project_generation(&path).unwrap();
    let inventory = generation
        .unadmitted_graph_files_inventory()
        .unwrap()
        .unwrap();
    let layout = layout(&inventory);
    let (relative, digest, length) = &layout.node_objects[0];
    let object: PathBuf = graph_object_path(generation.container_root(), digest).unwrap();
    // Hydration hard-links the content-store object into the workspace, so a
    // flip here is the same inode the query opens.
    flip(&object, length / 2);
    eprintln!("flipped {relative} ({length} bytes) at {}", length / 2);

    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let lookup = HashMap::from([("uuid".to_owned(), IrLiteral::Uuid(*node_uuid(7).as_bytes()))]);
    let error = forge
        .execute_with_params(SHAPES[3].text, &lookup)
        .expect_err("a scan over a corrupted node object must be refused");
    assert!(error.to_string().contains("XXH64"), "{error}");

    // The row count planning takes from the shard name is a hint, never an
    // answer: counting the nodes reads the object and refuses it.
    let error = forge
        .execute("MATCH (n) RETURN count(n) AS total")
        .expect_err("a count over a corrupted node object must be refused");
    assert!(error.to_string().contains("XXH64"), "{error}");

    // The ordered one-hop never touches node objects, so it still answers.
    let result = forge.execute(SHAPES[2].text).unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        10
    );
}
