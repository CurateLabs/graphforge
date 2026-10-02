//! Bounded-query cost by query shape at two graph sizes (#1388).
//!
//! Planning used to open every node and edge object of a table for a footer
//! row count, and that open admitted each object whole (exact length and
//! XXH64 on first touch, #1716), so every query paid for the whole node table
//! before reading a row. Construction shards are named by the surrogate range
//! they were encoded over, which is an upper bound on their rows (a delete
//! restages fewer rows under the same name; a per-route edge shard holds a
//! fraction of a window's range), so planning now takes that bound from the
//! authenticated inventory path as an inexact statistic and opens nothing
//! (`ParquetFragment::for_declared`). The node table reads only the files the
//! inventory declares, never a directory listing of the hydrated workspace.
//!
//! Five shapes run against compact (V2) projects as construction published
//! them, at 16,384 and 65,536 nodes (4x) with fan-out 17, so the larger
//! fixture spans two CSR shards per index (1,114,112 edges over a
//! 1,048,576-edge shard cap). Each shape either meets the structural bound or
//! is recorded here as unbounded:
//!
//! | shape | bound (from the manifest) | status |
//! | --- | --- | --- |
//! | ordered one-hop `LIMIT 1000` | 1 CSR shard + residual | bounded |
//! | ordered two-hop `LIMIT 1000` | 2 CSR shards + residual | bounded |
//! | ordered one-hop `LIMIT 10` | 1 CSR shard + residual | bounded |
//! | lookup by `node_uuid` | every node object | **unbounded pending an index probe**: `TopologyNodeTable::scan` ignores its filters, so the lookup scans the `node_uuid` column of every node object and its reads grow with the node table; a later #1388 slice prunes through `uuid_membership` / `read_nodes_filtered`. Bounded here at the declared node bytes so a regression to edge bytes still fails |
//! | one-hop with a property projection | 1 CSR shard + every node object + property fragments | **unbounded by design**: the projection leaves the ordered fast path for the generic expand, which reads the node table and the property fragments; bounded here at node + property + one shard bytes |
//!
//! What a query reads is the lifecycle attribution (application read bytes),
//! never wall time, which a shared runner cannot hold steady; wall time is
//! printed. Open is bounded from what it copies: hydration copies only the
//! small mutable controls (the route table and the UUID-membership manifests,
//! receipts, lock and tombstones) into private files and verifies the copy,
//! reading each twice; the node-linear forward and ordinal runs are
//! hard-linked and read nothing at open (#1719). The copied bytes come from
//! the open evidence and are capped at what the manifest declares for that
//! set; the property fragments are authenticated in full (#1716, "not in this
//! PR"); manifest, sidecars and footers fit a fixed slack. Execution also
//! reads the 64 KiB ordinal blocks holding the destinations it resolves plus
//! the two ends of the range (#1719), once per handle.
//!
//! Criterion 4 (same-inode, same-length flip refused) for the path this change
//! touches: a flipped node object is refused by the generic scan on first
//! touch, and `count(n)` over it is refused too, so the name-derived bound is
//! never substituted for an answer. DataFusion's `AggregateStatistics` rule
//! is registered (`with_default_features` in the exec session); it does not
//! fire for these counts only because of plan shape (the overlay join sits
//! between the aggregate and the scan, null counts are unknown, and
//! `count(*)` goes through `cypher_row_marker`), so reporting the bound exact
//! does not change this test's outcome today. `Inexact` is required because
//! the name is only an upper bound, and it is the guard if the plan shape
//! ever changes; `parquet_scan::tests::name_derived_counts_are_reported_as_inexact_statistics`
//! pins it. `a_shard_name_is_an_upper_bound_on_its_rows_after_a_delete` shows
//! the bound exceeding the footer, and
//! `an_unregistered_file_in_the_hydrated_node_directory_is_never_read` shows
//! the node table ignoring a file the inventory does not declare.

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
use graphforge_storage::ordinal_identity_v4::{ORDINAL_BLOCK_BYTES, ORDINAL_BLOCK_RECORDS};
use graphforge_storage::{GraphFilesInventory, graph_object_path, resolve_project_generation};

const SMALL_NODES: usize = 1 << 14;
const LARGE_NODES: usize = 1 << 16;
const FAN_OUT: usize = 17;
const LIMIT: usize = 1_000;
const WRITE_WINDOW: usize = 32 * 1024;
const CSR_SHARD_EDGES: usize = 1_048_576;

/// Hydration copies each single-link control and verifies the copy.
const OPEN_CONTROL_READS: u64 = 2;
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
    build_with_relations(dir, nodes, fan_out, &["LINK"]);
}

/// [`build`] with edge `e` typed `relations[e % relations.len()]`, so several
/// routes share each construction window. Without an ontology every type
/// shares the `_exploratory` route; `typed` adopts one declaring `Entity` and
/// each relation, so construction splits the window per route.
fn build_with_relations(dir: &Path, nodes: usize, fan_out: usize, relations: &[&str]) {
    build_graph(dir, nodes, fan_out, relations, false);
}

fn build_graph(dir: &Path, nodes: usize, fan_out: usize, relations: &[&str], typed: bool) {
    assert!(nodes > 2 * fan_out, "the ring must not wrap onto itself");
    let mut forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    if typed {
        use graphforge_api::{AdoptOntologyRequest, OntologyMode, OperationId, WriteContext};
        let mut document = String::from(
            "ontology_id: https://example.test/shapes\nversion: \"1\"\nentity_types:\n  - name: Entity\n    abstract: false\nrelation_types:\n",
        );
        for relation in relations {
            document.push_str(&format!(
                "  - name: {relation}\n    src: Entity\n    dst: Entity\n"
            ));
        }
        document.push_str(
            "properties:\n  - owner: Entity\n    name: name\n    type: utf8\n    nullable: true\n",
        );
        let ontology = dir
            .parent()
            .expect("project parent")
            .join("shapes-ontology.yaml");
        std::fs::write(&ontology, document).unwrap();
        forge
            .adopt_ontology(AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                path: ontology,
                mode: OntologyMode::Advisory,
            })
            .unwrap();
        // Construction binds the published semantic composition: publish the
        // adopted ontology's composition, then reopen so the facade loads it.
        drop(forge);
        let mut adopted = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
        let candidate = adopted.workspace_ontology_composition().unwrap().unwrap();
        let request = graphforge_api::CompositionChangeRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            expected_project_generation_uuid: resolve_project_generation(dir)
                .unwrap()
                .generation_uuid(),
            expected_composition_fingerprint: Some(candidate.composition_fingerprint.clone()),
            candidate,
            data_disposition: graphforge_api::CompositionDataDisposition::RequireConforming,
        };
        let preview = adopted
            .preview_ontology_composition_change(&request, None)
            .unwrap();
        assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
        adopted
            .publish_ontology_composition_change(&request, &preview, None)
            .unwrap();
        drop(adopted);
        forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    }
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
                Arc::new(StringArray::from(
                    (start..end)
                        .map(|edge| relations[edge % relations.len()])
                        .collect::<Vec<_>>(),
                )),
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

/// The top-level UUID-membership files hydration copies into single-link
/// private files and verifies (`requires_single_link_materialization`): the
/// manifests, receipts and lock, and the published tombstone runs. The
/// node-linear forward and ordinal runs are hard-linked (#1719).
fn is_copied_identity_control(path: &str) -> bool {
    let Some(name) = path.strip_prefix("topology/uuid-membership/") else {
        return false;
    };
    !name.contains('/')
        && (matches!(
            name,
            "manifest.json"
                | "topology-receipt.json"
                | "ordinal-v4-manifest.json"
                | "ordinal-v4-receipt.json"
                | "ordinal-v4.lock"
        ) || (name.starts_with("tombstones-v4-") && name.ends_with(".uuidx")))
}

/// What the layout declares, read from the manifest without admitting a byte.
struct Layout {
    node_bytes: u64,
    edge_bytes: u64,
    property_bytes: u64,
    /// Declared bytes of the controls hydration copies into single-link
    /// files: the route table and every top-level `topology/uuid-membership/`
    /// file (`requires_single_link_materialization`).
    copied_control_bytes: u64,
    largest_shard_bytes: u64,
    shards: u64,
    node_objects: Vec<(String, String, u64)>,
}

fn layout(inventory: &GraphFilesInventory) -> Layout {
    let mut by_class = BTreeMap::<&str, u64>::new();
    let mut largest_shard_bytes = 0;
    let mut shards = 0;
    let mut node_objects = Vec::new();
    let mut copied_control_bytes = 0;
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        if path == "semantic-routes.json" || is_copied_identity_control(path) {
            copied_control_bytes += file.byte_length;
        }
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
        copied_control_bytes,
        largest_shard_bytes,
        shards,
        node_objects,
    }
}

#[derive(Debug)]
struct Measured {
    open_read: u64,
    /// Declared bytes of the controls open copied (`GraphFilesOpenEvidence`).
    copied_bytes: u64,
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
    let mut open_phases: Vec<_> = open
        .phases
        .iter()
        .filter(|(_, totals)| totals.read_bytes != 0)
        .map(|(phase, totals)| {
            format!(
                "{phase:?}={}B/{}calls",
                totals.read_bytes, totals.read_calls
            )
        })
        .collect();
    open_phases.sort();
    eprintln!("    open phases: {open_phases:?}");

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
        copied_bytes: forge.graph_open_evidence().bytes_copied,
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
    /// Paths ending at one destination, per unit of fan-out; with the rows it
    /// gives the destinations resolved, so the ordinal blocks read.
    paths_per_destination: fn(usize) -> usize,
}

/// Ordinal blocks an execution reads to resolve its destinations: the blocks
/// holding them (a block read once per handle, then held) plus the two ends
/// of the one ordinal range construction publishes (#1719).
fn identity_bound(shape: &Shape, fan_out: usize) -> u64 {
    const RANGE_END_BLOCKS: u64 = 2;
    let destinations =
        ((shape.rows)(fan_out).div_ceil((shape.paths_per_destination)(fan_out)) + 1) as u64;
    (destinations.div_ceil(ORDINAL_BLOCK_RECORDS) + RANGE_END_BLOCKS) * ORDINAL_BLOCK_BYTES
}

const SHAPES: [Shape; 5] = [
    Shape {
        name: "ordered one-hop LIMIT 1000",
        text: "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
        bound: |layout| (layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES, true),
        rows: |_| LIMIT,
        paths_per_destination: |fan| fan,
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
        paths_per_destination: |fan| fan * fan,
    },
    Shape {
        name: "ordered one-hop LIMIT 10",
        text: "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 10",
        bound: |layout| (layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES, true),
        rows: |_| 10,
        paths_per_destination: |fan| fan,
    },
    Shape {
        name: "lookup by node_uuid",
        text: "MATCH (n) WHERE n.node_uuid = $uuid RETURN n.node_uuid AS id",
        // Unbounded pending an index probe: the scan ignores its filters.
        bound: |layout| (layout.node_bytes + EXECUTION_RESIDUAL_BYTES, false),
        rows: |_| 1,
        paths_per_destination: |_| 1,
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
        paths_per_destination: |fan| fan,
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
         copied_control_bytes={} shards={} largest_shard_bytes={} build_s={:.1}",
        nodes * FAN_OUT,
        layout.node_objects.len(),
        layout.node_bytes,
        layout.edge_bytes,
        layout.property_bytes,
        layout.copied_control_bytes,
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
            let bound = bound + identity_bound(shape, FAN_OUT);
            eprintln!(
                "  {}: open_read={} copied={} exec_read={} bound={bound} ({}) rows={} open_ms={:.1} exec_ms={:.1}",
                shape.name,
                measured.open_read,
                measured.copied_bytes,
                measured.execution_read,
                if bounded {
                    "bounded"
                } else if shape.name.starts_with("lookup") {
                    "unbounded pending an index probe"
                } else {
                    "unbounded by design"
                },
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
fn query_shapes_cost_their_result_not_their_graph_across_a_4x_node_range() {
    let small = run_size(SMALL_NODES);
    let large = run_size(LARGE_NODES);
    assert_eq!(LARGE_NODES, 4 * SMALL_NODES);
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
        let open_bound = OPEN_CONTROL_READS * size.layout.copied_control_bytes
            + size.layout.property_bytes
            + CONTROL_SLACK_BYTES;
        let copied_cap = size.layout.copied_control_bytes;
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
            // Open copies only the declared mutable controls, each read twice.
            assert!(
                measured.copied_bytes <= copied_cap,
                "{name} nodes={nodes}: open copied {} bytes against the {copied_cap} the \
                 manifest declares for the single-link controls",
                measured.copied_bytes
            );
            assert!(
                measured.open_read <= open_bound,
                "{name} nodes={nodes}: open read {} against a control bound of {open_bound} \
                 (2 x {} copied control bytes + {} property bytes + slack)",
                measured.open_read,
                size.layout.copied_control_bytes,
                size.layout.property_bytes
            );
            // Beyond the copied controls and the property fragments the bound
            // allows only the slack, and the slack is smaller than the node
            // and edge payload: reading one object at open fails this gate.
            assert!(
                CONTROL_SLACK_BYTES < size.layout.node_bytes + size.layout.edge_bytes,
                "{name} nodes={nodes}: the slack admits the payload"
            );
            let (bound, _) = (shape.bound)(&size.layout);
            let bound = bound + identity_bound(shape, FAN_OUT);
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

    // Across sizes the per-size bounds above are the statement: 4x nodes and
    // more edges than one shard holds leave the bounded shapes at `hops` shards
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

    // The bound planning takes from the shard name is a hint, never an
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

/// Footer row count of a Parquet file, read test-side with plain parquet.
fn footer_rows(path: &Path) -> i64 {
    parquet::file::reader::FileReader::metadata(
        &parquet::file::reader::SerializedFileReader::new(std::fs::File::open(path).unwrap())
            .unwrap(),
    )
    .file_metadata()
    .num_rows()
}

/// Every canonical `topology/<kind>/.../<range>.parquet` beneath `root`.
fn canonical_shards(root: &Path, kind: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
                && path
                    .ancestors()
                    .skip(1)
                    .filter_map(Path::file_name)
                    .take(3)
                    .any(|name| name == kind)
                && path
                    .ancestors()
                    .skip(1)
                    .filter_map(Path::file_name)
                    .any(|name| name == "topology")
            {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// A shard's name is the surrogate range it was encoded over, which is an
/// upper bound on its rows: `DETACH DELETE` restages the filtered batch
/// under the original name. The hint must stay at or above the footer and
/// be inexact.
#[test]
fn a_shard_name_is_an_upper_bound_on_its_rows_after_a_delete() {
    use graphforge_storage::ParquetFragment;

    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    build(&path, 512, 4);
    {
        let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
        let params = HashMap::from([(
            "uuid".to_owned(),
            IrLiteral::Uuid(*node_uuid(100).as_bytes()),
        )]);
        forge
            .execute_with_params(
                "MATCH (n) WHERE n.node_uuid = $uuid DETACH DELETE n",
                &params,
            )
            .unwrap();
    }
    // Publication may retain expanded paths or compact CAS paths. Keep the
    // manifest's logical shard names separate from their physical locations.
    let inventory = graphforge_storage::AuthenticatedPropertyInventory::capture(&path).unwrap();
    let nodes = inventory.node_fragments().expect("full node authority");
    let edges = inventory.edge_fragments(None);
    assert_eq!(nodes.len(), 1, "{nodes:?}");
    assert_eq!(edges.len(), 1, "{edges:?}");

    let node_rows = footer_rows(&nodes[0].0);
    let edge_rows = footer_rows(&edges[0].1);
    eprintln!(
        "{} holds {node_rows} rows; {} holds {edge_rows} rows",
        nodes[0].0.display(),
        edges[0].1.display()
    );
    assert_eq!(node_rows, 511, "one node deleted");
    assert_eq!(edge_rows, 2_040, "its four out- and four in-edges deleted");

    let canonical = project.path().join("retained-canonical");
    for (shard, relative, rows) in [
        (&nodes[0].0, &nodes[0].1, node_rows),
        (&edges[0].1, &edges[0].2, edge_rows),
    ] {
        // Exercise filename-derived hints on the exact same deleted payload
        // even when the published file is physically named by its CAS hash.
        let named = canonical.join(relative);
        std::fs::create_dir_all(named.parent().unwrap()).unwrap();
        std::fs::hard_link(shard, &named).unwrap();
        for fragment in [
            ParquetFragment::for_path(named, true),
            ParquetFragment::for_declared(shard.clone(), relative, true),
        ] {
            let hint = fragment.exact_rows.expect("a canonical shard has a hint");
            assert!(
                hint as i64 >= rows,
                "{relative}: hint {hint} below the footer's {rows} rows"
            );
            assert!(
                hint as i64 > rows,
                "{relative}: the fixture must show the name exceeding the rows"
            );
            assert!(
                !fragment.rows_exact,
                "{relative}: an upper bound is not exact"
            );
        }
    }
}

/// The node table reads the files the authenticated inventory declares. A
/// valid-looking shard dropped into the hydrated `topology/nodes/` is not in
/// the manifest, has no admission ticket (`admit_file` passes an inode
/// nothing registered), and must never reach a result. With the directory
/// listing (`TopologyNodeTable::open_project`) it doubles the count.
#[test]
fn an_unregistered_file_in_the_hydrated_node_directory_is_never_read() {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    build(&path, 1 << 12, 4);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let generation = resolve_project_generation(&path).unwrap();
    // The open hydrated a workspace beneath the container root.
    let workspace = std::fs::read_dir(generation.container_root())
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|candidate| {
            candidate.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("graphforge-graph-workspace-")
            })
        })
        .expect("hydrated workspace");
    let node_directory = workspace.join("topology").join("nodes");
    let shards = canonical_shards(&node_directory, "nodes");
    assert_eq!(shards.len(), 1, "{shards:?}");
    let declared = &shards[0];
    let planted = node_directory.join(format!("{:020}-{:020}.parquet", 4_097, 8_192));
    std::fs::copy(declared, &planted).unwrap();
    assert_eq!(footer_rows(&planted), 4_096);

    let count = forge.execute("MATCH (n) RETURN count(n) AS total").unwrap();
    let total = count.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        total, 4_096,
        "a file the inventory does not declare was counted"
    );

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

/// Every topology consumer shares the declared-file boundary, including
/// readers used while preparing and publishing writes. The planted files are
/// valid Parquet with canonical, non-overlapping names, so corruption checks
/// and filename validation cannot substitute for membership in the authority.
#[test]
fn undeclared_topology_files_do_not_change_readers_or_writes() {
    fn rows(batches: &[RecordBatch]) -> String {
        // Query ids and generation ids are intentionally fresh per invocation.
        // Compare data and counters, never random schema metadata.
        batches
            .iter()
            .map(|batch| format!("{:?}", batch.columns()))
            .collect()
    }
    fn exercise(surface: &str, planted: bool) -> Result<String, String> {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().join("state");
        build(&path, 8, 1);
        let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
        let generation = resolve_project_generation(&path).unwrap();
        let workspace = std::fs::read_dir(generation.container_root())
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .find(|candidate| {
                candidate.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with("graphforge-graph-workspace-")
                })
            })
            .unwrap();
        let declared =
            graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(
                &generation,
            )
            .unwrap();
        let topology =
            graphforge_storage::TopologyFileAuthority::from_inventory(&workspace, &declared)
                .unwrap();
        if surface == "bounded" && !planted {
            let narrowed = graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation_for_route(
                &generation, graphforge_storage::PropertyRouteKind::Node, "Entity").unwrap();
            assert!(
                graphforge_storage::TopologyFiles::from_inventory(&narrowed).is_err(),
                "route-scoped authority cannot authorize a complete topology read"
            );
        }
        #[cfg(feature = "search")]
        let search_before = if surface == "search" {
            Some(
                forge
                    .index_search(
                        "Entity",
                        graphforge_api::SearchIndexOptions::Text {
                            properties: Some(vec!["name".into()]),
                            rebuild: true,
                        },
                    )
                    .map_err(|error| error.to_string())?
                    .unwrap()
                    .source_fingerprint,
            )
        } else {
            None
        };
        if planted {
            let nodes = workspace.join("topology/nodes");
            let source = canonical_shards(&nodes, "nodes").remove(0);
            let planted = nodes.join(format!("{:020}-{:020}.parquet", 9, 16));
            // Conflicting labels make a UUID-map reader observable too: merely
            // copying identical labels could hide an unauthorized second read.
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(source).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            let mut writer = None;
            for batch in reader {
                let batch = batch.unwrap();
                let column = batch.schema().index_of("type_ids").unwrap();
                let arrow::datatypes::DataType::List(field) = batch.column(column).data_type()
                else {
                    unreachable!()
                };
                let mut labels = arrow::array::ListBuilder::new(arrow::array::UInt32Builder::new())
                    .with_field(Arc::clone(field));
                for _ in 0..batch.num_rows() {
                    labels.append(true);
                }
                let mut columns = batch.columns().to_vec();
                columns[column] = Arc::new(labels.finish());
                let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
                let writer = writer.get_or_insert_with(|| {
                    parquet::arrow::ArrowWriter::try_new(
                        std::fs::File::create(&planted).unwrap(),
                        batch.schema(),
                        None,
                    )
                    .unwrap()
                });
                writer.write(&batch).unwrap();
            }
            writer.unwrap().close().unwrap();
            if surface == "path" {
                let flat = workspace.join("topology/nodes.parquet");
                assert!(!flat.exists());
                assert!(
                    !declared
                        .node_fragments()
                        .unwrap()
                        .iter()
                        .any(|(_, relative)| relative == "topology/nodes.parquet")
                );
                std::fs::copy(&planted, flat).unwrap();
            }
            let edges = workspace.join("topology/edges");
            let route = std::fs::read_dir(&edges)
                .unwrap()
                .flatten()
                .map(|entry| entry.path())
                .find(|candidate| candidate.is_dir())
                .unwrap();
            let source = canonical_shards(&route, "edges").remove(0);
            std::fs::copy(source, route.join(format!("{:020}-{:020}.parquet", 9, 16))).unwrap();
        }
        let query = match surface {
            "bounded" => "MATCH (n) RETURN count(n) AS total",
            "recount" => "MATCH ()-[r]->() RETURN count(r) AS total",
            "filtered" => "MATCH (a {name:'entity-0'})-->(b) RETURN labels(b) AS labels",
            "delete" => "MATCH (n {name:'entity-0'}) DETACH DELETE n",
            "label set" => "MATCH (n) SET n:Tagged",
            "merge" => "MERGE (n:Entity) RETURN n.node_uuid AS id",
            "path" => {
                "MATCH p=(a {name:'entity-0'})-[*1..2]->(b) RETURN [n IN nodes(p) | labels(n)] AS labels"
            }
            "analyst" => {
                let batch = forge
                    .rank("Entity", graphforge_api::RankOptions::default())
                    .map_err(|error| error.to_string())?;
                return Ok(rows(&[batch]));
            }
            "search" => {
                #[cfg(feature = "search")]
                {
                    let inspection = forge
                        .index_search(
                            "Entity",
                            graphforge_api::SearchIndexOptions::Text {
                                properties: Some(vec!["name".into()]),
                                rebuild: true,
                            },
                        )
                        .map_err(|error| error.to_string())?;
                    let inspection = inspection.expect("text index inspection");
                    assert_eq!(
                        Some(&inspection.source_fingerprint),
                        search_before.as_ref(),
                        "undeclared topology changed the selected source"
                    );
                    let hits = forge
                        .find(graphforge_api::FindOptions {
                            label: Some("Entity".into()),
                            query: Some("entity".into()),
                            limit: 20,
                            ..Default::default()
                        })
                        .map_err(|error| error.to_string())?;
                    assert_eq!(hits.num_rows(), 8, "text retrieval lost selected members");
                    return Ok(format!(
                        "{:?}; {:?}; {}",
                        inspection.properties,
                        inspection.state,
                        rows(&[hits])
                    ));
                }
                #[cfg(not(feature = "search"))]
                return Ok("search feature disabled".into());
            }
            "uuid" => {
                let metrics = graphforge_storage::rebuild_uuid_membership_indexes_with_topology(
                    &workspace,
                    graphforge_storage::UuidIndexBuildLimits::default(),
                    topology,
                )
                .map_err(|error| error.to_string())?;
                return Ok(format!(
                    "nodes={}, edges={}",
                    metrics.node_count, metrics.edge_count
                ));
            }
            _ => unreachable!(),
        };
        let result = forge.execute(query).map_err(|error| error.to_string())?;
        if surface == "path" {
            let mut lengths = Vec::new();
            for batch in &result.batches {
                let paths = batch
                    .column_by_name("labels")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let value = paths.value(row);
                    let cells = value
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .unwrap();
                    lengths.push(cells.len());
                    for node in 0..cells.len() {
                        let value = cells.value(node);
                        let labels = value
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .unwrap();
                        assert_eq!(labels.iter().collect::<Vec<_>>(), vec![Some("Entity")]);
                    }
                }
            }
            lengths.sort_unstable();
            assert_eq!(lengths, vec![2, 3]);
        }
        if matches!(surface, "delete" | "label set" | "merge") {
            let staged = forge
                .execute("CREATE (n:Entity {name:'owned-after'}) WITH n MATCH (m:Entity) RETURN m")
                .map_err(|error| error.to_string())?;
            let expected = if surface == "delete" { 8 } else { 9 };
            assert_eq!(
                staged
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                expected,
                "a statement-owned append disappeared before publication"
            );
            let own = forge
                .execute("MATCH (n {name:'owned-after'}) RETURN count(n) AS total")
                .map_err(|error| error.to_string())?;
            let count = own.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count, 1, "a same-session staged append disappeared");
        }
        let after = forge
            .execute("MATCH (n) RETURN count(n) AS total")
            .map_err(|error| error.to_string())?;
        let reopened = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
        let reopened_count = reopened
            .execute("MATCH (n) RETURN count(n) AS total")
            .map_err(|error| error.to_string())?;
        assert_eq!(
            rows(&after.batches),
            rows(&reopened_count.batches),
            "selected topology changed on reopen"
        );
        let installed = resolve_project_generation(&path).unwrap();
        let admitted =
            graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(
                &installed,
            )
            .unwrap();
        let stray = format!("topology/nodes/{:020}-{:020}.parquet", 9, 16);
        assert!(
            !admitted
                .node_fragments()
                .unwrap()
                .iter()
                .any(|(_, name)| name == &stray),
            "publication authenticated an undeclared node file"
        );
        assert!(
            !admitted
                .edge_fragments(None)
                .iter()
                .any(|(_, _, name)| name.ends_with(&format!("/{:020}-{:020}.parquet", 9, 16))),
            "publication authenticated an undeclared edge file"
        );
        Ok(format!(
            "{}; {:?}; after={}",
            rows(&result.batches),
            result.side_effects,
            rows(&after.batches)
        ))
    }

    let mut differences = Vec::new();
    for surface in [
        "bounded",
        "recount",
        "filtered",
        "delete",
        "label set",
        "merge",
        "analyst",
        "search",
        "path",
        "uuid",
    ] {
        let declared = exercise(surface, false);
        assert!(
            declared.is_ok(),
            "{surface}: clean fixture failed: {declared:?}"
        );
        let planted = exercise(surface, true);
        if declared != planted {
            differences.push(format!(
                "{surface}: declared={declared:?}, planted={planted:?}"
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "undeclared topology changed answers:\n{}",
        differences.join("\n")
    );
}

/// Construction allocates edge surrogates per window and then splits the
/// rows by route, so each route's shard spans a range it only half fills:
/// the hint is an upper bound per route, inexact, and the fragments of one
/// route are exactly that route's objects.
#[test]
fn edge_fragment_hints_are_per_route_upper_bounds() {
    use graphforge_storage::{AuthenticatedPropertyInventory, ParquetFragment};

    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    let nodes = 1 << 10;
    let fan_out = 4;
    build_graph(&path, nodes, fan_out, &["LINK", "KNOWS"], true);
    let generation = resolve_project_generation(&path).unwrap();
    let inventory = AuthenticatedPropertyInventory::from_resolved_generation(&generation).unwrap();

    let all = inventory.edge_fragments(None);
    let routes: std::collections::BTreeSet<_> =
        all.iter().map(|(route, _, _)| route.clone()).collect();
    eprintln!("declared edge routes: {routes:?}");
    // Construction keys typed routes by their semantic route id; the catalog
    // resolves relation names to them through the published bindings, which
    // the counts below exercise.
    assert_eq!(routes.len(), 2, "one route per relation type: {routes:?}");
    let mut seen = std::collections::BTreeSet::new();
    for route in &routes {
        let fragments = inventory.edge_fragments(Some(route));
        assert!(!fragments.is_empty(), "{route}: no fragments");
        let mut rows = 0;
        let mut bound = 0;
        for (fragment_route, object, relative) in &fragments {
            assert_eq!(fragment_route, route);
            assert!(
                seen.insert(relative.clone()),
                "{relative} listed for two routes"
            );
            let footer = footer_rows(object);
            let fragment = ParquetFragment::for_declared(object.clone(), relative, false);
            let hint = fragment
                .exact_rows
                .expect("a canonical edge shard has a hint");
            assert!(
                hint as i64 >= footer,
                "{relative}: hint {hint} below {footer} rows"
            );
            assert!(
                !fragment.rows_exact,
                "{relative}: a per-route bound is not exact"
            );
            rows += footer;
            bound += hint as i64;
        }
        assert_eq!(
            rows as usize,
            nodes * fan_out / 2,
            "{route}: rows across its shards"
        );
        assert!(
            bound > rows,
            "{route}: the interleaved window must make the bound exceed the rows ({bound} vs {rows})"
        );
    }
    assert_eq!(
        seen.len(),
        all.len(),
        "every declared edge object belongs to one route"
    );

    // The providers resolve each route to its own objects.
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    for route in ["LINK", "KNOWS"] {
        let result = forge
            .execute(&format!(
                "MATCH ()-[r:{route}]->() RETURN count(r) AS total"
            ))
            .unwrap();
        let total = result.batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total as usize, nodes * fan_out / 2, "{route}: count");
    }
}
