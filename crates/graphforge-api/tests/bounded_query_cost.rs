//! Bounded-query cost gate at two graph sizes (#1388, acceptance criterion 6).
//!
//! The ladder's two canonical queries, an ordered one-hop and an ordered
//! two-hop with `LIMIT 1000`, run through the ordinary public path against
//! compact (V2) projects as construction published them. Edge fan-out grows
//! 16x over a fixed node set, so any work proportional to the edge payload
//! shows up and nothing else moves. Each query opens a fresh `GraphForge`, so
//! the cost is open plus first execution, the cost a user pays.
//!
//! The gate is deterministic: it asserts application read bytes (the
//! lifecycle attribution) and adjacency rows examined, never wall time, which
//! a shared CI runner cannot hold steady. Wall time is printed for the record.
//!
//! What a bounded query is allowed to read, derived from the layout and not
//! from any observed output:
//!
//! - **Open**: the manifest, the route table, the sidecars and the identity
//!   controls (about 40 bytes a node). Never a payload. At most
//!   `OPEN_CONTROL_BYTES_PER_NODE` per node plus `CONTROL_SLACK_BYTES`.
//! - **Execution**: every node object once, checksummed whole on first touch
//!   (the node-linear term, see below); at most one CSR shard per hop, read
//!   whole because a shard carries its own checksum; and a bounded residual of
//!   footers and manifests. Edge objects are never read.
//!
//! The node-linear term is a known defect, not a design: planning registers the
//! node table with `ParquetFragment::for_path`, whose footer read admits every
//! node object whole, so a query that touches 250 nodes pays for all of them.
//! It is bounded here separately, at 1x the declared node bytes, so the edge
//! property this gate asserts is not hidden behind a loose number. The CSR
//! shard term grows with the edges one shard holds, capped at
//! `DEFAULT_CSR_SHARD_EDGES` edges; both sizes here sit below that cap.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{GraphForge, LifecycleIoCapture, lifecycle_io_snapshot};
use graphforge_exec::demand::{self, DemandSnapshot};
use graphforge_storage::{GraphFilesInventory, resolve_project_generation};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 1 << 12;
const SMALL_FAN_OUT: usize = 8;
const LARGE_FAN_OUT: usize = 128;
const LIMIT: usize = 1_000;

const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";

/// Identity controls copied while hydrating are about 40 bytes a node; the
/// open-cost test (`open_reads_control_bytes_not_payload_bytes`) bounds the
/// copied bytes at 64 a node, and so does this gate.
const OPEN_CONTROL_BYTES_PER_NODE: u64 = 64;
/// Manifest, route table, sidecar and footer reads that do not scale with
/// either axis of this fixture.
const CONTROL_SLACK_BYTES: u64 = 64 * 1024;
/// Footer and sidecar reads one execution makes beyond the objects it admits.
const EXECUTION_RESIDUAL_BYTES: u64 = 16 * 1024;
/// Open grows with the file count (one manifest and route entry per file) and
/// with nothing else; a 16x payload may cost at most this much more to open.
const OPEN_GROWTH_TOLERANCE: u64 = 2;

struct Query {
    name: &'static str,
    text: &'static str,
    /// Expand operators on the path; each may read one CSR shard.
    hops: u64,
    /// Paths ending at one destination, per unit of fan-out.
    paths_per_destination: fn(usize) -> usize,
}

const QUERIES: [Query; 2] = [
    Query {
        name: "one-hop",
        text: ONE_HOP,
        hops: 1,
        paths_per_destination: |fan| fan,
    },
    Query {
        name: "two-hop",
        text: TWO_HOP,
        hops: 2,
        paths_per_destination: |fan| fan * fan,
    },
];

/// What the layout declares, read from the manifest without admitting a byte.
struct Layout {
    node_bytes: u64,
    edge_bytes: u64,
    largest_shard_bytes: u64,
}

fn layout(inventory: &GraphFilesInventory) -> Layout {
    let mut by_class = BTreeMap::<&str, u64>::new();
    let mut largest_shard_bytes = 0;
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        if path.contains(".csr.shards-") && path.ends_with(".csr") {
            largest_shard_bytes = largest_shard_bytes.max(file.byte_length);
        } else if path.starts_with("topology/nodes/") {
            *by_class.entry("nodes").or_default() += file.byte_length;
        } else if path.starts_with("topology/edges/") {
            *by_class.entry("edges").or_default() += file.byte_length;
        }
    }
    Layout {
        node_bytes: by_class["nodes"],
        edge_bytes: by_class["edges"],
        largest_shard_bytes,
    }
}

#[derive(Debug)]
struct Measured {
    open_read: u64,
    execution_read: u64,
    rows_examined: u64,
    candidates: u64,
    execution_batch_rows: u64,
    open_wall: Duration,
    execution_wall: Duration,
    ids: Vec<Vec<u8>>,
}

fn measure(path: &std::path::Path, query: &str) -> Measured {
    let _capture = LifecycleIoCapture::install();
    let before_open = lifecycle_io_snapshot().expect("requested observation");
    let started = Instant::now();
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    let open_wall = started.elapsed();
    let after_open = lifecycle_io_snapshot().expect("requested observation");
    let open = after_open.since(&before_open).expect("open attribution");
    open.validate_for_qualification().expect("open reconciles");

    let started = Instant::now();
    let (result, snapshot): (_, DemandSnapshot) = demand::capture(|| forge.execute(query));
    let execution_wall = started.elapsed();
    let result = result.unwrap();
    let execution = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&after_open)
        .expect("execution attribution");
    execution
        .validate_for_qualification()
        .expect("execution reconciles");

    let mut ids = Vec::new();
    for batch in &result.batches {
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("node_uuid is FixedSizeBinary");
        ids.extend((0..batch.num_rows()).map(|row| column.value(row).to_vec()));
    }
    Measured {
        open_read: open.totals.read_bytes,
        execution_read: execution.totals.read_bytes,
        rows_examined: snapshot
            .hops
            .values()
            .map(|hop| hop.adjacency_rows_examined)
            .sum(),
        candidates: snapshot
            .hops
            .values()
            .map(|hop| hop.candidates_generated)
            .sum(),
        execution_batch_rows: snapshot.execution_batch_rows,
        open_wall,
        execution_wall,
        ids,
    }
}

/// The answer from the construction rule alone: node `s` links to the next
/// `fan` nodes on a ring, so every node is the destination of `paths` paths
/// and the ordered answer is the identities in order, each repeated `paths`
/// times, cut at the limit.
fn expected_ids(paths_per_destination: usize) -> Vec<Vec<u8>> {
    (0..NODES)
        .flat_map(|node| {
            std::iter::repeat_n(
                bulk_fixture::fixture_node_uuid(node).as_bytes().to_vec(),
                paths_per_destination,
            )
        })
        .take(LIMIT)
        .collect()
}

struct Size {
    fan_out: usize,
    layout: Layout,
    /// Per query: open, execution.
    queries: Vec<(&'static str, Measured)>,
}

fn run_size(fan_out: usize) -> Size {
    assert!(NODES > 2 * fan_out, "the ring must not wrap onto itself");
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    // Without the trailing `index_adjacency`, which republishes the project
    // as an expanded (V1) generation: this measures the compact generation
    // the construction session published, with its shipped adjacency CSR.
    bulk_fixture::generate_bulk_graph_with_index(&path, NODES, fan_out, false);
    let inventory = resolve_project_generation(&path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let layout = layout(&inventory);
    let queries = QUERIES
        .iter()
        .map(|query| (query.name, measure(&path, query.text)))
        .collect();
    Size {
        fan_out,
        layout,
        queries,
    }
}

#[test]
fn bounded_queries_cost_their_result_not_their_graph_across_a_16x_edge_range() {
    let small = run_size(SMALL_FAN_OUT);
    let large = run_size(LARGE_FAN_OUT);
    assert_eq!(LARGE_FAN_OUT, 16 * SMALL_FAN_OUT);

    for size in [&small, &large] {
        let edges = NODES * size.fan_out;
        eprintln!(
            "size edges={edges} node_bytes={} edge_bytes={} largest_shard_bytes={}",
            size.layout.node_bytes, size.layout.edge_bytes, size.layout.largest_shard_bytes
        );
        for (query, (name, measured)) in QUERIES.iter().zip(&size.queries) {
            eprintln!(
                "  {name}: open_read={} exec_read={} rows_examined={} candidates={} \
                 open_ms={:.1} exec_ms={:.1}",
                measured.open_read,
                measured.execution_read,
                measured.rows_examined,
                measured.candidates,
                measured.open_wall.as_secs_f64() * 1e3,
                measured.execution_wall.as_secs_f64() * 1e3,
            );
            assert_eq!(*name, query.name);
            assert_query_bounds(query, size, measured);
        }
    }

    // The edge payload grew 16x. The comparison is meaningful only if it did.
    assert!(
        large.layout.edge_bytes > 8 * small.layout.edge_bytes,
        "edge payload did not grow: {} -> {}",
        small.layout.edge_bytes,
        large.layout.edge_bytes
    );
    // The node set is identical, so the node-linear term must be too.
    assert_eq!(small.layout.node_bytes, large.layout.node_bytes);

    for ((_, small_run), (name, large_run)) in small.queries.iter().zip(&large.queries) {
        assert!(
            large_run.open_read <= OPEN_GROWTH_TOLERANCE * small_run.open_read,
            "{name}: open read grew with the edge payload: {} -> {} bytes",
            small_run.open_read,
            large_run.open_read
        );
        // More fan-out means more rows per destination, so the same LIMIT
        // reaches fewer destinations: the examined rows can only fall.
        assert!(
            large_run.rows_examined <= small_run.rows_examined,
            "{name}: rows examined grew with the graph: {} -> {}",
            small_run.rows_examined,
            large_run.rows_examined
        );
    }
}

fn assert_query_bounds(query: &Query, size: &Size, measured: &Measured) {
    let name = query.name;
    let fan_out = size.fan_out;
    let layout = &size.layout;
    let paths = (query.paths_per_destination)(fan_out);

    // Correctness first: the answer is the construction rule's answer.
    assert_eq!(measured.ids.len(), LIMIT, "{name} fan-out {fan_out}");
    assert_eq!(
        measured.ids,
        expected_ids(paths),
        "{name} fan-out {fan_out}: wrong answer"
    );

    // Open reads controls, not payload.
    let open_bound = OPEN_CONTROL_BYTES_PER_NODE * NODES as u64 + CONTROL_SLACK_BYTES;
    assert!(
        measured.open_read <= open_bound,
        "{name} fan-out {fan_out}: open read {} bytes against a control bound of {open_bound}",
        measured.open_read
    );

    // Execution reads every node object once, at most one shard per hop, and
    // a bounded residual: and never an edge object.
    let execution_bound =
        layout.node_bytes + query.hops * layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES;
    assert!(
        measured.execution_read <= execution_bound,
        "{name} fan-out {fan_out}: execution read {} bytes against a structural bound of \
         {execution_bound} (nodes {}, {} shard(s) of at most {}, residual {EXECUTION_RESIDUAL_BYTES})",
        measured.execution_read,
        layout.node_bytes,
        query.hops,
        layout.largest_shard_bytes
    );
    // The bound must be tighter than the work it forbids, or it proves nothing.
    assert!(
        execution_bound < layout.edge_bytes,
        "{name} fan-out {fan_out}: the bound {execution_bound} admits a full edge scan \
         ({} edge bytes)",
        layout.edge_bytes
    );

    // Rows examined follow from the limit: each destination yields `paths`
    // rows, so `ceil(LIMIT / paths)` destinations satisfy it, plus the one
    // empty degree probe bulk construction's ordinal-from-one leaves in front.
    let rows_bound = (LIMIT.div_ceil(paths) + 1) as u64;
    assert!(
        (1..=rows_bound).contains(&measured.rows_examined),
        "{name} fan-out {fan_out}: {} adjacency rows examined against a bound of {rows_bound}",
        measured.rows_examined
    );
    assert!(
        measured.candidates <= LIMIT as u64 + measured.execution_batch_rows,
        "{name} fan-out {fan_out}: {} candidates generated for LIMIT {LIMIT}",
        measured.candidates
    );
}
