//! End-to-end fixed-hop `LIMIT` scale gate and release benchmark (#1248).
//!
//! The CI test asserts structural I/O bounds through the public `GraphForge`
//! facade. The ignored release test reports 1M/10M-edge wall time without a
//! brittle timing threshold.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, StringArray,
    UInt64Array,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
    OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableVerifyRequest, verify_portable_v2,
};
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_exec::demand::{self, DemandSnapshot};
use graphforge_ir::IrLiteral;
use graphforge_storage::adjacency::build_adjacency_index;
use graphforge_storage::{
    GraphWriter, PortableV2Limits, PortableV2Mode, PortableV2Output, PortableV2SelectionProfile,
    io_stats,
};
use tempfile::TempDir;

#[path = "support/project_fixture.rs"]
mod project_fixture;

const TS: i64 = 1_700_000_000_000_000;
const NODE_TYPE: TypeId = TypeId(0);
const FAN_OUT: usize = 8;
const LIMIT: usize = 1_000;
const MAX_BATCH_ROWS: u64 = 8_192;
const WRITE_WINDOW: usize = 32 * 1024;

/// Serializes the process-global storage counters used by the assertions.
static IO_GUARD: Mutex<()> = Mutex::new(());

const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 1000";
const TWO_HOP: &str = "MATCH (a)-[r1]->(b)-[r2]->(c) \
                       RETURN c.node_uuid AS id LIMIT 1000";
const ORDERED_ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const ORDERED_TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";

fn measured_identity_query(forge: &GraphForge, query: &str) -> (Vec<Vec<u8>>, DemandSnapshot) {
    io_stats::reset();
    demand::reset();
    let result = forge.execute(query).unwrap();
    demand::disable();
    let io = io_stats::snapshot();
    assert_eq!(io.edge_full_reads + io.edge_filtered_reads, 0, "{io:#?}");
    assert_eq!(io.node_full_reads + io.node_filtered_reads, 0, "{io:#?}");
    let snapshot = demand::snapshot();
    assert_eq!(snapshot.sorts.len(), 1, "{snapshot:#?}");
    let sort = &snapshot.sorts[0];
    assert_eq!(sort.fetch, Some(LIMIT), "{snapshot:#?}");
    assert!(
        sort.output_rows <= (LIMIT + MAX_BATCH_ROWS as usize) as u64,
        "{snapshot:#?}"
    );
    assert_eq!(sort.retained_bytes, 0, "{snapshot:#?}");
    assert_eq!(snapshot.operator_rss.len(), 2, "{snapshot:#?}");
    assert!(
        snapshot.operator_rss.iter().all(|operator| {
            operator.peak_bytes >= operator.before_bytes
                && operator.peak_bytes >= operator.after_bytes
                && (operator.after_bytes > 0 || !cfg!(target_os = "linux"))
        }),
        "{snapshot:#?}"
    );
    assert!(
        snapshot.memory_reserved_after
            <= snapshot
                .memory_reserved_before
                .saturating_add(snapshot.returned_batch_bytes),
        "{snapshot:#?}"
    );
    assert_eq!(
        snapshot
            .hops
            .values()
            .filter(|hop| hop.identity_revalidation_calls > 0)
            .count(),
        1,
        "{snapshot:#?}"
    );
    assert!(
        snapshot
            .hops
            .values()
            .all(|hop| hop.identity_per_record_seeks == 0
                && hop.identity_peak_buffer_bytes <= 16 * 1024 * 1024
                && hop.identity_read_calls
                    <= hop
                        .identity_ranges_selected
                        .saturating_mul(2)
                        .saturating_add(2)),
        "{snapshot:#?}"
    );
    (fixed_binary_values(&result, "id"), snapshot)
}

/// Deterministic ring: each node points to its next `fan_out` successors.
fn generate_graph(dir: &Path, nodes: usize, fan_out: usize, compact_v4: bool) {
    if compact_v4 {
        generate_bulk_graph(dir, nodes, fan_out);
        return;
    }
    assert!(nodes > fan_out);
    let workspace = TempDir::new().unwrap();
    let uuids: Vec<Uuid> = (0..nodes).map(|_| new_v7()).collect();
    for node_window in uuids.chunks(WRITE_WINDOW) {
        let mut writer =
            GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
        for uuid in node_window {
            writer.create_node(*uuid, NODE_TYPE).unwrap();
        }
        writer.flush().unwrap();
    }
    let mut edges = Vec::with_capacity(nodes.saturating_mul(fan_out));
    for src in 0..nodes {
        for offset in 1..=fan_out {
            edges.push((uuids[src], uuids[(src + offset) % nodes]));
        }
    }
    for edge_window in edges.chunks(WRITE_WINDOW) {
        let mut writer =
            GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
        let endpoints = edge_window
            .iter()
            .flat_map(|(source, target)| [*source, *target])
            .collect::<Vec<_>>();
        writer.register_existing_endpoints(&endpoints).unwrap();
        for (source, target) in edge_window {
            writer
                .create_edge(new_v7(), "LINK", source, target)
                .unwrap();
        }
        writer.flush().unwrap();
    }
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir, workspace.path());
}

#[derive(Debug, PartialEq, Eq)]
struct BulkFixtureEvidence {
    node_rows: usize,
    edge_rows: usize,
    node_batches: usize,
    edge_batches: usize,
    accepted_chunks: u64,
    input_rows: u64,
    peak_batch_rows: u64,
}

/// Construct scale fixtures through the same bounded Arrow publication path
/// used by ordinary high-volume ingestion. Scalar `GraphWriter::create_edge`
/// deliberately checks its in-flight topology window for duplicate UUIDs and
/// is therefore not a realistic bulk-ingestion primitive.
fn generate_bulk_graph(dir: &Path, nodes: usize, fan_out: usize) -> BulkFixtureEvidence {
    assert!(nodes > fan_out);
    let forge = GraphForge::new(Some(dir.to_str().expect("temp path is UTF-8"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();

    let mut node_batches = 0;
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(nodes);
        let rows = end - start;
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for node in start..end {
            identities
                .append_value(fixture_node_uuid(node).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_NODE_SCHEMA),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; rows])),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
        node_batches += 1;
    }

    let edge_rows = nodes.saturating_mul(fan_out);
    let mut edge_batches = 0;
    for start in (0..edge_rows).step_by(WRITE_WINDOW) {
        let end = start.saturating_add(WRITE_WINDOW).min(edge_rows);
        let rows = end - start;
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            let offset = edge % fan_out + 1;
            identities
                .append_value(fixture_edge_uuid(edge).as_bytes())
                .unwrap();
            sources
                .append_value(fixture_node_uuid(source).as_bytes())
                .unwrap();
            targets
                .append_value(fixture_node_uuid((source + offset) % nodes).as_bytes())
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
        edge_batches += 1;
    }

    session.seal_and_publish().unwrap();
    let progress = session.progress();
    drop(session);
    forge.index_adjacency().unwrap();
    drop(forge);

    BulkFixtureEvidence {
        node_rows: nodes,
        edge_rows,
        node_batches,
        edge_batches,
        accepted_chunks: progress.accepted_chunks,
        input_rows: progress.evidence.input_rows,
        peak_batch_rows: progress.evidence.peak_batch_rows,
    }
}

fn fixture_node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1)
}

fn fixture_edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1)
}

fn open_forge(dir: &Path) -> GraphForge {
    GraphForge::new(Some(dir.to_str().expect("temp path is UTF-8"))).unwrap()
}

fn uint64_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<u64> {
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing {column} column"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("{column} must be UInt64"));
        values.extend(array.values().iter().copied());
    }
    values
}

fn fixed_binary_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing {column} column"))
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap_or_else(|| panic!("{column} must be FixedSizeBinary"));
        values.extend((0..batch.num_rows()).map(|row| array.value(row).to_vec()));
    }
    values
}

fn int64_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<i64> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("missing {column} column"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{column} must be Int64"))
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn string_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<String> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("missing {column} column"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("{column} must be Utf8"));
            (0..values.len())
                .map(|row| values.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn stable_fixture_uuid(kind: u8, ordinal: usize) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    Uuid::from_bytes(bytes)
}

fn generate_semantic_v4_graph(dir: &Path) -> Vec<Uuid> {
    let workspace = TempDir::new().unwrap();
    let nodes = (1..=3)
        .map(|ordinal| stable_fixture_uuid(3, ordinal))
        .collect::<Vec<_>>();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    for (node, name) in nodes.iter().zip(["A", "B", "C"]) {
        writer.create_node(*node, NODE_TYPE).unwrap();
        writer
            .set_properties(
                node,
                None,
                HashMap::from([("name".to_owned(), IrLiteral::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    for (ordinal, (source, destination, weight)) in
        [(0_usize, 1_usize, 1_i64), (0, 1, 2), (0, 0, 3), (1, 2, 4)]
            .into_iter()
            .enumerate()
    {
        let edge = stable_fixture_uuid(4, ordinal + 1);
        writer
            .create_edge(edge, "LINK", &nodes[source], &nodes[destination])
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("LINK"),
                HashMap::from([("weight".to_owned(), IrLiteral::Int(weight))]),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace_v4(dir, workspace.path());
    nodes
}

/// Build a graph whose first productive edge ids are localized but whose
/// destination node ids are evenly scattered through the node table. Target
/// UUIDs are stable across scales so the public results are directly
/// comparable even though their physical node ids move.
fn generate_scattered_destinations(
    dir: &Path,
    nodes: usize,
    fan_out: usize,
    targets: usize,
) -> usize {
    assert!(nodes > targets + fan_out);
    let workspace = TempDir::new().unwrap();
    let source = stable_fixture_uuid(1, 0);
    let target_uuids: Vec<Uuid> = (0..targets)
        .map(|ordinal| stable_fixture_uuid(2, ordinal + 1))
        .collect();
    let target_positions: Vec<usize> = (0..targets)
        .map(|ordinal| 1 + ordinal * (nodes - 1) / targets)
        .collect();
    let mut uuids = Vec::with_capacity(nodes);
    uuids.push(source);
    let mut target_cursor = 0usize;
    for position in 1..nodes {
        let uuid = if target_cursor < targets && target_positions[target_cursor] == position {
            let uuid = target_uuids[target_cursor];
            target_cursor += 1;
            uuid
        } else {
            new_v7()
        };
        uuids.push(uuid);
    }
    assert_eq!(target_cursor, targets);
    for nodes in uuids.chunks(WRITE_WINDOW) {
        let mut writer =
            GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
        for uuid in nodes {
            writer.create_node(*uuid, NODE_TYPE).unwrap();
        }
        writer.flush().unwrap();
    }
    let mut edges = target_uuids
        .iter()
        .map(|target| (source, *target))
        .collect::<Vec<_>>();
    for src in 1..nodes {
        for offset in 1..=fan_out {
            edges.push((uuids[src], uuids[(src + offset) % nodes]));
        }
    }
    for edge_window in edges.chunks(WRITE_WINDOW) {
        let mut writer =
            GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
        let endpoints = edge_window
            .iter()
            .flat_map(|(source, target)| [*source, *target])
            .collect::<Vec<_>>();
        writer.register_existing_endpoints(&endpoints).unwrap();
        for (source, target) in edge_window {
            writer
                .create_edge(new_v7(), "LINK", source, target)
                .unwrap();
        }
        writer.flush().unwrap();
    }
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir, workspace.path());
    targets + (nodes - 1) * fan_out
}

fn run_measured(
    forge: &GraphForge,
    query: &str,
) -> (Duration, io_stats::IoSnapshot, DemandSnapshot) {
    io_stats::reset();
    demand::reset();
    let started = Instant::now();
    let result = forge.execute(query).unwrap();
    let elapsed = started.elapsed();
    demand::disable();
    assert_eq!(result.stats.rows_produced, LIMIT as u64, "{query}");
    (elapsed, io_stats::snapshot(), demand::snapshot())
}

#[derive(Debug)]
struct ScaleResult {
    nodes: usize,
    edges: usize,
    one_hop: Duration,
    two_hop: Duration,
    one_hop_io: io_stats::IoSnapshot,
    two_hop_io: io_stats::IoSnapshot,
    one_hop_demand: DemandSnapshot,
    two_hop_demand: DemandSnapshot,
}

fn run_scale(nodes: usize, fan_out: usize) -> ScaleResult {
    let dir = TempDir::new().unwrap();
    generate_bulk_graph(dir.path(), nodes, fan_out);
    let forge = open_forge(dir.path());

    let one_plan = forge.explain(ONE_HOP).unwrap();
    assert!(one_plan.contains("ExpandExec"), "{one_plan}");
    assert!(one_plan.contains("adjacency=hit"), "{one_plan}");
    assert!(one_plan.contains("fetch=1000"), "{one_plan}");
    let two_plan = forge.explain(TWO_HOP).unwrap();
    assert_eq!(two_plan.matches("ExpandExec").count(), 2, "{two_plan}");
    assert!(two_plan.contains("fetch=1000"), "{two_plan}");
    assert!(two_plan.contains("demand_batch=1000"), "{two_plan}");
    assert!(two_plan.contains("cancel=guarded"), "{two_plan}");
    assert!(!two_plan.contains("RoundRobinBatch"), "{two_plan}");
    drop(forge);

    // Fresh facades keep the process-global compatibility counters attributable
    // to one query. Query-scoped demand counters additionally prove that all
    // started reads quiesce before result collection returns.
    let one_forge = open_forge(dir.path());
    let (one_hop, one_hop_io, one_hop_demand) = run_measured(&one_forge, ONE_HOP);
    drop(one_forge);
    let two_forge = open_forge(dir.path());
    let (two_hop, two_hop_io, two_hop_demand) = run_measured(&two_forge, TWO_HOP);
    drop(two_forge);

    ScaleResult {
        nodes,
        edges: nodes * fan_out,
        one_hop,
        two_hop,
        one_hop_io,
        two_hop_io,
        one_hop_demand,
        two_hop_demand,
    }
}

fn assert_indexed_limit_io(io: &io_stats::IoSnapshot) {
    // A terminal immutable shard with fewer than twice the demand rows may
    // legitimately take the filtered reader's >50% full-shard fallback. That
    // remains neighborhood-bounded; a full large shard does not.
    assert!(io.edge_full_reads <= 1, "{io:?}");
    assert!(io.edge_full_rows < (2 * LIMIT) as u64, "{io:?}");
    assert!(io.edge_filtered_reads >= 1, "{io:?}");
    assert!(io.edge_filtered_rows > 0, "{io:?}");
    assert_eq!(io.node_full_reads, 0, "{io:?}");
    assert!(io.node_filtered_reads >= 1, "{io:?}");
}

fn assert_projected_identity_io(io: &io_stats::IoSnapshot) {
    assert_eq!(io.edge_full_reads + io.edge_filtered_reads, 0, "{io:?}");
    assert_eq!(io.node_full_reads + io.node_filtered_reads, 0, "{io:?}");
}

fn assert_bounded_demand(snapshot: &DemandSnapshot, expected_hops: usize, required: u64) {
    assert_eq!(snapshot.hops.len(), expected_hops, "{snapshot:#?}");
    assert!(snapshot.cancellations >= 1, "{snapshot:#?}");
    assert!(snapshot.max_in_flight_reads <= 1, "{snapshot:#?}");
    for hop in snapshot.hops.values() {
        assert_eq!(hop.reads_after_cancel, 0, "{snapshot:#?}");
        assert_eq!(
            hop.edge_reads_started, hop.edge_reads_completed,
            "{snapshot:#?}"
        );
        assert_eq!(
            hop.node_reads_started, hop.node_reads_completed,
            "{snapshot:#?}"
        );
        assert_eq!(hop.edge_reads_failed, 0, "{snapshot:#?}");
        assert_eq!(hop.node_reads_failed, 0, "{snapshot:#?}");
        assert!(hop.edge_full_reads <= 1, "{snapshot:#?}");
        assert_eq!(hop.node_full_reads, 0, "{snapshot:#?}");
        assert!(
            hop.candidates_generated <= required + MAX_BATCH_ROWS,
            "required={required}, snapshot={snapshot:#?}"
        );
    }
}

#[test]
fn terminal_limit_keeps_fixed_hop_io_bounded_as_graph_grows() {
    let _guard = IO_GUARD.lock().unwrap();
    let small = run_scale(32_768, FAN_OUT);
    let large = run_scale(327_680, FAN_OUT);
    println!("fixed-hop LIMIT structural smoke: small={small:?}, large={large:?}");

    for scale in [&small, &large] {
        assert_projected_identity_io(&scale.one_hop_io);
        assert_projected_identity_io(&scale.two_hop_io);
        assert_bounded_demand(&scale.one_hop_demand, 1, LIMIT as u64);
        assert_bounded_demand(&scale.two_hop_demand, 2, LIMIT as u64);
    }

    // A 10x graph may move Parquet page boundaries, but must not create a
    // graph-proportional increase in rows materialized for the same LIMIT.
    assert!(
        large.one_hop_io.edge_filtered_rows <= small.one_hop_io.edge_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.two_hop_io.edge_filtered_rows <= small.two_hop_io.edge_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.one_hop_io.node_filtered_rows <= small.one_hop_io.node_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.two_hop_io.node_filtered_rows <= small.two_hop_io.node_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
}

#[test]
fn scale_fixture_uses_bounded_bulk_publications() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let nodes = WRITE_WINDOW + 1;
    let evidence = generate_bulk_graph(dir.path(), nodes, 2);
    assert_eq!(
        evidence,
        BulkFixtureEvidence {
            node_rows: nodes,
            edge_rows: nodes * 2,
            node_batches: 2,
            edge_batches: 3,
            accepted_chunks: 5,
            input_rows: (nodes * 3) as u64,
            peak_batch_rows: WRITE_WINDOW as u64,
        }
    );

    let forge = open_forge(dir.path());
    let plan = forge.explain(ORDERED_ONE_HOP).unwrap();
    assert!(plan.contains("adjacency=hit"), "{plan}");
    assert!(plan.contains("identity=v4"), "{plan}");
    io_stats::reset();
    let result = forge.execute(ORDERED_ONE_HOP).unwrap();
    assert_eq!(result.stats.rows_produced, LIMIT as u64);
    assert_projected_identity_io(&io_stats::snapshot());
}

fn run_scattered_destination_scale(
    nodes: usize,
) -> (
    Vec<Vec<u8>>,
    io_stats::IoSnapshot,
    DemandSnapshot,
    usize,
    u64,
) {
    let dir = TempDir::new().unwrap();
    let edges = generate_scattered_destinations(dir.path(), nodes, 4, 1_500);
    let forge = open_forge(dir.path());
    io_stats::reset();
    demand::reset();
    let result = forge.execute(ONE_HOP).unwrap();
    demand::disable();
    assert_eq!(result.stats.rows_produced, LIMIT as u64);
    let mut values = fixed_binary_values(&result, "id");
    values.sort_unstable();
    (
        values,
        io_stats::snapshot(),
        demand::snapshot(),
        edges,
        u64::try_from(nodes.div_ceil(WRITE_WINDOW)).unwrap(),
    )
}

#[test]
fn scattered_node_hydration_is_neighborhood_proportional() {
    let _guard = IO_GUARD.lock().unwrap();
    let (small_values, small_io, small_demand, small_edges, small_node_shards) =
        run_scattered_destination_scale(16_384);
    let (large_values, large_io, large_demand, large_edges, large_node_shards) =
        run_scattered_destination_scale(163_840);

    assert_eq!(small_values, large_values);
    assert!(
        large_edges >= small_edges * 9,
        "{small_edges} vs {large_edges}"
    );
    for (io, demand, node_shards) in [
        (&small_io, &small_demand, small_node_shards),
        (&large_io, &large_demand, large_node_shards),
    ] {
        assert_indexed_limit_io(io);
        assert!(io.node_filtered_reads <= node_shards, "{io:#?}");
        assert_eq!(
            io.node_dense_row_selection_reads, io.node_filtered_reads,
            "{io:#?}"
        );
        assert_eq!(io.node_row_group_predicate_reads, 0, "{io:#?}");
        assert_eq!(io.node_metadata_fallbacks, 0, "{io:#?}");
        assert_eq!(io.node_validation_fallbacks, 0, "{io:#?}");
        assert_eq!(io.node_scanned_rows, io.node_exact_rows_selected, "{io:#?}");
        assert_bounded_demand(demand, 1, LIMIT as u64);
        let hop = demand.hops.values().next().unwrap();
        assert_eq!(
            hop.node_dense_row_selection_reads, hop.node_reads_completed,
            "{demand:#?}"
        );
        assert_eq!(hop.node_row_group_predicate_reads, 0, "{demand:#?}");
        assert_eq!(hop.reads_after_cancel, 0, "{demand:#?}");
    }
    assert!(
        large_io.node_scanned_rows <= 3 * small_io.node_scanned_rows.max(1),
        "scattered node work must stay bounded across 10x graph growth: \
         small={small_io:#?}, large={large_io:#?}"
    );
}

fn run_ordered_projection_scale(nodes: usize) -> (Vec<Vec<u8>>, DemandSnapshot) {
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), nodes, FAN_OUT, true);
    let forge = open_forge(dir.path());
    let plan = forge.explain(ORDERED_ONE_HOP).unwrap();
    assert!(plan.contains("ExpandExec"), "{plan}");
    assert!(plan.contains("identity=v4"), "{plan}");
    assert!(plan.contains("SortExec"), "{plan}");
    assert!(plan.contains("projection=1"), "{plan}");

    io_stats::reset();
    demand::reset();
    let first = forge.execute(ORDERED_ONE_HOP).unwrap();
    demand::disable();
    let io = io_stats::snapshot();
    let snapshot = demand::snapshot();
    let first_values = fixed_binary_values(&first, "id");
    assert_eq!(first_values.len(), LIMIT);
    assert!(first_values.windows(2).all(|pair| pair[0] <= pair[1]));
    let node_scan = forge
        .execute("MATCH (b) RETURN b.node_uuid AS id ORDER BY id")
        .unwrap();
    let expected = fixed_binary_values(&node_scan, "id")
        .into_iter()
        .flat_map(|uuid| std::iter::repeat_n(uuid, FAN_OUT))
        .take(LIMIT)
        .collect::<Vec<_>>();
    assert_eq!(
        first_values, expected,
        "ordered fixed hop differs from scan oracle"
    );
    assert_eq!(io.edge_full_reads + io.edge_filtered_reads, 0, "{io:#?}");
    assert_eq!(io.node_full_reads + io.node_filtered_reads, 0, "{io:#?}");
    let hop = snapshot.hops.values().next().expect("one projected hop");
    assert!(hop.projected_chunks > 1, "{snapshot:#?}");
    assert_eq!(hop.projected_rows, (nodes * FAN_OUT) as u64);
    assert_eq!(hop.projected_columns, 1);
    assert_eq!(hop.identity_per_record_seeks, 0);
    assert!(hop.identity_read_calls > 0, "{snapshot:#?}");
    assert!(hop.identity_bytes_read > 0, "{snapshot:#?}");
    assert!(hop.identity_peak_buffer_bytes <= 16 * 1024 * 1024);

    let repeated = forge.execute(ORDERED_ONE_HOP).unwrap();
    assert_eq!(
        fixed_binary_values(&repeated, "id"),
        first_values,
        "ordered result changed across identical execution"
    );
    (first_values, snapshot)
}

#[test]
fn destination_uuid_projection_uses_authenticated_legacy_hydration_without_v4_authority() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 64, 4, false);
    let result = open_forge(dir.path()).execute(ORDERED_ONE_HOP).unwrap();
    let values = fixed_binary_values(&result, "id");
    assert!(!values.is_empty());
    assert!(
        values
            .iter()
            .all(|value| value.iter().any(|byte| *byte != 0))
    );
}

#[test]
fn ordered_destination_uuid_projection_is_exact_and_linear_at_1x_2x_4x() {
    let _guard = IO_GUARD.lock().unwrap();
    let mut work = Vec::new();
    for nodes in [4_096, 8_192, 16_384] {
        let (_, snapshot) = run_ordered_projection_scale(nodes);
        let hop = snapshot.hops.values().next().unwrap();
        work.push((
            hop.projected_rows,
            hop.identity_bytes_read,
            hop.identity_read_calls,
            hop.identity_revalidation_calls,
            snapshot.sorts[0].fetch,
            snapshot.sorts[0].retained_bytes,
        ));
    }
    for pair in work.windows(2) {
        let (prior_rows, prior_bytes, prior_calls, prior_revalidation, prior_fetch, prior_retained) =
            pair[0];
        let (next_rows, next_bytes, next_calls, next_revalidation, next_fetch, next_retained) =
            pair[1];
        assert_eq!(next_rows, prior_rows * 2, "{work:?}");
        // Fixed block/range boundaries may add one coalesced read, but neither
        // bytes nor calls may acquire a chunk-times-graph multiplier.
        assert!(next_bytes <= prior_bytes * 2 + 2 * 1024 * 1024, "{work:?}");
        assert!(next_calls <= prior_calls * 2 + 2, "{work:?}");
        assert_eq!((prior_fetch, next_fetch), (Some(LIMIT), Some(LIMIT)));
        assert_eq!((prior_retained, next_retained), (0, 0));
        assert!(
            next_revalidation <= prior_revalidation * 2 + 2,
            "session authentication must be linear in retained artifacts: {work:?}"
        );
    }
}

#[test]
fn portable_v2_clean_import_preserves_projected_ordered_hops_and_io() {
    let _guard = IO_GUARD.lock().unwrap();
    let root = TempDir::new().unwrap();
    let source_path = root.path().join("source");
    generate_graph(&source_path, 4_096, FAN_OUT, true);
    let source = open_forge(&source_path);
    let source_results =
        [ORDERED_ONE_HOP, ORDERED_TWO_HOP].map(|query| measured_identity_query(&source, query).0);

    let limits = PortableV2Limits::default();
    let package = root.path().join("project.gfpb");
    let exported = source
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    drop(source);
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(verified.package_digest, exported.package_digest);

    let imported_path = root.path().join("imported");
    GraphForge::import_portable_v2(
        &imported_path,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::from_u128(966)),
            limits,
        },
        None,
    )
    .unwrap();
    let imported = open_forge(&imported_path);
    for (query, expected) in [ORDERED_ONE_HOP, ORDERED_TWO_HOP]
        .into_iter()
        .zip(source_results)
    {
        assert_eq!(
            measured_identity_query(&imported, query).0,
            expected,
            "{query}"
        );
    }
}

#[test]
fn optimized_v4_two_hop_direction_type_alias_and_quiescence_are_exact() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let nodes = 4_096;
    generate_graph(dir.path(), nodes, FAN_OUT, true);
    let forge = open_forge(dir.path());
    let scan = forge
        .execute("MATCH (n) RETURN n.node_uuid AS id ORDER BY id")
        .unwrap();
    let node_ids = fixed_binary_values(&scan, "id");

    let cases = [
        (
            "MATCH (a)-[:LINK]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
            FAN_OUT,
            true,
            true,
        ),
        (
            "MATCH (a)<-[:LINK]-(b) RETURN a.node_uuid AS id ORDER BY id LIMIT 1000",
            FAN_OUT,
            true,
            false,
        ),
        (
            "MATCH (a)-[:LINK]-(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
            FAN_OUT * 2,
            false,
            true,
        ),
        (
            "MATCH (a)-[:LINK]->(b)-[:LINK]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000",
            FAN_OUT * FAN_OUT,
            true,
            true,
        ),
    ];
    for (query, multiplicity, identity_only, expects_v4_lookup) in cases {
        io_stats::reset();
        demand::reset();
        let result = forge.execute(query).unwrap();
        demand::disable();
        let expected = node_ids
            .iter()
            .flat_map(|uuid| std::iter::repeat_n(uuid.clone(), multiplicity))
            .take(LIMIT)
            .collect::<Vec<_>>();
        assert_eq!(fixed_binary_values(&result, "id"), expected, "{query}");
        let io = io_stats::snapshot();
        if identity_only {
            assert_eq!(
                io.edge_full_reads + io.edge_filtered_reads,
                0,
                "{query}: {io:#?}"
            );
            assert_eq!(
                io.node_full_reads + io.node_filtered_reads,
                0,
                "{query}: {io:#?}"
            );
        }
        let snapshot = demand::snapshot();
        assert!(
            snapshot
                .hops
                .values()
                .all(|hop| { hop.identity_per_record_seeks == 0 && hop.reads_after_cancel == 0 }),
            "{query}: {snapshot:#?}"
        );
        let pinned_hops = snapshot
            .hops
            .values()
            .map(|hop| hop.identity_revalidation_calls)
            .filter(|calls| *calls > 0)
            .count();
        assert_eq!(
            pinned_hops,
            usize::from(expects_v4_lookup),
            "a facade session pin is attributed exactly when destination identity lookup is required: {query}: {snapshot:#?}"
        );
    }

    let alias = forge
        .execute(
            "MATCH (left)-[:LINK]->(right) RETURN right.node_uuid AS renamed ORDER BY renamed LIMIT 1000",
        )
        .unwrap();
    let canonical = forge
        .execute("MATCH (a)-[:LINK]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000")
        .unwrap();
    assert_eq!(
        fixed_binary_values(&alias, "renamed"),
        fixed_binary_values(&canonical, "id")
    );

    let empty = forge
        .execute("MATCH (a)-[:MISSING]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000")
        .unwrap();
    assert_eq!(empty.stats.rows_produced, 0);
}

#[test]
fn optimized_v4_preserves_parallel_self_loop_and_demanded_property_semantics() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let nodes = generate_semantic_v4_graph(dir.path());
    let forge = open_forge(dir.path());

    let destination_query = "MATCH (a)-[:LINK]->(b) RETURN b.node_uuid AS id ORDER BY id";
    let destination_plan = forge.explain(destination_query).unwrap();
    assert!(
        destination_plan.contains("identity=v4"),
        "{destination_plan}"
    );
    assert!(
        destination_plan.contains("projection=1"),
        "{destination_plan}"
    );
    io_stats::reset();
    let destinations = forge.execute(destination_query).unwrap();
    let expected = [nodes[0], nodes[1], nodes[1], nodes[2]]
        .into_iter()
        .map(|uuid| uuid.as_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(fixed_binary_values(&destinations, "id"), expected);
    let io = io_stats::snapshot();
    assert_eq!(io.edge_full_reads + io.edge_filtered_reads, 0, "{io:#?}");
    assert_eq!(io.node_full_reads + io.node_filtered_reads, 0, "{io:#?}");

    let relationship = forge
        .execute("MATCH (a)-[r:LINK]->(b) RETURN r.weight AS weight ORDER BY weight")
        .unwrap();
    assert_eq!(int64_values(&relationship, "weight"), [1, 2, 3, 4]);

    let predicate = forge
        .execute("MATCH (a)-[r:LINK]->(b) WHERE r.weight >= 2 RETURN b.node_uuid AS id ORDER BY id")
        .unwrap();
    assert_eq!(
        fixed_binary_values(&predicate, "id"),
        [nodes[0], nodes[1], nodes[2]]
            .into_iter()
            .map(|uuid| uuid.as_bytes().to_vec())
            .collect::<Vec<_>>()
    );

    let node_property = forge
        .execute("MATCH (a)-[:LINK]->(b) RETURN b.name AS name ORDER BY name")
        .unwrap();
    assert_eq!(string_values(&node_property, "name"), ["A", "B", "B", "C"]);

    let undirected = forge
        .execute("MATCH (a)-[:LINK]-(b) RETURN b.node_uuid AS id ORDER BY id")
        .unwrap();
    let values = fixed_binary_values(&undirected, "id");
    // The undirected self-loop is emitted once, while the two parallel LINK
    // identities remain two distinct matches in each orientation.
    assert_eq!(values.len(), 7);
    assert_eq!(
        values
            .iter()
            .filter(|value| value.as_slice() == nodes[0].as_bytes())
            .count(),
        3
    );
    assert_eq!(
        values
            .iter()
            .filter(|value| value.as_slice() == nodes[1].as_bytes())
            .count(),
        3
    );
    assert_eq!(
        values
            .iter()
            .filter(|value| value.as_slice() == nodes[2].as_bytes())
            .count(),
        1
    );
}

#[test]
fn limits_sweep_bounded_multi_hop_work_and_repartition() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 4_096, FAN_OUT, false);
    let forge = open_forge(dir.path());

    for limit in [10_u64, 100, 1_000] {
        let query = format!("MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id LIMIT {limit}");
        let plan = forge.explain(&query).unwrap();
        assert_eq!(plan.matches("ExpandExec").count(), 2, "{plan}");
        assert!(
            plan.matches(&format!("demand_batch={limit}")).count() >= 2,
            "{plan}"
        );
        assert!(!plan.contains("RoundRobinBatch"), "{plan}");

        io_stats::reset();
        demand::reset();
        let result = forge.execute(&query).unwrap();
        demand::disable();
        assert_eq!(result.stats.rows_produced, limit);
        let io = io_stats::snapshot();
        assert_indexed_limit_io(&io);
        assert_bounded_demand(&demand::snapshot(), 2, limit);
    }
}

#[test]
fn selective_filter_tops_up_without_crossing_blockers() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 64, 4, false);
    let forge = open_forge(dir.path());

    let selective = "MATCH (a)-[r1]->(b)-[r2]->(c) \
                     WHERE c.node_id = 64 RETURN c.node_id AS id LIMIT 10";
    demand::reset();
    let result = forge.execute(selective).unwrap();
    demand::disable();
    assert_eq!(result.stats.rows_produced, 10);
    let snapshot = demand::snapshot();
    assert_bounded_demand(&snapshot, 2, 10);
    assert!(
        snapshot
            .filters
            .values()
            .any(|filter| filter.input_rows > filter.output_rows),
        "selective filter did not reject any candidates: {snapshot:#?}"
    );
    assert!(
        snapshot
            .hops
            .values()
            .any(|hop| hop.candidates_generated > 10),
        "selective query did not top up: {snapshot:#?}"
    );

    for query in [
        "MATCH ()-[r]->(b) RETURN b.node_id AS id ORDER BY id DESC LIMIT 5",
        "MATCH ()-[r]->(b) RETURN DISTINCT b.node_id AS id LIMIT 5",
        "MATCH ()-[r]->() RETURN count(r) AS total LIMIT 1",
    ] {
        let plan = forge.explain(query).unwrap();
        assert!(plan.contains("demand_batch=all"), "{plan}");
        assert!(plan.contains("cancel=none"), "{plan}");
        assert!(!plan.contains("DemandGuardExec"), "{plan}");
    }

    let unlimited = forge
        .explain("MATCH ()-[r1]->()-[r2]->() RETURN r1, r2")
        .unwrap();
    assert!(!unlimited.contains("DemandGuardExec"), "{unlimited}");
    assert!(unlimited.contains("demand_batch=all"), "{unlimited}");
    assert!(unlimited.contains("cancel=none"), "{unlimited}");
}

#[test]
fn high_degree_source_resumes_without_losing_neighbors() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let src = new_v7();
    let dst = new_v7();
    writer.create_node(src, NODE_TYPE).unwrap();
    writer.create_node(dst, NODE_TYPE).unwrap();
    for _ in 0..10_000 {
        writer.create_edge(new_v7(), "LINK", &src, &dst).unwrap();
    }
    writer.flush().unwrap();
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir.path(), workspace.path());

    let result = open_forge(dir.path())
        .execute("MATCH ()-[r]->() RETURN r.edge_uuid AS id")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 10_000);
    assert!(
        result.batches.iter().all(|batch| batch.num_rows() <= 8_192),
        "ExpandExec must honor the DataFusion output batch size"
    );
}

#[test]
fn fixed_hop_limit_preserves_skip_parameters_filters_and_blockers() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 64, 4, false);
    let forge = open_forge(dir.path());

    io_stats::reset();
    assert_eq!(
        forge
            .execute("MATCH ()-[r]->() RETURN r LIMIT 0")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let zero = io_stats::snapshot();
    assert_eq!(
        zero.edge_full_reads + zero.edge_filtered_reads,
        0,
        "{zero:?}"
    );

    assert_eq!(
        forge
            .execute("MATCH ()-[r]->() RETURN r SKIP 5 LIMIT 7")
            .unwrap()
            .stats
            .rows_produced,
        7
    );
    let params = HashMap::from([("n".to_owned(), IrLiteral::Int(9))]);
    assert_eq!(
        forge
            .execute_with_params("MATCH ()-[r]->() RETURN r LIMIT $n", &params)
            .unwrap()
            .stats
            .rows_produced,
        9
    );
    let filtered = forge
        .execute(
            "MATCH ()-[r]->(b) WHERE b.node_id >= 61 \
             RETURN b.node_id AS id LIMIT 11",
        )
        .unwrap();
    let filtered_ids = uint64_values(&filtered, "id");
    assert_eq!(filtered_ids.len(), 11);
    assert!(
        filtered_ids.iter().all(|id| (61..=64).contains(id)),
        "post-filter LIMIT returned unexpected IDs: {filtered_ids:?}"
    );

    let ordered = forge
        .execute(
            "MATCH ()-[r]->(b) RETURN b.node_id AS id \
             ORDER BY id DESC LIMIT 5",
        )
        .unwrap();
    assert_eq!(uint64_values(&ordered, "id"), [64, 64, 64, 64, 63]);

    let distinct = forge
        .execute(
            "MATCH ()-[r]->(b) RETURN DISTINCT b.node_id AS id \
             ORDER BY id ASC LIMIT 5",
        )
        .unwrap();
    assert_eq!(uint64_values(&distinct, "id"), [1, 2, 3, 4, 5]);

    let aggregate = forge
        .execute("MATCH ()-[r]->() RETURN count(r) AS total LIMIT 1")
        .unwrap();
    let total = aggregate.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 256, "aggregation must consume the complete hop");
}

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|e| panic!("invalid {key}={value:?}: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => panic!("{key} is not valid Unicode"),
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[derive(Debug)]
struct LiveJournalSample {
    elapsed: Duration,
    io: io_stats::IoSnapshot,
    demand: DemandSnapshot,
}

fn livejournal_query(hops: usize, limit: usize) -> String {
    match hops {
        1 => format!("MATCH ()-[]->() RETURN 1 AS x LIMIT {limit}"),
        2 => format!("MATCH ()-[]->()-[]->() RETURN 1 AS x LIMIT {limit}"),
        _ => unreachable!("benchmark covers one and two hops"),
    }
}

fn physical_plan_only(explain: &str) -> &str {
    explain
        .split_once("PhysicalPlan\n------------\n")
        .map_or(explain, |(_, physical)| physical)
}

fn livejournal_sample(forge: &GraphForge, query: &str, limit: usize) -> LiveJournalSample {
    io_stats::reset();
    demand::reset();
    let started = Instant::now();
    let result = forge
        .execute(query)
        .unwrap_or_else(|error| panic!("LiveJournal traversal execution failed: {error}"));
    let elapsed = started.elapsed();
    demand::disable();
    assert_eq!(result.stats.rows_produced, limit as u64);
    LiveJournalSample {
        elapsed,
        io: io_stats::snapshot(),
        demand: demand::snapshot(),
    }
}

#[test]
#[ignore = "release-only cached LiveJournal benchmark"]
fn release_livejournal_fixed_hop_limits() {
    assert!(
        !cfg!(debug_assertions),
        "run with cargo test --release; debug timings are not useful"
    );
    let _guard = IO_GUARD.lock().unwrap();
    let project = std::env::var_os("GF_LIVEJOURNAL_PROJECT")
        .expect("GF_LIVEJOURNAL_PROJECT must name the cached canonical fixture");
    let project = Path::new(&project);
    let project_str = project
        .to_str()
        .expect("GF_LIVEJOURNAL_PROJECT must be valid UTF-8");
    let forge = GraphForge::new(Some(project_str))
        .unwrap_or_else(|error| panic!("failed to open cached LiveJournal fixture: {error}"));

    for hops in [1, 2] {
        for limit in [10, 100, 1_000] {
            let query = livejournal_query(hops, limit);
            let explain = forge
                .explain(&query)
                .unwrap_or_else(|error| panic!("failed to plan LiveJournal traversal: {error}"));
            let plan = physical_plan_only(&explain);
            assert_eq!(plan.matches("ExpandExec").count(), hops, "{plan}");
            assert!(!plan.contains("RoundRobinBatch"), "{plan}");
            let cold = livejournal_sample(&forge, &query, limit);
            // One untimed warm-up precedes five independently captured samples.
            let warm = forge
                .execute(&query)
                .unwrap_or_else(|error| panic!("LiveJournal traversal warm-up failed: {error}"));
            assert_eq!(warm.stats.rows_produced, limit as u64);
            let mut samples: Vec<LiveJournalSample> = (0..5)
                .map(|_| livejournal_sample(&forge, &query, limit))
                .collect();
            samples.sort_by_key(|sample| sample.elapsed);
            let median = &samples[2];
            for sample in &samples {
                assert_indexed_limit_io(&sample.io);
                assert_bounded_demand(&sample.demand, hops, limit as u64);
                if hops == 1 && limit == 1_000 {
                    assert_eq!(sample.io.node_filtered_reads, 1, "{sample:#?}");
                    assert_eq!(sample.io.node_dense_row_selection_reads, 1, "{sample:#?}");
                    assert!(sample.io.node_scanned_rows <= 1_048_576, "{sample:#?}");
                }
                if hops == 2 && limit == 1_000 {
                    assert!(sample.io.edge_scanned_rows <= 2_949_120, "{sample:#?}");
                    assert!(sample.io.node_scanned_rows <= 11_612_672, "{sample:#?}");
                }
            }
            println!(
                "LIVEJOURNAL hops={hops} limit={limit} cold_ms={:.3} count=5 median_ms={:.3} range_ms={:.3}..{:.3} io={:?} demand={:?}\nPLAN_BEGIN\n{plan}\nPLAN_END",
                cold.elapsed.as_secs_f64() * 1_000.0,
                median.elapsed.as_secs_f64() * 1_000.0,
                samples[0].elapsed.as_secs_f64() * 1_000.0,
                samples[4].elapsed.as_secs_f64() * 1_000.0,
                median.io,
                median.demand,
            );
        }
    }
}

fn release_scale(nodes: usize, fan_out: usize) -> ScaleResult {
    let dir = TempDir::new().unwrap();
    generate_bulk_graph(dir.path(), nodes, fan_out);
    let warm = open_forge(dir.path());
    warm.execute(ONE_HOP).unwrap();
    warm.execute(TWO_HOP).unwrap();
    drop(warm);

    let mut one_samples = Vec::with_capacity(5);
    let mut two_samples = Vec::with_capacity(5);
    let mut one_hop_io = io_stats::IoSnapshot::default();
    let mut two_hop_io = io_stats::IoSnapshot::default();
    for _ in 0..5 {
        let forge = open_forge(dir.path());
        let (elapsed, io, _) = run_measured(&forge, ONE_HOP);
        one_samples.push(elapsed);
        one_hop_io = io;
        drop(forge);
    }
    for _ in 0..5 {
        let forge = open_forge(dir.path());
        let (elapsed, io, _) = run_measured(&forge, TWO_HOP);
        two_samples.push(elapsed);
        two_hop_io = io;
        drop(forge);
    }
    ScaleResult {
        nodes,
        edges: nodes * fan_out,
        one_hop: median(one_samples),
        two_hop: median(two_samples),
        one_hop_io,
        two_hop_io,
        one_hop_demand: DemandSnapshot::default(),
        two_hop_demand: DemandSnapshot::default(),
    }
}

#[test]
#[ignore = "release-only 1M/10M-edge benchmark"]
fn release_fixed_hop_limit_1m_10m() {
    assert!(
        !cfg!(debug_assertions),
        "run with cargo test --release; debug timings are not useful"
    );
    let _guard = IO_GUARD.lock().unwrap();
    let fan_out = env_usize("GF_FIXED_HOP_BENCH_FANOUT", 16);
    let small = release_scale(env_usize("GF_FIXED_HOP_BENCH_N1", 62_500), fan_out);
    let large = release_scale(env_usize("GF_FIXED_HOP_BENCH_N2", 625_000), fan_out);

    println!(
        "| scale | nodes | edges | one-hop ms | two-hop ms | one-hop edge rows | two-hop edge rows |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for (label, result) in [("S", small), ("M", large)] {
        println!(
            "| {label} | {} | {} | {:.2} | {:.2} | {} | {} |",
            result.nodes,
            result.edges,
            result.one_hop.as_secs_f64() * 1_000.0,
            result.two_hop.as_secs_f64() * 1_000.0,
            result.one_hop_io.edge_filtered_rows,
            result.two_hop_io.edge_filtered_rows,
        );
    }
}
