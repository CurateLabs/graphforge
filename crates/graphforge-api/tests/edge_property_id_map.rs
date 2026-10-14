//! A by-id edge property lookup reads the id pages of an index once per
//! generation, not once per call (#1984).
//!
//! A traversal that reads an edge property addresses the property index by
//! `edge_id`. Every call used to read the manifest and decode the id pages of
//! each shard it might hit, so a query issuing thousands of calls decoded the
//! same pages thousands of times. The provider now keeps, per route and
//! topology generation, a map from `edge_id` to the shard and row that holds
//! it, built once from the authenticated id pages. These tests count the pages
//! decoded and the maps built through the storage I/O counters, never time,
//! and check every value against the arithmetic that generated it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_storage::io_stats;

const WINDOW: usize = 4_096;
const MEMO_BYTES: usize = 32;
/// Three shards of the index: it holds at most 4096 rows each.
const EDGES: usize = 12_288;
/// Edge UUIDs are `edge_uuid(slot)`; the graph takes every second slot, so
/// the frontier below (every `STRIDE` slots) lands on edges of every shard.
const STEP: usize = 2;
const FRONTIER: usize = 16;
const STRIDE: usize = 1_500;
/// Relationships of another type, from node 0 to node 1, whose properties are
/// held by a second route.
const TWINS: usize = 8;
const SUM_TIMESTAMPS: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN sum(e.timestamp) AS n";
const LINK_ROWS: &str = "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) \
                         RETURN e.timestamp AS t, e.amount AS a, e.memo AS m, e.seen AS s";
const TWIN_ROWS: &str = "MATCH (a:Entity {ident: $ident})-[e:TWIN]->(b) \
                         RETURN e.timestamp AS t, e.amount AS a, e.memo AS m, e.seen AS s";
const OTHER_ROWS: &str = "MATCH (a:Entity {ident: $ident})-[e:OTHER]->(b) \
                          RETURN e.timestamp AS t, e.amount AS a, e.memo AS m, e.seen AS s";
const ANY_ROWS: &str = "MATCH (a:Entity {ident: $ident})-[e]->(b) \
                        RETURN e.timestamp AS t, e.amount AS a, e.memo AS m, e.seen AS s";

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(slot: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (slot as u128 + 1))
}

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

fn memo(index: usize) -> String {
    let mut text = String::with_capacity(MEMO_BYTES);
    let mut state = index as u64 + 1;
    while text.len() < MEMO_BYTES {
        state = mix(state);
        text.push_str(&format!("{state:016x}"));
    }
    text.truncate(MEMO_BYTES);
    text
}

/// Whether the edge at `slot` is one of node 0's frontier edges.
fn in_frontier(slot: usize) -> bool {
    slot % STRIDE == 0 && slot / STRIDE < FRONTIER
}

/// The slots of the frontier edges.
fn frontier_slots() -> impl Iterator<Item = usize> {
    (0..FRONTIER).map(|edge| edge * STRIDE)
}

/// The frontier edges leave node 0; every other edge leaves
/// node `1 + slot % (nodes - 1)`.
fn source_of(slot: usize) -> usize {
    if in_frontier(slot) {
        0
    } else {
        1 + slot % (EDGES - 1)
    }
}

fn timestamp_of(slot: usize) -> i64 {
    1_000 + 3 * slot as i64
}

fn amount_of(slot: usize) -> f64 {
    slot as f64 * 0.25
}

/// Some edges have no `amount`.
fn has_amount(slot: usize) -> bool {
    slot % 7 != 0
}

fn seen_of(slot: usize) -> bool {
    slot % 11 < 5
}

fn twin_timestamp(twin: usize) -> i64 {
    1_000_000 + twin as i64
}

/// Build a graph of `EDGES` `LINK` edges and as many nodes, with `twins`
/// further relationships of another type.
fn construct(dir: &Path, twins: usize) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("timestamp", DataType::Int64, true)));
    edge_fields.push(Arc::new(Field::new("amount", DataType::Float64, true)));
    edge_fields.push(Arc::new(Field::new("memo", DataType::Utf8, true)));
    edge_fields.push(Arc::new(Field::new("seen", DataType::Boolean, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..EDGES).step_by(WINDOW) {
        let end = (start + WINDOW).min(EDGES);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..EDGES).step_by(WINDOW) {
        let end = (start + WINDOW).min(EDGES);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut timestamps = Int64Builder::with_capacity(rows);
        let mut amounts = Float64Builder::with_capacity(rows);
        let mut memos = StringBuilder::new();
        let mut seen = Vec::with_capacity(rows);
        for edge in start..end {
            let slot = edge * STEP;
            let source = source_of(slot);
            ids.append_value(edge_uuid(slot).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + 1) % EDGES).as_bytes())
                .unwrap();
            timestamps.append_value(timestamp_of(slot));
            amounts.append_option(has_amount(slot).then(|| amount_of(slot)));
            memos.append_value(memo(slot));
            seen.push(seen_of(slot));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(timestamps.finish()),
                Arc::new(amounts.finish()),
                Arc::new(memos.finish()),
                Arc::new(BooleanArray::from(seen)),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    if twins > 0 {
        let mut ids = FixedSizeBinaryBuilder::with_capacity(twins, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(twins, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(twins, 16);
        let mut timestamps = Int64Builder::with_capacity(twins);
        let mut memos = StringBuilder::new();
        let mut seen = Vec::with_capacity(twins);
        for twin in 0..twins {
            ids.append_value(edge_uuid(STEP * EDGES + twin).as_bytes())
                .unwrap();
            sources.append_value(node_uuid(0).as_bytes()).unwrap();
            targets.append_value(node_uuid(1).as_bytes()).unwrap();
            timestamps.append_value(twin_timestamp(twin));
            memos.append_value(format!("twin-{twin}"));
            seen.push(twin % 2 == 0);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["TWIN"; twins])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(timestamps.finish()),
                // A twin has no `amount`.
                Arc::new(Float64Array::from(vec![None; twins])),
                Arc::new(memos.finish()),
                Arc::new(BooleanArray::from(seen)),
            ],
        )
        .unwrap();
        session.append_edges("twins", &batch).unwrap();
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

fn run(forge: &GraphForge, query: &str, ident: i64) -> Vec<RecordBatch> {
    forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes")
        .batches
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

/// One relationship's four properties.
type Row = (i64, Option<f64>, Option<String>, Option<bool>);

/// The rows of a `RETURN e.timestamp AS t, e.amount AS a, e.memo AS m,
/// e.seen AS s` statement, ordered by timestamp.
fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = |name: &str| batch.column_by_name(name).expect(name);
        let timestamps = column("t").as_any().downcast_ref::<Int64Array>().unwrap();
        let amounts = column("a").as_any().downcast_ref::<Float64Array>().unwrap();
        let memos = column("m").as_any().downcast_ref::<StringArray>().unwrap();
        let seen = column("s").as_any().downcast_ref::<BooleanArray>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                timestamps.value(row),
                amounts.is_valid(row).then(|| amounts.value(row)),
                memos.is_valid(row).then(|| memos.value(row).to_owned()),
                seen.is_valid(row).then(|| seen.value(row)),
            ));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

/// What node 0's `LINK` frontier holds, ordered by timestamp.
fn frontier_rows() -> Vec<Row> {
    let mut rows: Vec<Row> = frontier_slots()
        .map(|slot| {
            (
                timestamp_of(slot),
                has_amount(slot).then(|| amount_of(slot)),
                Some(memo(slot)),
                Some(seen_of(slot)),
            )
        })
        .collect();
    rows.sort_by_key(|row| row.0);
    rows
}

/// What node 0's `TWIN` relationships hold, ordered by timestamp.
fn twin_rows() -> Vec<Row> {
    (0..TWINS)
        .map(|twin| {
            (
                twin_timestamp(twin),
                None,
                Some(format!("twin-{twin}")),
                Some(twin % 2 == 0),
            )
        })
        .collect()
}

fn frontier_timestamps() -> i64 {
    frontier_slots().map(timestamp_of).sum()
}

/// The `timestamp` sum over the `LINK` edges leaving `ident`.
fn timestamps_of_node(ident: usize) -> i64 {
    (0..EDGES)
        .map(|edge| edge * STEP)
        .filter(|&slot| source_of(slot) == ident)
        .map(timestamp_of)
        .sum()
}

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

fn build(twins: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, twins);
    Built {
        _root: root,
        project,
    }
}

/// The decoded manifests of the project's edge property routes.
fn manifests(project: &Path) -> Vec<serde_json::Value> {
    let generation = graphforge_storage::resolve_project_generation(project).unwrap();
    let inventory = generation
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory");
    inventory
        .files
        .into_iter()
        .filter(|file| file.relative_path.ends_with(".epx"))
        .map(|entry| {
            let path = graphforge_storage::graph_object_path(
                generation.container_root(),
                &entry.content_sha256,
            )
            .unwrap();
            let bytes = std::fs::read(path).unwrap();
            assert_eq!(bytes[16], b'\n');
            serde_json::from_slice(&bytes[17..]).expect("manifest body is JSON")
        })
        .collect()
}

/// The id pages of every route: what building every map decodes.
fn id_pages(project: &Path) -> u64 {
    manifests(project)
        .iter()
        .map(|manifest| {
            let page_rows = manifest["page_rows"].as_u64().expect("page rows");
            manifest["shards"]
                .as_array()
                .unwrap()
                .iter()
                .map(|shard| shard["rows"].as_u64().unwrap().div_ceil(page_rows))
                .sum::<u64>()
        })
        .sum()
}

fn counters() -> io_stats::IoSnapshot {
    io_stats::snapshot().expect("requested I/O statistics")
}

/// The counters are recorded where the statement runs, so statements that
/// are counted run on one partition.
fn counted(project: &Path) -> (GraphForge, LifecycleIoCapture) {
    let forge = open(project, 1);
    let capture = LifecycleIoCapture::install();
    io_stats::reset();
    (forge, capture)
}

/// Repeated by-id lookups over a multi-shard index decode its id pages once:
/// the first statement of a generation builds the map from every id page, and
/// no statement after it decodes another, whatever the node it reads.
#[test]
fn repeated_lookups_decode_the_id_pages_once_per_generation() {
    let _serial = serial();
    let built = build(0);
    let shards = manifests(&built.project)[0]["shards"]
        .as_array()
        .unwrap()
        .len();
    assert!(shards >= 3, "the index must span shards: {shards}");
    let total_pages = id_pages(&built.project);
    let (forge, _capture) = counted(&built.project);

    assert_eq!(
        scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
        frontier_timestamps()
    );
    let first = counters();
    assert_eq!(first.edge_property_id_map_builds, 1, "{first:?}");
    assert_eq!(
        first.edge_property_id_pages_decoded, total_pages,
        "{first:?}"
    );
    assert!(first.edge_property_id_map_bytes > 0, "{first:?}");
    assert!(first.edge_property_id_map_hits >= 1, "{first:?}");
    assert_eq!(first.edge_property_id_map_declines, 0, "{first:?}");

    let mut previous = first;
    for ident in [0, 1, 2, 3, 4, 0] {
        let expected = if ident == 0 {
            frontier_timestamps()
        } else {
            timestamps_of_node(ident)
        };
        assert_eq!(
            scalar_i64(&run(&forge, SUM_TIMESTAMPS, ident as i64)),
            expected
        );
        let after = counters();
        assert_eq!(after.edge_property_id_map_builds, 1, "{after:?}");
        assert_eq!(
            after.edge_property_id_pages_decoded, total_pages,
            "node {ident} decoded id pages again: {after:?}"
        );
        assert!(
            after.edge_property_id_map_hits > previous.edge_property_id_map_hits,
            "node {ident} did not read through the map: {previous:?} -> {after:?}"
        );
        previous = after;
    }
}

/// A write drops every view the provider holds, the map among them. When the
/// index still describes the route the next statement builds a fresh map once,
/// and the statements after it share that one.
#[test]
fn a_write_rebuilds_the_map_once_for_the_new_generation() {
    let _serial = serial();
    let built = build(0);
    let total_pages = id_pages(&built.project);
    let (forge, _capture) = counted(&built.project);
    assert_eq!(
        scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
        frontier_timestamps()
    );
    assert_eq!(counters().edge_property_id_map_builds, 1);

    // A node changes neither the edge topology nor an edge property, so the
    // index keeps describing the route; only the provider's views are dropped.
    forge
        .execute("CREATE (:Entity {ident: 1000000})")
        .expect("the write executes");
    assert_eq!(
        manifests(&built.project)[0]["topology_digest"].is_string(),
        true,
        "the write must leave the ids published"
    );

    for _ in 0..3 {
        assert_eq!(
            scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
            frontier_timestamps()
        );
    }
    let rebuilt = counters();
    assert_eq!(rebuilt.edge_property_id_map_builds, 2, "{rebuilt:?}");
    assert_eq!(
        rebuilt.edge_property_id_pages_decoded,
        2 * total_pages,
        "a generation decodes the id pages once: {rebuilt:?}"
    );
}

/// A write to the topology leaves the index's ids describing edges that are no
/// longer the route's, so no map answers, and the map of the earlier
/// generation is not reused. A rebuild publishes ids for the new topology and
/// the next statement builds a map for it once.
#[test]
fn a_topology_write_is_answered_without_a_map_until_the_index_is_rebuilt() {
    let _serial = serial();
    let built = build(0);
    let total_pages = id_pages(&built.project);
    let (forge, _capture) = counted(&built.project);
    assert_eq!(
        scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
        frontier_timestamps()
    );
    let warm = counters();
    assert_eq!(warm.edge_property_id_map_builds, 1, "{warm:?}");

    forge
        .execute("MATCH (a:Entity {ident: 1}), (b:Entity {ident: 2}) CREATE (a)-[:LINK]->(b)")
        .expect("the write executes");
    for _ in 0..2 {
        assert_eq!(
            scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
            frontier_timestamps()
        );
    }
    let stale = counters();
    assert_eq!(
        stale.edge_property_id_map_builds, 1,
        "stale ids must not build a map: {stale:?}"
    );
    assert_eq!(
        stale.edge_property_id_map_hits, warm.edge_property_id_map_hits,
        "the earlier generation's map must not answer: {warm:?} -> {stale:?}"
    );

    let report = forge.rebuild_property_indexes(None).unwrap();
    assert!(report.edge_properties.indexes >= 1, "{report:?}");
    for _ in 0..3 {
        assert_eq!(
            scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
            frontier_timestamps()
        );
    }
    let rebuilt = counters();
    assert_eq!(rebuilt.edge_property_id_map_builds, 2, "{rebuilt:?}");
    assert_eq!(
        rebuilt.edge_property_id_pages_decoded,
        total_pages + id_pages(&built.project),
        "the rebuilt index's id pages are decoded once: {rebuilt:?}"
    );
    assert!(
        rebuilt.edge_property_id_map_hits > stale.edge_property_id_map_hits,
        "{stale:?} -> {rebuilt:?}"
    );
}

/// The map answers exactly what the shards did: every property of edges spread
/// over every shard, with the `NULL` amounts, at one partition and at four.
#[test]
fn lookups_return_the_stored_values_at_every_partition_count() {
    let _serial = serial();
    let built = build(0);
    for partitions in [1, 4] {
        let forge = open(&built.project, partitions);
        let capture = (partitions == 1).then(LifecycleIoCapture::install);
        io_stats::reset();
        // Twice: the second statement reads through the map the first built.
        for _ in 0..2 {
            let found = rows(&run(&forge, LINK_ROWS, 0));
            assert_eq!(found, frontier_rows(), "partitions={partitions}");
        }
        assert_eq!(
            scalar_i64(&run(&forge, SUM_TIMESTAMPS, 0)),
            frontier_timestamps()
        );
        // Another node's edge, not a frontier one.
        let found = rows(&run(&forge, LINK_ROWS, 3));
        assert_eq!(
            found.iter().map(|row| row.0).sum::<i64>(),
            timestamps_of_node(3),
            "partitions={partitions}"
        );
        for (timestamp, amount, memo_value, seen) in &found {
            let slot = usize::try_from((timestamp - 1_000) / 3).unwrap();
            assert_eq!(*amount, has_amount(slot).then(|| amount_of(slot)));
            assert_eq!(memo_value.as_deref(), Some(memo(slot).as_str()));
            assert_eq!(*seen, Some(seen_of(slot)));
        }
        if capture.is_some() {
            let after = counters();
            assert_eq!(after.edge_property_id_map_builds, 1, "{after:?}");
            assert!(after.edge_property_id_map_hits >= 4, "{after:?}");
        }
        assert!(
            frontier_rows().iter().any(|row| row.1.is_none()),
            "the frontier must hold a NULL amount"
        );
    }
}

/// Construction stores every relationship type's properties in one route, so
/// one map answers a hop of any type: relationships of another type read
/// their own values (and `NULL` for a property they never had), and a typed
/// hop does not read the other type's rows.
#[test]
fn one_map_answers_relationships_of_every_type() {
    let _serial = serial();
    let built = build(TWINS);
    assert_eq!(
        manifests(&built.project).len(),
        1,
        "one route for all types"
    );
    let mut both = frontier_rows();
    both.extend(twin_rows());
    both.sort_by_key(|row| row.0);
    for partitions in [1, 4] {
        let forge = open(&built.project, partitions);
        let capture = (partitions == 1).then(LifecycleIoCapture::install);
        io_stats::reset();
        for _ in 0..2 {
            assert_eq!(
                rows(&run(&forge, ANY_ROWS, 0)),
                both,
                "partitions={partitions}"
            );
        }
        assert_eq!(
            rows(&run(&forge, LINK_ROWS, 0)),
            frontier_rows(),
            "partitions={partitions}"
        );
        assert_eq!(
            rows(&run(&forge, TWIN_ROWS, 0)),
            twin_rows(),
            "partitions={partitions}"
        );
        if capture.is_some() {
            let after = counters();
            assert_eq!(
                after.edge_property_id_map_builds, 1,
                "the route builds its map once for all four statements: {after:?}"
            );
            assert_eq!(after.edge_property_id_map_declines, 0, "{after:?}");
        }
    }
}

/// A route a Cypher write created publishes no ids, so a hop that reads it is
/// answered through the relationships' public identities and builds no map for
/// it, while the construction route keeps serving its own hops from its map.
#[test]
fn a_route_without_ids_does_not_stop_another_route_using_its_map() {
    let _serial = serial();
    let built = build(0);
    let (forge, _capture) = counted(&built.project);
    forge
        .execute(
            "MATCH (a:Entity {ident: 1}), (b:Entity {ident: 2}) \
             CREATE (a)-[:OTHER {timestamp: 777, memo: 'cypher', seen: true}]->(b)",
        )
        .expect("the write executes");
    forge.rebuild_property_indexes(None).unwrap();
    let routes = manifests(&built.project);
    assert_eq!(routes.len(), 2, "the write adds a route: {routes:?}");
    assert_eq!(
        routes
            .iter()
            .filter(|route| route["topology_digest"].is_string())
            .count(),
        1,
        "only the construction route publishes ids"
    );

    for _ in 0..3 {
        assert_eq!(rows(&run(&forge, LINK_ROWS, 0)), frontier_rows());
    }
    let linked = counters();
    assert_eq!(linked.edge_property_id_map_builds, 1, "{linked:?}");
    assert!(linked.edge_property_id_map_hits > 0, "{linked:?}");

    for _ in 0..2 {
        assert_eq!(
            rows(&run(&forge, OTHER_ROWS, 1)),
            vec![(777, None, Some("cypher".to_owned()), Some(true))]
        );
    }
    let other = counters();
    assert_eq!(other.edge_property_id_map_builds, 1, "{other:?}");
    assert_eq!(other.edge_property_id_map_declines, 0, "{other:?}");
    assert_eq!(
        other.edge_property_id_pages_decoded, linked.edge_property_id_pages_decoded,
        "the route without ids decodes no id page: {linked:?} -> {other:?}"
    );
}
