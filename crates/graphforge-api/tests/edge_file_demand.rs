//! An expansion reads nothing from the relation's edge file unless the edge
//! variable's identity or properties are demanded (#1984).
//!
//! The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`, so an
//! anchored expansion reaches the same edges however many nodes the graph has.
//! The gate is deterministic and counts the edge reads of the storage layer,
//! never time.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, Int64Builder,
    ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::io_stats;

const WRITE_WINDOW: usize = 4 * 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn construct(dir: &Path, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
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
    let edges = nodes * 2;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut weights = Int64Builder::with_capacity(rows);
        for edge in start..end {
            let source = edge / 2;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % 2 + 1) % nodes).as_bytes())
                .unwrap();
            weights.append_value(edge as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(weights.finish()),
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

#[derive(Debug)]
struct Measured {
    batches: Vec<RecordBatch>,
    read_bytes: u64,
    io: io_stats::IoSnapshot,
    /// What the expansion operators themselves report of their edge reads,
    /// summed over the hops: filtered reads started, rows returned, rows
    /// scanned, and edge columns materialised.
    hop_edge_work: [u64; 4],
}

impl Measured {
    /// The edge-file reads of every path: full, filtered, and the rows each
    /// decoded or scanned.
    fn edge_work(&self) -> [u64; 4] {
        [
            self.io.edge_full_reads,
            self.io.edge_filtered_reads,
            self.io.edge_filtered_rows,
            self.io.edge_scanned_rows,
        ]
    }

    fn int_column(&self, name: &str) -> Vec<i64> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"));
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{name} is Int64"));
            values.extend((0..column.len()).map(|row| column.value(row)));
        }
        values
    }
}

/// Runs the statement and reports its I/O.
///
/// The storage counters (`io`, `read_bytes`) are attributed through a
/// thread-local capture that DataFusion's partition workers do not carry, so
/// they are exact only at one target partition, where the statement runs on
/// the calling thread, and read zero beyond it. `hop_edge_work` comes from the
/// expansion operators' own process-wide counters and is exact at every
/// partition count, but only the single-hop expansion reports its edge reads
/// through them. Callers assert the storage counters at one partition only.
fn measured(forge: &GraphForge, query: &str) -> Measured {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let (result, demand) = graphforge_exec::demand::capture(|| forge.execute(query));
    let result = result.expect("statement executes");
    let hop_edge_work = demand.hops.values().fold([0_u64; 4], |sum, hop| {
        [
            sum[0] + hop.edge_reads_started,
            sum[1] + hop.edge_rows_returned,
            sum[2] + hop.edge_rows_scanned,
            sum[3] + hop.edge_projected_columns,
        ]
    });
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    Measured {
        batches: result.batches,
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        io: io_stats::snapshot().expect("requested I/O statistics"),
        hop_edge_work,
    }
}

/// The statement after one warm-up run, which admits the footers, the
/// adjacency index and the node topology once per session.
fn warm_measured(forge: &GraphForge, query: &str) -> Measured {
    let _ = measured(forge, query);
    measured(forge, query)
}

/// Both anchor on the node whose `ident` is 5: it links to nodes 6 and 7.
const ONE_HOP: &str = "MATCH (a:Entity {ident: 5})-[r:LINK]->(b:Entity)";
const VAR_LEN: &str = "MATCH (a:Entity {ident: 5})-[r:LINK*1..2]->(b:Entity)";
/// Two hops of one relationship type in one pattern: the binder adds a
/// relationship-uniqueness check between `r1` and `r2`, which compares their
/// `edge_id`s and nothing else.
const TWO_HOPS: &str = "MATCH (a:Entity {ident: 5})-[r1:LINK]->(b:Entity)-[r2:LINK]->(c:Entity)";
/// One hop under `OPTIONAL MATCH`: the optional sub-plan is joined to the outer
/// rows, and the demand on its columns must reach the expansion inside it.
const OPTIONAL_ONE_HOP: &str =
    "MATCH (a:Entity {ident: 5}) OPTIONAL MATCH (a)-[r:LINK]->(b:Entity)";
/// The same two hops under `OPTIONAL MATCH`.
const OPTIONAL_TWO_HOPS: &str =
    "MATCH (a:Entity {ident: 5}) OPTIONAL MATCH (a)-[r1:LINK]->(b:Entity)-[r2:LINK]->(c:Entity)";

struct Statement {
    query: String,
    column: &'static str,
    expected: Vec<i64>,
}

fn statement(query: String, column: &'static str, expected: &[i64]) -> Statement {
    Statement {
        query,
        column,
        expected: expected.to_vec(),
    }
}

/// Statements that never read `r`, with the values they must return.
fn unused_edge_statements() -> Vec<Statement> {
    vec![
        statement(format!("{ONE_HOP} RETURN count(b) AS n"), "n", &[2]),
        statement(
            format!("{ONE_HOP} RETURN b.ident AS v ORDER BY v"),
            "v",
            &[6, 7],
        ),
        statement(format!("{VAR_LEN} RETURN count(b) AS n"), "n", &[6]),
        statement(
            format!("{VAR_LEN} RETURN b.ident AS v ORDER BY v"),
            "v",
            &[6, 7, 7, 8, 8, 9],
        ),
        // Node 5 reaches 6 and 7, which reach 7, 8, 8 and 9. The uniqueness
        // check between `r1` and `r2` reads only their `edge_id`s, which the
        // adjacency holds.
        statement(format!("{TWO_HOPS} RETURN count(c) AS n"), "n", &[4]),
        statement(
            format!("{TWO_HOPS} RETURN c.ident AS v ORDER BY v"),
            "v",
            &[7, 8, 8, 9],
        ),
        statement(
            format!("{OPTIONAL_ONE_HOP} RETURN count(b) AS n"),
            "n",
            &[2],
        ),
        statement(
            format!("{OPTIONAL_TWO_HOPS} RETURN count(c) AS n"),
            "n",
            &[4],
        ),
    ]
}

/// Statements that read only a property of the edge variable. The edge-property
/// index addresses a bulk-built relation by the `edge_id` the adjacency already
/// holds, so the edge file is not read for them either (#1984).
fn ordinal_property_statements() -> Vec<Statement> {
    vec![
        statement(
            format!("{ONE_HOP} RETURN r.weight AS w ORDER BY w"),
            "w",
            &[10, 11],
        ),
        // `r2` is unused and `r1` is read only for its property, under the
        // uniqueness check between them.
        statement(
            format!("{TWO_HOPS} RETURN r1.weight AS w ORDER BY w"),
            "w",
            &[10, 10, 11, 11],
        ),
        statement(
            format!("{OPTIONAL_ONE_HOP} RETURN r.weight AS w ORDER BY w"),
            "w",
            &[10, 11],
        ),
    ]
}

/// Statements that read the edge variable's identity and so must read the edge
/// file. A one-hop `RETURN r` is asserted by
/// `a_returned_edge_variable_carries_the_stored_edge_uuids`.
fn demanded_edge_statements() -> Vec<Statement> {
    vec![statement(
        format!("{VAR_LEN} RETURN size(r) AS len ORDER BY len"),
        "len",
        &[1, 1, 2, 2, 2, 2],
    )]
}

/// An expansion whose edge variable is never read decodes nothing from the
/// edge file, however many edge files the relation spans, and neither does a
/// one-hop expansion that reads only an edge property, which the edge-property
/// index serves by edge id. Statements that read the edge variable's identity
/// decode the frontier's edges and no others.
///
/// The single-hop expansion's own counters are asserted at one and at four
/// partitions. The storage counters, which also cover the variable-length
/// expansion, are asserted at one partition, the only count at which they are
/// attributed (see `measured`).
#[test]
fn an_unused_edge_variable_reads_no_edge_file_as_the_relation_doubles() {
    let _serial = serial();
    let mut demanded_rows: HashMap<(usize, String), Vec<[u64; 2]>> = HashMap::new();
    for nodes in [SMALL_NODES, LARGE_NODES] {
        let built = build(nodes);
        for partitions in [1, 4] {
            let forge = open(&built.project, partitions);
            for Statement {
                query,
                column,
                expected,
            } in unused_edge_statements()
            {
                let plan = forge.explain(&query).unwrap();
                assert!(plan.contains("Expand"), "{plan}");
                let run = warm_measured(&forge, &query);
                assert_eq!(
                    run.int_column(column),
                    expected,
                    "{nodes} nodes, {partitions} partitions: {query}"
                );
                assert_eq!(
                    run.hop_edge_work, [0; 4],
                    "{nodes} nodes, {partitions} partitions: {query}"
                );
                if partitions == 1 {
                    assert_eq!(
                        run.edge_work(),
                        [0; 4],
                        "{nodes} nodes: {query}\n{:?}",
                        run.io
                    );
                }
            }
            for Statement {
                query,
                column,
                expected,
            } in ordinal_property_statements()
            {
                let plan = forge.explain(&query).unwrap();
                assert!(plan.contains("Expand"), "{plan}");
                let run = warm_measured(&forge, &query);
                assert_eq!(
                    run.int_column(column),
                    expected,
                    "{nodes} nodes, {partitions} partitions: {query}"
                );
                assert_eq!(
                    run.hop_edge_work, [0; 4],
                    "an edge property keyed by edge id reads no edge file: \
                     {nodes} nodes, {partitions} partitions: {query}"
                );
                if partitions == 1 {
                    assert_eq!(
                        run.edge_work(),
                        [0; 4],
                        "{nodes} nodes: {query}\n{:?}",
                        run.io
                    );
                }
            }
            for Statement {
                query,
                column,
                expected,
            } in demanded_edge_statements()
            {
                let run = warm_measured(&forge, &query);
                assert_eq!(
                    run.int_column(column),
                    expected,
                    "{nodes} nodes, {partitions} partitions: {query}"
                );
                if partitions == 1 {
                    assert!(
                        run.io.edge_filtered_rows > 0,
                        "a demanded edge variable reads edge rows: {query}\n{:?}",
                        run.io
                    );
                    assert_eq!(run.io.edge_full_reads, 0, "{query}");
                }
                demanded_rows
                    .entry((partitions, query))
                    .or_default()
                    .push([run.io.edge_filtered_rows, run.hop_edge_work[1]]);
            }
        }
    }
    for ((partitions, query), rows) in demanded_rows {
        assert_eq!(rows.len(), 2, "{query}");
        assert_eq!(
            rows[0], rows[1],
            "{partitions} partitions: edge rows read grew with the relation: {query}"
        );
    }
}

/// Reading no edge file saves its bytes: the same expansion reads strictly
/// fewer bytes when the edge variable is unused than when it is demanded.
#[test]
fn an_unused_edge_variable_saves_the_edge_file_bytes() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project, 1);
    for (unused, demanded) in [
        (
            format!("{ONE_HOP} RETURN count(b) AS n"),
            format!("{ONE_HOP} RETURN count(r.weight) AS n"),
        ),
        (
            format!("{VAR_LEN} RETURN count(b) AS n"),
            format!("{VAR_LEN} RETURN count(size(r)) AS n"),
        ),
    ] {
        let without = warm_measured(&forge, &unused);
        let with = warm_measured(&forge, &demanded);
        assert_eq!(without.io.node_full_reads, with.io.node_full_reads);
        assert!(
            without.read_bytes < with.read_bytes,
            "{unused}: {} bytes without the edge variable, {} with it",
            without.read_bytes,
            with.read_bytes
        );
    }
}

fn edge_uuid_bytes(index: usize) -> [u8; 16] {
    *edge_uuid(index).as_bytes()
}

fn struct_edge_uuid(structs: &StructArray, row: usize) -> [u8; 16] {
    let ids = structs
        .column_by_name("edge_uuid")
        .expect("edge_uuid field")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("edge_uuid is FixedSizeBinary");
    ids.value(row).try_into().expect("16 bytes")
}

/// `RETURN r` still returns the stored edge identities, one hop and
/// variable length, at one and at four partitions.
#[test]
fn a_returned_edge_variable_carries_the_stored_edge_uuids() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    for partitions in [1, 4] {
        let forge = open(&built.project, partitions);
        let one_hop = measured(&forge, &format!("{ONE_HOP} RETURN r"));
        let mut ids = Vec::new();
        let mut weights = Vec::new();
        for batch in &one_hop.batches {
            let edges = batch
                .column_by_name("r")
                .expect("r column")
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("r is a struct");
            let weight = edges
                .column_by_name("weight")
                .expect("weight field")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("weight is Int64");
            for row in 0..edges.len() {
                ids.push(struct_edge_uuid(edges, row));
                weights.push(weight.value(row));
            }
        }
        ids.sort();
        weights.sort();
        assert_eq!(
            ids,
            vec![edge_uuid_bytes(10), edge_uuid_bytes(11)],
            "{partitions} partitions"
        );
        assert_eq!(weights, vec![10, 11]);
        assert!(one_hop.hop_edge_work[1] > 0, "{:?}", one_hop.hop_edge_work);
        if partitions == 1 {
            assert!(one_hop.io.edge_filtered_rows > 0, "{:?}", one_hop.io);
        }

        let var_len = measured(&forge, &format!("{VAR_LEN} RETURN r"));
        let mut paths: Vec<Vec<[u8; 16]>> = Vec::new();
        for batch in &var_len.batches {
            let lists = batch
                .column_by_name("r")
                .expect("r column")
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("r is a list");
            for row in 0..lists.len() {
                assert!(lists.is_valid(row), "a path has a list");
                let entry = lists.value(row);
                let structs = entry
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("list of structs");
                paths.push(
                    (0..structs.len())
                        .map(|i| struct_edge_uuid(structs, i))
                        .collect(),
                );
            }
        }
        paths.sort();
        let expected: Vec<Vec<[u8; 16]>> = [
            vec![10],
            vec![10, 12],
            vec![10, 13],
            vec![11],
            vec![11, 14],
            vec![11, 15],
        ]
        .into_iter()
        .map(|path: Vec<usize>| path.into_iter().map(edge_uuid_bytes).collect())
        .collect();
        assert_eq!(paths, expected, "{partitions} partitions");
        if partitions == 1 {
            assert!(var_len.io.edge_filtered_rows > 0, "{:?}", var_len.io);
        }
    }
}
