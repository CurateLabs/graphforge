//! A full scan of the property overlay runs on several output partitions and
//! returns the rows the single-partition scan returns (#1984).
//!
//! The overlay scan used to expose one partition, so enriching every node of a
//! large route decoded on one thread. It now splits the route's UUID space into
//! disjoint ranges, one per partition, and each partition runs the same
//! newest-wins merge restricted to its range. The fixture builds a route of
//! several base fragments, then rewrites and deletes nodes spread across it, so
//! the rewrite and tombstone fragments span every range cut. Each read runs at
//! several partition targets; the physical plan must name the partition count
//! the session allows and the rows must equal what the mutations imply.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage, GraphConstructionBudgets,
    GraphForge, GraphForgeOptions,
};
use graphforge_core::uuid::Uuid;

const ENTITIES: usize = 4_000;
const PAYLOAD_BYTES: usize = 4_000;

/// Entities whose payload the first statement rewrites, one per quarter of the
/// route.
const REWRITTEN: [usize; 4] = [10, 1_500, 2_500, 3_999];
/// Entities the second statement deletes, in two further quarters.
const DELETED: [usize; 2] = [1_200, 3_500];

fn entity_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Incompressible and unique per node, so a fragment's size is its row count.
fn payload(node: usize) -> String {
    let mut state = (node as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(PAYLOAD_BYTES + 16);
    while out.len() < PAYLOAD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(PAYLOAD_BYTES);
    out
}

fn construct(dir: &Path) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, true)));
    let schema = Arc::new(Schema::new(fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    let mut ids = FixedSizeBinaryBuilder::with_capacity(ENTITIES, 16);
    let mut idents = Int64Builder::with_capacity(ENTITIES);
    for node in 0..ENTITIES {
        ids.append_value(entity_uuid(node).as_bytes()).unwrap();
        idents.append_value(node as i64);
    }
    let nodes = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; ENTITIES])),
            Arc::new(idents.finish()),
            Arc::new(StringArray::from(
                (0..ENTITIES).map(payload).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    session.append_nodes("entities", &nodes).unwrap();
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

/// Every entity with its payload, ordered by ident.
const SCAN: &str = "MATCH (n:Entity) RETURN n.ident AS ident, n.payload AS payload \
                    ORDER BY ident";

type Rows = Vec<(i64, String)>;

fn rows(forge: &GraphForge) -> Rows {
    let mut out = Vec::new();
    for batch in forge.execute(SCAN).expect("scan executes").batches {
        let idents = batch
            .column_by_name("ident")
            .expect("ident column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ident is Int64");
        let payloads = batch
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is Utf8");
        out.extend(
            (0..batch.num_rows()).map(|row| (idents.value(row), payloads.value(row).to_owned())),
        );
    }
    out
}

/// What the mutations leave: every entity but the deleted ones, with the
/// rewritten payloads.
fn expected() -> Rows {
    (0..ENTITIES)
        .filter(|node| !DELETED.contains(node))
        .map(|node| {
            let value = if REWRITTEN.contains(&node) {
                "changed".to_owned()
            } else {
                payload(node)
            };
            (node as i64, value)
        })
        .collect()
}

/// The number the physical plan gives the overlay scan's output partitions, or
/// `None` when it names none (one partition).
fn scan_partitions(forge: &GraphForge) -> Option<usize> {
    let plan = forge
        .explain_stage(SCAN, ExplainStage::PhysicalPlan)
        .expect("physical plan");
    let scans = plan
        .lines()
        .filter(|line| line.contains("PropertyOverlayExec"))
        .collect::<Vec<_>>();
    assert_eq!(
        scans.len(),
        1,
        "the scan plans exactly one overlay scan:\n{plan}"
    );
    scans[0]
        .split("partitions=")
        .nth(1)
        .map(|rest| rest.trim().parse().expect("a partition count"))
}

#[test]
fn a_partitioned_scan_returns_the_rows_the_mutations_leave() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    construct(&project);
    let writer = open(&project, 1);
    writer
        .execute(&format!(
            "MATCH (n:Entity) WHERE {} SET n.payload = 'changed'",
            REWRITTEN
                .iter()
                .map(|node| format!("n.ident = {node}"))
                .collect::<Vec<_>>()
                .join(" OR ")
        ))
        .expect("rewrite executes");
    writer
        .execute(&format!(
            "MATCH (n:Entity) WHERE {} DELETE n",
            DELETED
                .iter()
                .map(|node| format!("n.ident = {node}"))
                .collect::<Vec<_>>()
                .join(" OR ")
        ))
        .expect("delete executes");
    drop(writer);

    let expected = expected();
    assert_eq!(expected.len(), ENTITIES - DELETED.len());
    for target in [1, 2, 4, 8] {
        let forge = open(&project, target);
        assert_eq!(
            scan_partitions(&forge),
            (target > 1).then_some(target),
            "the scan exposes the session's partition target ({target})"
        );
        let found = rows(&forge);
        assert_eq!(
            found.len(),
            expected.len(),
            "{target} partitions: row count"
        );
        assert!(
            found == expected,
            "{target} partitions: the rows differ from what the mutations leave"
        );
    }
}
