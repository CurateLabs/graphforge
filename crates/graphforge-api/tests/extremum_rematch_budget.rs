//! The extremum re-match (#1982) retains one entry per first-stage row.
//! The query pool rejects a group that cannot fit, naming the aggregate.
//! Reservations are checked after each batch; the pool does not cap every
//! transient allocation or process resident memory. Measure a generous resident
//! growth bound as well as the pool error, and check the same group succeeds
//! when the rewrite declines and the stages stream.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, ResourcePolicyMode,
};
use graphforge_core::uuid::Uuid;

const MIB: u64 = 1024 * 1024;
/// The smallest pool the facade admits.
const POOL: u64 = 16 * MIB;
/// First-stage rows in the one group: at a few hundred bytes each, collected
/// they are several times the pool.
const LIKES: i64 = 50_000;
/// Rows per constructed batch.
const WRITE_WINDOW: usize = 4 * 1024;
/// Labelled bound: resident growth during the query stays within this multiple
/// of the pool. The pool charges the collected rows; the rest of what the
/// query touches is a small constant.
const RSS_GROWTH_MULTIPLE: u64 = 8;

const REWRITTEN: &str =
    "MATCH (person:Person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
     WHERE (message:Post OR message:Comment)
     WITH person, liker, max(like.creationDate) AS likeTime
     MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)
     WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime
     WITH person, liker, likeTime, min(message.id) AS latestMessageId
     RETURN person.id AS personId, liker.id AS likerId, likeTime, latestMessageId";

/// The same query with one more conjunct in the second stage: not the
/// extremum match, so the pass declines and both stages stream.
const STREAMED: &str =
    "MATCH (person:Person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
     WHERE (message:Post OR message:Comment)
     WITH person, liker, max(like.creationDate) AS likeTime
     MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)
     WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime
       AND like.creationDate IS NOT NULL
     WITH person, liker, likeTime, min(message.id) AS latestMessageId
     RETURN person.id AS personId, liker.id AS likerId, likeTime, latestMessageId";

fn resident_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .expect("Linux reports resident memory")
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    kib * 1024
}

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// One creator (`Person` 1), one liker (`Person` 10), one `Post` (100) created
/// by the creator, and `LIKES` parallel `LIKES` edges between the liker and the
/// post, each with its own `creationDate`: the liker's group holds `LIKES`
/// first-stage rows, and the latest like is at `LIKES`.
///
/// Built in bulk, because a statement that creates the edges from an unwound
/// list cross joins the list with the anchors it matches, and under the smallest
/// pool that join alone exhausts the memory the test means to measure.
fn construct(dir: &Path) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("id", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("creationDate", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));

    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();

    // Node 0 is the creator, node 1 the liker, node 2 the post.
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(3, 16);
    for node in 0..3 {
        uuids.append_value(node_uuid(node).as_bytes()).unwrap();
    }
    let nodes = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(uuids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Person", "Person", "Post"])),
            Arc::new(Int64Array::from(vec![1, 10, 100])),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &nodes).unwrap();

    // Edge 0 is HAS_CREATOR (post to creator); edges 1..=LIKES are LIKES
    // (liker to post), edge `n` created at `n`.
    let edges = LIKES as usize + 1;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut created = Int64Builder::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        for edge in start..end {
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            if edge == 0 {
                sources.append_value(node_uuid(2).as_bytes()).unwrap();
                targets.append_value(node_uuid(0).as_bytes()).unwrap();
                created.append_null();
                types.push("HAS_CREATOR");
            } else {
                sources.append_value(node_uuid(1).as_bytes()).unwrap();
                targets.append_value(node_uuid(2).as_bytes()).unwrap();
                created.append_value(edge as i64);
                types.push("LIKES");
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(types)),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(created.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
}

fn build() -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project);
    Built {
        _root: root,
        project,
    }
}

fn open(built: &Built, resource: ExecutionResourcePolicy) -> GraphForge {
    GraphForge::new_with_options(
        Some(built.project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource,
            ..GraphForgeOptions::default()
        },
    )
    .expect("project opens")
}

/// Resident memory is process-wide, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn pooled() -> ExecutionResourcePolicy {
    ExecutionResourcePolicy {
        mode: ResourcePolicyMode::Explicit,
        memory_budget_bytes: Some(POOL),
        tokio_worker_threads: Some(2),
        target_partitions: Some(1),
        io_concurrency: Some(1),
        compute_threads: Some(1),
        ..ExecutionResourcePolicy::default()
    }
}

fn only_row(forge: &GraphForge, query: &str) -> Vec<i64> {
    let result = forge
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
                        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        assert!(!values.is_null(row));
                        values.value(row)
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    assert_eq!(rows.len(), 1, "{rows:?}");
    rows.remove(0)
}

fn rematches(forge: &GraphForge, query: &str) -> bool {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap()
        .contains("UnwindExec: list=_extremum_rematch_")
}

#[test]
fn a_group_that_fits_the_default_pool_is_answered_from_the_first_stages_rows() {
    let _serial = serial();
    let built = build();
    let forge = open(&built, ExecutionResourcePolicy::default());
    assert!(rematches(&forge, REWRITTEN));
    assert_eq!(only_row(&forge, REWRITTEN), [1, 10, LIKES, 100]);
}

#[test]
fn a_group_too_large_for_the_pool_fails_typed_with_bounded_resident_growth() {
    let _serial = serial();
    let built = build();
    let forge = open(&built, pooled());
    assert!(rematches(&forge, REWRITTEN));

    let baseline = resident_bytes();
    let peak = Arc::new(AtomicU64::new(baseline));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (Arc::clone(&peak), Arc::clone(&done));
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(resident_bytes(), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let outcome = forge.execute(REWRITTEN);
    done.store(true, Ordering::Relaxed);
    sampler.join().unwrap();

    let growth = peak.load(Ordering::Relaxed).saturating_sub(baseline);
    let error = outcome.expect_err("one group of 50,000 rows cannot fit 16 MiB");
    assert_eq!(error.code(), "GF_EXECUTION");
    let message = error.to_string();
    assert!(message.contains("Resources exhausted"), "{message}");
    let primary = message.lines().next().expect("execution error diagnostic");
    assert!(
        primary.contains("allocation failed for GroupedHashAggregateStream")
            && primary.contains("cypher_collect"),
        "the aggregate itself must exhaust its reservation, not merely appear among other consumers: {message}"
    );
    assert!(
        growth <= RSS_GROWTH_MULTIPLE * POOL,
        "resident growth {growth} bytes exceeds {RSS_GROWTH_MULTIPLE} x the {POOL} byte pool: {message}"
    );
}

#[test]
fn the_same_group_streams_through_the_pool_when_the_pass_declines() {
    let _serial = serial();
    let built = build();
    let forge = open(&built, pooled());
    assert!(!rematches(&forge, STREAMED));
    assert_eq!(only_row(&forge, STREAMED), [1, 10, LIKES, 100]);
}
