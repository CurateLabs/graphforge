//! Barrier and failpoint coverage for `add_edge` visibility coordination (#704).

use std::collections::{BTreeSet, HashMap};
use std::process::Command;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array};
use uuid::Uuid;

use crate::concurrency_test_support::hold_writer;
use crate::{
    CheckpointRequest, EdgeHandle, GfError, GraphForge, NodeHandle, OperationId, PropValue,
};

const DEADLINE: Duration = Duration::from_secs(10);
const FAILPOINT_COOKIE: &str = "graphforge-internal-subprocess-v1";
const EDGE_QUERY: &str = "MATCH ()-[r:KNOWS]->() RETURN r.edge_uuid AS edge, r.edge_id AS id \
     ORDER BY r.edge_id";

fn empty_props() -> HashMap<String, PropValue> {
    HashMap::new()
}

fn seed_pair(graph: &GraphForge) -> (NodeHandle, NodeHandle) {
    let src = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("A".into()))]),
        )
        .expect("phase=seed source");
    let dst = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("B".into()))]),
        )
        .expect("phase=seed destination");
    (src, dst)
}

fn edge_inventory(graph: &GraphForge) -> Result<(Vec<[u8; 16]>, Vec<u64>), GfError> {
    let result = graph.execute(EDGE_QUERY)?;
    let mut uuids = Vec::new();
    let mut ids = Vec::new();
    for batch in result.batches {
        let edge = batch
            .column_by_name("edge")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Execution("edge uuid column malformed".into()))?;
        let id = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Execution("edge id column malformed".into()))?;
        for row in 0..batch.num_rows() {
            uuids.push(
                edge.value(row)
                    .try_into()
                    .map_err(|_| GfError::Execution("malformed edge uuid".into()))?,
            );
            ids.push(id.value(row));
        }
    }
    Ok((uuids, ids))
}

fn generation(graph: &GraphForge) -> Uuid {
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned")
}

fn durable_generation(root: &std::path::Path) -> Uuid {
    graphforge_storage::resolve_project_generation(root)
        .expect("resolve durable generation")
        .generation_uuid()
}

fn recv_pair<T>(receiver: &mpsc::Receiver<(usize, T)>, phase: &str) -> [(usize, T); 2] {
    let deadline = Instant::now() + DEADLINE;
    let first = receiver
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .unwrap_or_else(|error| panic!("phase={phase} first timeout={DEADLINE:?} {error}"));
    let second = receiver
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .unwrap_or_else(|error| panic!("phase={phase} second timeout={DEADLINE:?} {error}"));
    [first, second]
}

#[test]
fn concurrent_add_edge_calls_publish_distinct_edges() {
    let project = tempfile::tempdir().expect("phase=add-edge/add-edge tempdir");
    let graph = Arc::new(GraphForge::new(project.path().to_str()).expect("open"));
    let (src, dst) = seed_pair(&graph);
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    let workers = (0..2)
        .map(|worker| {
            let graph = Arc::clone(&graph);
            let src = src.clone();
            let dst = dst.clone();
            let barrier = Arc::clone(&barrier);
            let sender = sender.clone();
            thread::Builder::new()
                .name(format!("gf-add-edge-{worker}"))
                .spawn(move || {
                    barrier.wait();
                    let outcome = graph
                        .add_edge(&src, "KNOWS", &dst, &empty_props())
                        .map(|edge| edge.uuid.into_bytes());
                    let _ = sender.send((worker, outcome));
                })
                .expect("spawn add_edge worker")
        })
        .collect::<Vec<_>>();
    drop(sender);
    let mut outcomes = recv_pair(&receiver, "add-edge/add-edge");
    for handle in workers {
        handle.join().expect("add_edge worker panicked");
    }
    outcomes.sort_unstable_by_key(|(worker, _)| *worker);
    let left = outcomes[0]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("phase=add-edge/add-edge worker=0 failed: {error}"));
    let right = outcomes[1]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("phase=add-edge/add-edge worker=1 failed: {error}"));
    assert_ne!(left, right, "overlapping edge UUIDs");

    let (uuids, ids) = edge_inventory(&graph).expect("inventory");
    assert_eq!(uuids.len(), 2);
    assert_eq!(BTreeSet::from_iter(uuids.iter().copied()).len(), 2);
    assert_eq!(BTreeSet::from_iter(ids.iter().copied()).len(), 2);
    assert!(uuids.contains(left));
    assert!(uuids.contains(right));

    let reopened = GraphForge::new(project.path().to_str()).expect("reopen");
    let (reopened_uuids, reopened_ids) = edge_inventory(&reopened).expect("reopen inventory");
    assert_eq!(reopened_uuids, uuids);
    assert_eq!(reopened_ids, ids);
}

#[test]
fn concurrent_add_edge_and_cypher_write_serialize() {
    let project = tempfile::tempdir().expect("phase=add-edge/cypher tempdir");
    let graph = Arc::new(GraphForge::new(project.path().to_str()).expect("open"));
    let (src, dst) = seed_pair(&graph);
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    let add_edge_worker = {
        let graph = Arc::clone(&graph);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::Builder::new()
            .name("gf-add-edge-vs-cypher".into())
            .spawn(move || {
                barrier.wait();
                let outcome = graph
                    .add_edge(&src, "KNOWS", &dst, &empty_props())
                    .map(|edge: EdgeHandle| edge.uuid.into_bytes());
                let _ = sender.send((0, outcome.map(|_| "add_edge".to_owned())));
            })
            .expect("spawn add_edge")
    };
    let cypher_worker = {
        let graph = Arc::clone(&graph);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::Builder::new()
            .name("gf-cypher-vs-add-edge".into())
            .spawn(move || {
                barrier.wait();
                let outcome = graph
                    .execute("CREATE (:Person {name:'Cypher'})")
                    .map(|_| "cypher".to_owned());
                let _ = sender.send((1, outcome));
            })
            .expect("spawn cypher")
    };
    drop(sender);
    let outcomes = recv_pair(&receiver, "add-edge/cypher");
    add_edge_worker.join().expect("add_edge panicked");
    cypher_worker.join().expect("cypher panicked");
    for (worker, result) in &outcomes {
        result
            .as_ref()
            .unwrap_or_else(|error| panic!("phase=add-edge/cypher worker={worker} {error}"));
    }

    let (uuids, ids) = edge_inventory(&graph).expect("edge inventory");
    assert_eq!(uuids.len(), 1);
    assert_eq!(ids.len(), 1);
    let people = graph
        .execute("MATCH (n:Person) RETURN count(n) AS total")
        .expect("count people");
    let total = people.batches[0]
        .column_by_name("total")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .expect("count column")
        .value(0);
    assert_eq!(total, 3);
}

#[test]
fn concurrent_readers_observe_only_committed_generations() {
    let project = tempfile::tempdir().expect("phase=add-edge/read tempdir");
    let graph = Arc::new(GraphForge::new(project.path().to_str()).expect("open"));
    let (src, dst) = seed_pair(&graph);
    let prior = generation(&graph);
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    let reader = {
        let graph = Arc::clone(&graph);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::Builder::new()
            .name("gf-add-edge-reader".into())
            .spawn(move || {
                barrier.wait();
                let outcome = edge_inventory(&graph).map(|(uuids, _)| uuids.len());
                let _ = sender.send((0, outcome));
            })
            .expect("spawn reader")
    };
    let writer = {
        let graph = Arc::clone(&graph);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::Builder::new()
            .name("gf-add-edge-writer".into())
            .spawn(move || {
                barrier.wait();
                let outcome = graph
                    .add_edge(&src, "KNOWS", &dst, &empty_props())
                    .map(|_| 1usize);
                let _ = sender.send((1, outcome));
            })
            .expect("spawn writer")
    };
    drop(sender);
    let outcomes = recv_pair(&receiver, "add-edge/read");
    reader.join().expect("reader panicked");
    writer.join().expect("writer panicked");
    let mut by_worker = [0usize; 2];
    for (worker, result) in outcomes {
        by_worker[worker] = result
            .unwrap_or_else(|error| panic!("phase=add-edge/read worker={worker} failed: {error}"));
    }
    assert_eq!(by_worker[1], 1, "writer must publish one edge");
    assert!(
        by_worker[0] == 0 || by_worker[0] == 1,
        "reader observed partial edge count {}",
        by_worker[0]
    );
    let after = generation(&graph);
    assert_ne!(after, prior);
    let (uuids, _) = edge_inventory(&graph).expect("final inventory");
    assert_eq!(uuids.len(), 1);
}

#[test]
fn add_edge_checkpoint_and_adjacency_remain_correct() {
    let project = tempfile::tempdir().expect("phase=checkpoint tempdir");
    let graph = GraphForge::new(project.path().to_str()).expect("open");
    let (src, dst) = seed_pair(&graph);
    let edge = graph
        .add_edge(&src, "KNOWS", &dst, &empty_props())
        .expect("add_edge");
    graph
        .checkpoint(CheckpointRequest {
            name: "AfterEdge".into(),
            description: None,
            idempotency_key: OperationId(Uuid::from_u128(704)),
            actor_uuid: None,
        })
        .expect("checkpoint");
    let (uuids, ids) = edge_inventory(&graph).expect("inventory");
    assert_eq!(uuids, [edge.uuid.into_bytes()]);
    assert_eq!(ids.len(), 1);

    let view = graph.open_checkpoint("AfterEdge").expect("open checkpoint");
    let pinned = view.execute(EDGE_QUERY).expect("checkpoint edge query");
    let pinned_uuid = pinned.batches[0]
        .column_by_name("edge")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .expect("checkpoint uuid column")
        .value(0);
    assert_eq!(pinned_uuid, edge.uuid.as_bytes());

    let neighbors = graph
        .execute("MATCH (a:Person {name:'A'})-[:KNOWS]->(b:Person) RETURN b.name AS name")
        .expect("adjacency visibility");
    assert_eq!(neighbors.stats.rows_produced, 1);
}

#[test]
fn cross_process_writer_busy_rejects_add_edge() {
    let project = tempfile::tempdir().expect("phase=cross-process tempdir");
    let graph = GraphForge::new(project.path().to_str()).expect("open");
    let (src, dst) = seed_pair(&graph);
    let prior = generation(&graph);
    let held = hold_writer(project.path()).expect("hold writer");
    let error = graph
        .add_edge(&src, "KNOWS", &dst, &empty_props())
        .expect_err("add_edge must observe writer busy");
    assert_eq!(error.code(), "GF_WRITER_BUSY");
    drop(held);
    assert_eq!(generation(&graph), prior);
    assert_eq!(durable_generation(project.path()), prior);
    assert!(edge_inventory(&graph).expect("inventory").0.is_empty());
    let published = graph
        .add_edge(&src, "KNOWS", &dst, &empty_props())
        .expect("add_edge after lock release");
    assert_eq!(
        edge_inventory(&graph).expect("after").0,
        [published.uuid.into_bytes()]
    );
}

#[test]
fn add_edge_failpoint_helper() {
    if std::env::var("GF_ADD_EDGE_FAILPOINT_HELPER").as_deref() != Ok("1") {
        return;
    }
    let root = std::env::var("GF_ADD_EDGE_ROOT").expect("helper root");
    let expect_committed = std::env::var("GF_ADD_EDGE_EXPECT_COMMITTED").expect("flag") == "1";
    let src_uuid = Uuid::parse_str(&std::env::var("GF_ADD_EDGE_SRC").expect("src uuid"))
        .expect("parse src uuid");
    let dst_uuid = Uuid::parse_str(&std::env::var("GF_ADD_EDGE_DST").expect("dst uuid"))
        .expect("parse dst uuid");
    let graph = GraphForge::new(Some(root.as_str())).expect("helper open");
    let parent = generation(&graph);
    let src = NodeHandle::new(src_uuid, "Person", graph.identity.clone());
    let dst = NodeHandle::new(dst_uuid, "Person", graph.identity.clone());
    let error = graph
        .add_edge(&src, "KNOWS", &dst, &empty_props())
        .expect_err("failpoint must abort publication");
    assert!(!error.to_string().is_empty());
    let durable = durable_generation(std::path::Path::new(&root));
    let visible = generation(&graph);
    if expect_committed {
        assert_ne!(durable, parent);
        assert_eq!(visible, durable);
        assert_eq!(edge_inventory(&graph).expect("committed").0.len(), 1);
        let reopened = GraphForge::new(Some(root.as_str())).expect("reopen after commit");
        assert_eq!(
            edge_inventory(&reopened).expect("reopen committed").0.len(),
            1
        );
    } else {
        assert_eq!(durable, parent);
        assert_eq!(visible, parent);
        assert!(edge_inventory(&graph).expect("rolled back").0.is_empty());
        let reopened = GraphForge::new(Some(root.as_str())).expect("reopen after rollback");
        assert_eq!(generation(&reopened), parent);
        assert!(
            edge_inventory(&reopened)
                .expect("reopen inventory")
                .0
                .is_empty()
        );
    }
}

#[test]
fn add_edge_failpoints_reconcile_before_and_after_current() {
    for (failpoint, committed) in [
        ("project.before_current_replace.error", false),
        ("project.after_current_replace.error", true),
    ] {
        let dir = tempfile::TempDir::new().expect("failpoint tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir(&root).expect("create project root");
        let graph = GraphForge::new(root.to_str()).expect("seed open");
        let src = graph.add_node("Person", &empty_props()).expect("seed src");
        let dst = graph.add_node("Person", &empty_props()).expect("seed dst");
        let src_uuid = src.uuid.to_string();
        let dst_uuid = dst.uuid.to_string();
        drop(graph);

        let status = Command::new(std::env::current_exe().expect("current exe"))
            .arg("--exact")
            .arg("construction_concurrency_tests::add_edge_failpoint_helper")
            .arg("--nocapture")
            .env("GF_ADD_EDGE_FAILPOINT_HELPER", "1")
            .env("GF_ADD_EDGE_ROOT", &root)
            .env(
                "GF_ADD_EDGE_EXPECT_COMMITTED",
                if committed { "1" } else { "0" },
            )
            .env("GF_ADD_EDGE_SRC", &src_uuid)
            .env("GF_ADD_EDGE_DST", &dst_uuid)
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", FAILPOINT_COOKIE)
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .expect("spawn failpoint helper");
        assert!(
            status.success(),
            "failpoint helper failed for {failpoint}: {status}"
        );
    }
}
