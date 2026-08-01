use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::{GfError, GraphForge, RankOptions};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

const DEADLINE: Duration = Duration::from_secs(10);
const QUERY: &str = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";

type Fingerprints = ([u8; 32], [u8; 32]);
type Worker = Box<dyn FnOnce() -> Result<Fingerprints, GfError> + Send>;

fn logical_fingerprint(batches: &[RecordBatch]) -> Result<[u8; 32], GfError> {
    let logical = batches
        .iter()
        .map(|batch| {
            let schema = Arc::new(Schema::new(batch.schema().fields().clone()));
            RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|error| {
                GfError::Execution(format!("logical Arrow normalization failed: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::canonical_arrow::result_fingerprint(&logical)
        .map_err(|error| GfError::Execution(format!("canonical Arrow result failed: {error}")))
}

fn seed(path: &std::path::Path) {
    let graph = GraphForge::new(path.to_str()).expect("phase=seed open project");
    graph
        .execute(
            "CREATE \
             (alice:Person {name:'Alice'}), \
             (bob:Person {name:'Bob'}), \
             (carol:Person {name:'Carol'}), \
             (alice)-[:KNOWS]->(bob), \
             (bob)-[:KNOWS]->(carol)",
        )
        .expect("phase=seed publish fixture");
}

fn fingerprints(graph: &GraphForge) -> Result<Fingerprints, GfError> {
    let query = graph.execute(QUERY)?;
    let rank = graph.rank("Person", RankOptions::default())?;
    let query = logical_fingerprint(&query.batches)?;
    let rank = logical_fingerprint(&[rank])?;
    Ok((query, rank))
}

fn run_workers(phase: &'static str, workers: Vec<Worker>) -> Vec<Fingerprints> {
    let count = workers.len();
    let deadline = Instant::now() + DEADLINE;
    let barrier = Arc::new(Barrier::new(count));
    let (sender, receiver) = mpsc::sync_channel(count);
    let handles = workers
        .into_iter()
        .enumerate()
        .map(|(worker, task)| {
            let barrier = Arc::clone(&barrier);
            let sender = sender.clone();
            thread::Builder::new()
                .name(format!("gf-{phase}-{worker}"))
                .spawn(move || {
                    barrier.wait();
                    let outcome = std::panic::catch_unwind(AssertUnwindSafe(task))
                        .map_err(|_| "worker task panicked".to_owned())
                        .and_then(|result| {
                            result.map_err(|error| format!("operation failed: {error}"))
                        });
                    let _ = sender.send((worker, outcome));
                })
                .unwrap_or_else(|error| {
                    panic!("phase={phase} worker={worker} spawn failed: {error}")
                })
        })
        .collect::<Vec<_>>();
    drop(sender);

    let mut results = Vec::with_capacity(count);
    for completed in 0..count {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (worker, result) = receiver.recv_timeout(remaining).unwrap_or_else(|error| {
            panic!(
                "phase={phase} completed={completed}/{count} timeout={DEADLINE:?} \
                 channel={error}"
            )
        });
        let result =
            result.unwrap_or_else(|error| panic!("phase={phase} worker={worker} failed: {error}"));
        results.push((worker, result));
    }
    for (worker, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|_| panic!("phase={phase} worker={worker} panicked"));
    }
    results.sort_unstable_by_key(|(worker, _)| *worker);
    results.into_iter().map(|(_, result)| result).collect()
}

fn assert_equal(phase: &str, results: &[Fingerprints]) {
    let expected = results
        .first()
        .unwrap_or_else(|| panic!("phase={phase} produced no results"));
    for (worker, actual) in results.iter().enumerate().skip(1) {
        assert_eq!(
            actual, expected,
            "phase={phase} worker={worker} canonical ordered Arrow differs"
        );
    }
}

#[test]
fn independent_instances_and_one_instance_reads_are_deterministic() {
    let first = tempfile::tempdir().expect("phase=independent first tempdir");
    let second = tempfile::tempdir().expect("phase=independent second tempdir");
    seed(first.path());
    seed(second.path());
    let first_graph = GraphForge::new(first.path().to_str()).expect("phase=independent first open");
    let second_graph =
        GraphForge::new(second.path().to_str()).expect("phase=independent second open");
    let results = run_workers(
        "independent-instances",
        vec![
            Box::new(move || fingerprints(&first_graph)),
            Box::new(move || fingerprints(&second_graph)),
        ],
    );
    assert_eq!(
        results[0].0, results[1].0,
        "phase=independent-instances ordered query Arrow differs"
    );

    let shared = Arc::new(GraphForge::new(first.path().to_str()).expect("phase=one-instance open"));
    let workers = (0..4)
        .map(|_| {
            let graph = Arc::clone(&shared);
            Box::new(move || fingerprints(&graph)) as Worker
        })
        .collect();
    assert_equal(
        "one-instance-reads",
        &run_workers("one-instance-reads", workers),
    );
}

#[test]
fn cross_session_reads_are_canonically_equal() {
    let project = tempfile::tempdir().expect("phase=cross-session tempdir");
    seed(project.path());
    let workers = (0..4)
        .map(|worker| {
            let graph = GraphForge::new(project.path().to_str()).unwrap_or_else(|error| {
                panic!("phase=cross-session worker={worker} open: {error}")
            });
            Box::new(move || fingerprints(&graph)) as Worker
        })
        .collect();
    assert_equal(
        "cross-session-reads",
        &run_workers("cross-session-reads", workers),
    );
}

#[test]
fn synchronous_calls_complete_inside_existing_tokio_runtime() {
    let project = tempfile::tempdir().expect("phase=nested-runtime tempdir");
    seed(project.path());
    let graph = GraphForge::new(project.path().to_str()).expect("phase=nested-runtime open");
    let (sender, receiver) = mpsc::sync_channel(1);
    let handle = thread::Builder::new()
        .name("gf-existing-tokio-runtime".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("phase=nested-runtime build ambient runtime");
            let result = runtime.block_on(async move { fingerprints(&graph) });
            let _ = sender.send(result);
        })
        .expect("phase=nested-runtime spawn");
    let nested = receiver.recv_timeout(DEADLINE).unwrap_or_else(|error| {
        panic!("phase=nested-runtime timeout={DEADLINE:?} channel={error}")
    });
    nested.unwrap_or_else(|error| panic!("phase=nested-runtime operation failed: {error}"));
    handle
        .join()
        .unwrap_or_else(|_| panic!("phase=nested-runtime worker panicked"));
}
