//! Public-facade regression coverage for scoped node label mutation routing.

use arrow::array::{Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use graphforge_api::GraphForge;
use graphforge_storage::concurrency_attribution::RegionCapture;
use std::process::Command;
use tempfile::TempDir;

fn graph_with_shards(unrelated_nodes: usize) -> (TempDir, GraphForge) {
    let root = tempfile::tempdir().expect("project directory");
    let path = root.path().join("state");
    let forge =
        GraphForge::new(Some(path.to_str().expect("UTF-8 test path"))).expect("open project");
    for index in 0..unrelated_nodes {
        forge
            .execute(&format!("CREATE (:Base {{name: 'other-{index}'}})"))
            .expect("create unrelated node");
    }
    forge
        .execute("CREATE (:Base {name: 'target'})")
        .expect("create target node");
    (root, forge)
}

fn scoped_work(
    capture: graphforge_storage::concurrency_attribution::RegionSnapshot,
) -> (u64, u64, u64) {
    let row = capture
        .regions
        .iter()
        .find(|(path, _)| path.ends_with("/node_label_mutation"))
        .map(|(_, row)| row)
        .expect("label mutation attribution");
    (
        row.work.get("fragments_read").copied().unwrap_or_default(),
        row.work.get("rows_decoded").copied().unwrap_or_default(),
        row.work
            .get("label_memberships_rebuilt")
            .copied()
            .unwrap_or_default(),
    )
}

fn only_labels(batches: &[RecordBatch]) -> Vec<String> {
    let labels = batches[0]
        .column_by_name("labels")
        .expect("labels result")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels list")
        .value(0);
    let labels = labels
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("label strings");
    (0..labels.len())
        .filter(|index| !labels.is_null(*index))
        .map(|index| labels.value(index).to_owned())
        .collect()
}

fn exercise(unrelated_nodes: usize) -> ((u64, u64, u64), (u64, u64, u64)) {
    let (_root, forge) = graph_with_shards(unrelated_nodes);
    let retained = forge
        .execute_stream("MATCH (n:Base {name: 'target'}) RETURN labels(n) AS labels")
        .expect("open retained reader");

    let set_capture = RegionCapture::start("label_mutation_test");
    forge
        .execute("MATCH (n:Base {name: 'target'}) SET n:Extra")
        .expect("add label");
    let set_work = scoped_work(set_capture.finish());

    let retained_batches = futures::executor::block_on(retained.try_collect::<Vec<_>>())
        .expect("consume retained reader");
    assert_eq!(only_labels(&retained_batches), vec!["Base".to_owned()]);

    let remove_capture = RegionCapture::start("label_mutation_test");
    forge
        .execute("MATCH (n:Base {name: 'target'}) REMOVE n:Base:Extra")
        .expect("remove labels");
    let remove_work = scoped_work(remove_capture.finish());

    let path = forge.path().expect("durable path").to_path_buf();
    drop(forge);
    let reopened =
        GraphForge::new(Some(path.to_str().expect("UTF-8 test path"))).expect("reopen project");
    let result = reopened
        .execute("MATCH (n) WHERE n.name = 'target' RETURN labels(n) AS labels")
        .expect("read reopened node");
    assert_eq!(only_labels(&result.batches), Vec::<String>::new());
    (set_work, remove_work)
}

#[test]
fn label_rewrites_read_only_the_routed_fragment_at_two_topology_sizes() {
    let small = exercise(2);
    let large = exercise(12);
    // The admitted fixture routes through two rows at both sizes: the legacy
    // flat node membership and the immutable range shard containing the target.
    assert_eq!(small.0, (2, 2, 2));
    assert_eq!(large.0, small.0);
    assert_eq!(small.1, (2, 2, 3));
    assert_eq!(large.1, small.1);
}

#[test]
fn last_label_removal_failure_helper() {
    let Ok(root) = std::env::var("GF_LABEL_MUTATION_FAILURE_ROOT") else {
        return;
    };
    let forge = GraphForge::new(Some(&root)).expect("open project in child");
    let error = forge
        .execute("MATCH (n:Base) REMOVE n:Base")
        .expect_err("injected publication failure");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
}

#[test]
fn last_label_removal_publication_failure_preserves_the_previous_generation() {
    let root = tempfile::tempdir().expect("project directory");
    let path = root.path().join("state");
    let forge =
        GraphForge::new(Some(path.to_str().expect("UTF-8 test path"))).expect("open project");
    forge
        .execute("CREATE (:Base {name: 'target'})")
        .expect("create single-label node");
    drop(forge);

    let status = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "last_label_removal_failure_helper",
            "--nocapture",
        ])
        .env("GF_LABEL_MUTATION_FAILURE_ROOT", &path)
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT",
            "project.before_current_replace.error",
        )
        .status()
        .expect("run publication failure child");
    assert!(status.success(), "failure child exited with {status}");

    let reopened = GraphForge::new(Some(path.to_str().expect("UTF-8 test path")))
        .expect("reopen previous generation");
    let result = reopened
        .execute("MATCH (n:Base) RETURN labels(n) AS labels")
        .expect("read previous label membership");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(only_labels(&result.batches), vec!["Base".to_owned()]);
}
