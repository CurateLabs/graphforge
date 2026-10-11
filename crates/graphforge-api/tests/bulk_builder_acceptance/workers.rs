//! Published artifacts are byte-identical across forced worker counts, with
//! enough batches per kind that decode tasks compete (ADR 0058; #1881).

use super::child::{ChildSpec, Conditions, run, spec_for};
use super::support::*;

/// More than sixteen batches per kind, in several row groups, so a decode task
/// (sixteen batches) is not the whole input and tasks run side by side.
pub fn spec() -> Spec {
    Spec {
        nodes: 40 * BATCH_ROWS,
        edges: 60 * BATCH_ROWS,
        node_blobs: 40,
        edge_blobs: 40,
        blob_bytes: 48 << 10,
    }
}

#[test]
fn artifacts_are_byte_identical_across_forced_worker_counts() {
    let directory = tempfile::tempdir().unwrap();
    let spec = spec();
    assert!(spec.nodes / BATCH_ROWS > 16 && spec.edges / BATCH_ROWS > 16);
    let sources = Sources::write(&directory.path().join("input"), spec);

    let mut builds = Vec::new();
    for lanes in [1, 2, 8] {
        let project = directory.path().join(format!("project-{lanes}"));
        std::fs::create_dir(&project).unwrap();
        let outcome = run(
            &ChildSpec {
                lanes: Some(lanes),
                commit: true,
                ..spec_for(&project, &sources)
            },
            &Conditions::default(),
        );
        assert!(outcome.succeeded(), "{lanes} lanes: {outcome:?}");
        let report = outcome.report();
        assert_eq!(report["workers"], lanes);
        // Each kind decoded as several tasks, each reserving its own workspace.
        assert!(
            report["source_workspace_reservations"].as_u64().unwrap() >= 5,
            "{lanes} lanes: {report}"
        );
        assert_eq!(
            (report["nodes"].as_u64(), report["edges"].as_u64()),
            (Some(spec.nodes as u64), Some(spec.edges as u64))
        );
        builds.push((lanes, outcome.inventory()));
    }
    let (first_lanes, first) = &builds[0];
    assert!(first.len() > 30, "{}", first.len());
    for (lanes, inventory) in &builds[1..] {
        assert_eq!(
            inventory_differences(first, inventory),
            Vec::<String>::new(),
            "{lanes} lanes published different bytes than {first_lanes}"
        );
    }
}
