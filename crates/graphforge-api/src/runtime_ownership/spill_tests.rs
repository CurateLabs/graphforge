//! #1595: where each instance's queries may spill.
//!
//! What DataFusion does with a resolved configuration (spill into the given
//! directory under its cap, or refuse when disabled) is proven in
//! `graphforge-exec`'s session tests. These tests prove which configuration
//! each instance and policy resolves to.

use crate::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, ResourcePolicyMode, SpillPolicy,
};
use graphforge_storage::query_spill::{DEFAULT_QUERY_SPILL_MAX_BYTES, QUERY_SPILL_DIR};

fn options(spill: SpillPolicy) -> GraphForgeOptions {
    GraphForgeOptions {
        resource: ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Explicit,
            spill,
            ..ExecutionResourcePolicy::default()
        },
        ..GraphForgeOptions::default()
    }
}

fn scratch_entries(project: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(project.join(QUERY_SPILL_DIR)) else {
        return Vec::new();
    };
    let mut names = entries
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn a_durable_project_spills_into_its_own_capped_scratch_by_default() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy::default()),
    )
    .unwrap();
    assert!(
        scratch_entries(project.path()).is_empty(),
        "scratch is acquired by the first query, not at open"
    );
    let resources = graph.session_resource_config().unwrap();
    assert!(resources.spill_enabled);
    assert_eq!(
        resources.spill_max_bytes,
        Some(DEFAULT_QUERY_SPILL_MAX_BYTES)
    );
    let directory = resources.spill_directory.unwrap();
    assert!(directory.is_dir());
    assert_eq!(
        directory.canonicalize().unwrap().parent().unwrap(),
        project.path().join(QUERY_SPILL_DIR).canonicalize().unwrap()
    );
    // Later queries reuse the same directory.
    assert_eq!(
        graph.session_resource_config().unwrap().spill_directory,
        Some(directory.clone())
    );
    // A query really runs with it.
    assert_eq!(
        graph
            .execute("RETURN 1 AS one")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    let entries = scratch_entries(project.path());
    assert_eq!(entries.len(), 2, "one directory and its lock: {entries:?}");
    drop(graph);
    assert!(
        scratch_entries(project.path()).is_empty(),
        "the instance's scratch outlived it"
    );
}

#[test]
fn the_callers_cap_applies_to_project_scratch() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy {
            enabled: true,
            directory: None,
            max_bytes: Some(4096),
        }),
    )
    .unwrap();
    let resources = graph.session_resource_config().unwrap();
    assert!(resources.spill_enabled);
    assert_eq!(resources.spill_max_bytes, Some(4096));
}

#[test]
fn disabled_spill_resolves_to_no_directory_and_creates_no_scratch() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy {
            enabled: false,
            directory: None,
            max_bytes: None,
        }),
    )
    .unwrap();
    let resources = graph.session_resource_config().unwrap();
    assert!(!resources.spill_enabled);
    assert_eq!(resources.spill_directory, None);
    graph.execute("RETURN 1 AS one").unwrap();
    assert!(!project.path().join(QUERY_SPILL_DIR).exists());
}

#[test]
fn an_in_memory_instance_does_not_spill_by_default() {
    let graph = GraphForge::new_with_options(None, options(SpillPolicy::default())).unwrap();
    let resources = graph.session_resource_config().unwrap();
    assert!(!resources.spill_enabled);
    assert_eq!(resources.spill_directory, None);
}

#[test]
fn a_configured_directory_is_used_instead_of_project_scratch() {
    let project = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy {
            enabled: true,
            directory: Some(spill.path().to_path_buf()),
            max_bytes: None,
        }),
    )
    .unwrap();
    let resources = graph.session_resource_config().unwrap();
    assert!(resources.spill_enabled);
    assert_eq!(resources.spill_max_bytes, None);
    assert_eq!(
        resources.spill_directory.unwrap().canonicalize().unwrap(),
        spill.path().canonicalize().unwrap()
    );
    graph.execute("RETURN 1 AS one").unwrap();
    assert!(!project.path().join(QUERY_SPILL_DIR).exists());
}

/// Two instances of one project each own a scratch directory; dropping one
/// leaves the other's in place.
#[test]
fn two_instances_of_one_project_do_not_share_scratch() {
    let project = tempfile::tempdir().unwrap();
    let path = project.path().to_str().unwrap();
    let first = GraphForge::new_with_options(Some(path), options(SpillPolicy::default())).unwrap();
    let first_dir = first
        .session_resource_config()
        .unwrap()
        .spill_directory
        .unwrap();
    let second = GraphForge::new_with_options(Some(path), options(SpillPolicy::default())).unwrap();
    let second_dir = second
        .session_resource_config()
        .unwrap()
        .spill_directory
        .unwrap();
    assert_ne!(first_dir, second_dir);
    drop(second);
    assert!(
        first_dir.is_dir(),
        "a live instance's scratch was reclaimed"
    );
    assert!(!second_dir.exists());
}

/// A read-only view of a durable project (here a checkpoint view) spills like
/// any other query of that project: it acquires its own scratch.
#[test]
fn a_checkpoint_view_of_a_durable_project_gets_scratch() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy::default()),
    )
    .unwrap();
    graph.execute("CREATE (:Row {v: 1})").unwrap();
    graph
        .checkpoint(crate::CheckpointRequest {
            name: "Spill".into(),
            description: None,
            idempotency_key: crate::OperationId(uuid::Uuid::from_u128(1595)),
            actor_uuid: None,
        })
        .unwrap();
    let before = scratch_entries(project.path());
    let view = graph.open_checkpoint("Spill").unwrap();
    view.execute("MATCH (n:Row) RETURN n.v AS v ORDER BY v")
        .unwrap();
    let during = scratch_entries(project.path());
    assert_eq!(during.len(), before.len() + 2, "{before:?} -> {during:?}");
    drop(view);
    assert_eq!(scratch_entries(project.path()), before);
}

/// EXPLAIN renders a plan and never executes it, so it acquires no scratch.
#[test]
fn explain_acquires_no_scratch() {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        options(SpillPolicy::default()),
    )
    .unwrap();
    graph.explain("MATCH (n) RETURN n ORDER BY n").unwrap();
    assert!(scratch_entries(project.path()).is_empty());
}
