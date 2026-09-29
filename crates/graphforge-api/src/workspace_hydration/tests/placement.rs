use super::*;

// Only the child changes its environment: other libtest workers may be using
// TMPDIR concurrently. Linux's tmpfs fixture exercises actual EXDEV boundaries.
#[cfg(target_os = "linux")]
#[test]
fn durable_hydration_ignores_cross_volume_tmpdir() {
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;

    const CHILD_PROJECT: &str = "GF_HYDRATION_PLACEMENT_PROJECT";
    if let Some(project) = std::env::var_os(CHILD_PROJECT) {
        let project = PathBuf::from(project);
        assert_ne!(
            std::fs::metadata(&project).unwrap().dev(),
            std::fs::metadata(std::env::temp_dir()).unwrap().dev()
        );
        exercise_project(&project);
        let replay = tempfile::tempdir_in(project.parent().unwrap()).unwrap();
        exercise_compact_replay(replay.path());
        let ephemeral = GraphForge::new(None).unwrap();
        ephemeral.execute("CREATE (:Memory)").unwrap();
        assert_eq!(
            ephemeral
                .execute("MATCH (n:Memory) RETURN n")
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        let unsupported = std::env::temp_dir().join("durable-project");
        let error = GraphForge::new(unsupported.to_str()).err().unwrap();
        assert!(
            error.to_string().contains("GF_UNSUPPORTED_FILESYSTEM"),
            "{error}"
        );
        return;
    }

    let project = tempfile::tempdir().unwrap();
    let ambient = tempfile::tempdir_in("/dev/shm").expect("Linux tmpfs fixture");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "workspace_hydration::tests::placement::durable_hydration_ignores_cross_volume_tmpdir",
            "--nocapture",
        ])
        .env(CHILD_PROJECT, project.path())
        .env("TMPDIR", ambient.path())
        .env("TMP", ambient.path())
        .env("TEMP", ambient.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn hydration_workspaces_are_private_and_owned() {
    let project = tempfile::tempdir().unwrap();
    exercise_project(project.path());
}

fn exercise_project(project: &Path) {
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();
    publish_compact_graph_workspace(project, &graph.dir());
    drop(graph);

    let first = GraphForge::new(project.to_str()).unwrap();
    let second = GraphForge::new(project.to_str()).unwrap();
    let retained = first.dir();
    let first_path = retained.path().to_path_buf();
    let second_path = second.dir().path().to_path_buf();
    assert_eq!(first_path.parent(), Some(project));
    assert_eq!(second_path.parent(), Some(project));
    assert_ne!(first_path, second_path);
    assert!(first.graph_open_evidence().files_reused > 0);
    let current = std::fs::read(project.join("CURRENT")).unwrap();

    let resolved = graphforge_storage::resolve_project_generation(project).unwrap();
    let (read_only_path, read_only_owner, evidence) =
        hydrate_graph_workspace(&resolved, true).unwrap();
    assert_eq!(read_only_path.parent(), Some(project));
    assert!(evidence.files_reused > 0);
    drop(read_only_owner);
    assert!(!read_only_path.exists());
    drop(resolved);

    drop(first);
    assert!(
        first_path.exists(),
        "a retained reader still owns its workspace"
    );
    drop(retained);
    assert!(!first_path.exists(), "last owner removes its workspace");
    assert!(
        second_path.exists(),
        "another reader's workspace is untouched"
    );
    assert_ada(&second);
    drop(second);
    assert!(!second_path.exists());
    assert_eq!(std::fs::read(project.join("CURRENT")).unwrap(), current);

    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_ada(&reopened);
    drop(reopened);
    assert!(std::fs::read_dir(project).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        let name = name.to_string_lossy();
        !name.starts_with("graphforge-graph-workspace-")
            && !name.starts_with(".gf-property-scratch-")
    }));
}

fn assert_ada(graph: &GraphForge) {
    let result = graph
        .execute("MATCH (n:Person) RETURN n.name AS name")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
    let names = result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Ada");
}
