//! Publication staging placement and native payload sizing.
use super::*;

#[test]
fn publication_staging_is_project_adjacent_or_explicit_and_preflighted() {
    let history = history();
    let default = prepare_staging(&history.graph, None).unwrap();
    assert_eq!(default.path().parent(), Some(history.root.path()));
    let explicit = history.root.path().join("stage");
    std::fs::create_dir(&explicit).unwrap();
    let staged = prepare_staging(&history.graph, Some(&explicit)).unwrap();
    assert_eq!(staged.path().parent(), Some(explicit.as_path()));
    let before = std::fs::read_dir(&explicit).unwrap().count();
    assert!(prepare_staging_with_space(&history.graph, Some(&explicit), |_| Ok(0)).is_err());
    assert_eq!(
        std::fs::read_dir(&explicit).unwrap().count(),
        before,
        "low space fails before creating a materialization"
    );
    let missing = history.root.path().join("missing");
    assert!(prepare_staging(&history.graph, Some(&missing)).is_err());
    assert!(!missing.exists());
    assert!(prepare_staging(&history.graph, Some(Path::new(""))).is_err());
}

#[test]
fn publication_staging_counts_graph_and_retained_research_before_materialization() {
    let history = history();
    let preview = history
        .graph
        .preview_portable_selection(&PortableSelectionPreviewRequest {
            selection: PortableSelection::Current,
            request: PortableSelectionRequest {
                profile: PortableSelectionProfile::Complete,
                strict: true,
            },
            limits: PortableLimits::default(),
        })
        .unwrap();
    let payload = history.graph.publication_payload_bytes().unwrap();
    assert!(
        payload > preview.estimated_payload_bytes,
        "native graph and historical research are outside participant metadata"
    );
    let old_allowance = preview.estimated_payload_bytes * 3 + 512 * 1024 * 1024;
    let stage = history.root.path().join("size-preflight");
    std::fs::create_dir(&stage).unwrap();
    let before = std::fs::read_dir(&stage).unwrap().count();
    let error = prepare_staging_with_space(&history.graph, Some(&stage), |_| Ok(old_allowance))
        .unwrap_err();
    assert!(error.to_string().contains("insufficient free space"));
    assert_eq!(std::fs::read_dir(&stage).unwrap().count(), before);
    let scratch =
        prepare_staging_with_space(&history.graph, Some(&stage), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(scratch.path().parent(), Some(stage.as_path()));
    let after = history
        .graph
        .preview_portable_selection(&PortableSelectionPreviewRequest {
            selection: PortableSelection::Current,
            request: PortableSelectionRequest {
                profile: PortableSelectionProfile::Complete,
                strict: true,
            },
            limits: PortableLimits::default(),
        })
        .unwrap();
    assert_eq!(
        preview, after,
        "sizing leaves selection identity and source unchanged"
    );
}

#[test]
fn publication_staging_accepts_a_bare_relative_project_path() {
    const CHILD: &str = "GRAPHFORGE_TEST_RELATIVE_PUBLICATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let graph = GraphForge::new(Some("project")).unwrap();
        assert_eq!(graph.path().unwrap().parent(), Some(Path::new("")));
        let stage = prepare_staging(&graph, None).unwrap();
        assert_eq!(
            stage.path().parent().unwrap().canonicalize().unwrap(),
            std::env::current_dir().unwrap()
        );
        std::fs::write("relative-stage-verified", b"verified").unwrap();
        return;
    }
    // Isolate the real relative-path invocation and its parent admission lock;
    // never mutate the test process's shared current directory.
    let root = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "hub_publish::tests::staging::publication_staging_accepts_a_bare_relative_project_path",
        ])
        .current_dir(root.path())
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read(root.path().join("relative-stage-verified")).unwrap(),
        b"verified",
        "the child must execute its assertions, not merely match zero tests"
    );
}

#[test]
fn publication_staging_counts_current_graph_files_without_research() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:42})").unwrap();
    assert!(
        graph
            .research_version_retention()
            .unwrap()
            .versions
            .is_empty()
    );
    let metadata = graph
        .preview_portable_selection(&PortableSelectionPreviewRequest {
            selection: PortableSelection::Current,
            request: PortableSelectionRequest {
                profile: PortableSelectionProfile::Complete,
                strict: true,
            },
            limits: PortableLimits::default(),
        })
        .unwrap();
    assert!(
        graph.publication_payload_bytes().unwrap() > metadata.estimated_payload_bytes,
        "current native graph files must contribute without any historical Versions"
    );
}

#[test]
fn publication_staging_counts_historical_only_graph_payload() {
    let mut history = history();
    let before = history.graph.publication_payload_bytes().unwrap();
    let current_bytes = GraphForge::new(history.graph.path().unwrap().to_str())
        .unwrap()
        .graph_open_evidence()
        .bytes_validated;
    assert!(
        current_bytes > 0,
        "native reopen must observe the current graph inventory"
    );
    let mut state = 0x1932_1776_u32;
    let mut text = String::new();
    for _ in 0..64 * 1024 {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        use std::fmt::Write as _;
        write!(&mut text, "{:02x}", state as u8).unwrap();
    }
    history
        .graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                label: None,
                description: None,
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&history.graph),
                branch_uuid: history.branch,
                version_uuid: Uuid::now_v7(),
                created_at: 3,
                query: format!("MATCH (n:Item) SET n.payload='{text}'"),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        GraphForge::new(history.graph.path().unwrap().to_str())
            .unwrap()
            .graph_open_evidence()
            .bytes_validated,
        current_bytes,
        "reopened current graph excludes the historical-only payload"
    );
    let after = history.graph.publication_payload_bytes().unwrap();
    assert!(
        after >= before + 64 * 1024,
        "retained incompressible graph bytes must be counted"
    );
    let stage = history.root.path().join("historical-space");
    std::fs::create_dir(&stage).unwrap();
    let error = prepare_staging_with_space(&history.graph, Some(&stage), |_| {
        Ok(before * 3 + 512 * 1024 * 1024)
    })
    .unwrap_err();
    assert!(error.to_string().contains("insufficient free space"));
    assert_eq!(std::fs::read_dir(&stage).unwrap().count(), 0);
}
