use super::*;
fn current(graph: &GraphForge) -> Uuid {
    graph.generation_for_read().unwrap().generation_uuid()
}
pub(super) fn prepare(graph: &mut GraphForge) -> UpdateResearchBranchRequest {
    graph.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Recovered study".into(),
    };
    graph
        .create_research_branch(&branch, &CancellationToken::new())
        .unwrap();
    graph.execute("MATCH (n:Item) SET n.x=1,n.y=2").unwrap();
    let request = PreviewResearchUpstreamRequest {
        branch_uuid: branch.branch_uuid,
        scope: ResearchUpstreamScope::Branch,
    };
    let view = preview::load(graph, &request, &CancellationToken::new()).unwrap();
    let x = view
        .rows
        .iter()
        .find(|row| row.key.2 == "property:x")
        .unwrap();
    UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        version_uuid: Uuid::now_v7(),
        preview: request,
        preview_sha256: view.digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: x.key.0.clone(),
                    object_uuid: x.key.1,
                    field: x.key.2.clone(),
                },
                resolution: ResearchUpstreamResolution::KeepLocal,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        explanation: "Retain local interpretation".into(),
    }
}
#[test]
fn upstream_fault_helper() {
    let Ok(root) = std::env::var("GF_UPSTREAM_FAULT_ROOT") else {
        return;
    };
    let request: UpdateResearchBranchRequest =
        serde_json::from_slice(&std::fs::read(format!("{root}.request.json")).unwrap()).unwrap();
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let error = graph
        .update_research_branch(&request, &CancellationToken::new())
        .unwrap_err();
    let post = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT")
        .unwrap()
        .contains("after_current");
    let registry = graph.research_version_retention().unwrap();
    if post {
        assert!(error.to_string().contains("committed=true"), "{error}");
        assert_eq!(registry.upstream.reviews.len(), 1);
        assert_eq!(
            registry.heads[&request.preview.branch_uuid],
            request.version_uuid
        );
        let before = current(&graph);
        graph
            .update_research_branch(&request, &CancellationToken::new())
            .unwrap();
        assert_eq!(current(&graph), before);
    } else {
        assert_eq!(current(&graph), request.expected_generation_uuid);
        assert!(registry.upstream.reviews.is_empty());
        assert_ne!(
            registry.heads[&request.preview.branch_uuid],
            request.version_uuid
        );
    }
}
#[test]
fn both_current_boundaries_preserve_atomic_baseline_history_and_exact_replay() {
    for phase in ["before", "after"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        let request = prepare(&mut graph);
        std::fs::write(
            format!("{}.request.json", root.display()),
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "research_upstream::tests::recovery::upstream_fault_helper",
                "--nocapture",
            ])
            .env("GF_UPSTREAM_FAULT_ROOT", &root)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                request.operation_uuid.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                format!("project.{phase}_current_replace.error"),
            )
            .status()
            .unwrap();
        assert!(status.success(), "{phase}: {status}");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        let before = current(&graph);
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .upstream
                .reviews
                .len(),
            usize::from(phase == "after")
        );
        let receipt = graph
            .update_research_branch(&request, &CancellationToken::new())
            .unwrap();
        if phase == "after" {
            assert_eq!(current(&graph), before);
        }
        assert_eq!(
            graph
                .update_research_branch(&request, &CancellationToken::new())
                .unwrap(),
            receipt
        );
        let registry = graph.research_version_retention().unwrap();
        let base = registry.branches[&request.preview.branch_uuid].base_version_uuid;
        graph
            .restore_research_branch(
                &RestoreResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    branch_uuid: request.preview.branch_uuid,
                    source_version_uuid: base,
                    version_uuid: Uuid::now_v7(),
                    created_at: 3,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        drop(graph);
        graphforge_storage::execute_project_cleanup(
            &root,
            graphforge_storage::ProjectRetentionPolicy {
                retained_ancestors: 0,
            },
            Default::default(),
        )
        .unwrap();
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        let restored = current(&graph);
        assert_eq!(
            graph
                .update_research_branch(&request, &CancellationToken::new())
                .unwrap(),
            receipt
        );
        assert_eq!(current(&graph), restored);
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .upstream
                .reviews
                .len(),
            1
        );
        let review = preview::load(&graph, &request.preview, &CancellationToken::new()).unwrap();
        assert_eq!(
            review
                .rows
                .iter()
                .find(|row| row.key.2 == "property:x")
                .unwrap()
                .change,
            "upstream"
        );
    }
}
