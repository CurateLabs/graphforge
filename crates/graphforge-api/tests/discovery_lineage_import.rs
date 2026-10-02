//! Research interchange imports preserve immutable Version citations across destinations.

use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, ExecuteResearchBranchRequest,
    ExportResearchRequest, GraphForge, OperationId, PortableV2ImportRequest,
    ResearchReferenceTarget,
};
use uuid::Uuid;

#[test]
fn distinct_imports_reopen_with_identical_version_lineage() {
    let cancellation = CancellationToken::new();
    let source_project = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(source_project.path().to_str()).unwrap();
    graph.execute("CREATE (:Item {x:0})").unwrap();
    let branch_uuid = Uuid::now_v7();
    let base = Uuid::now_v7();
    let origin = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid,
                version_uuid: base,
                source: BranchSource::Current {
                    origin_version_uuid: origin,
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
            },
            &cancellation,
        )
        .unwrap();
    let generation = graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    let head = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation,
                branch_uuid,
                version_uuid: head,
                created_at: 2,
                query: "MATCH (n:Item) SET n.x=1".into(),
            },
            &cancellation,
        )
        .unwrap();
    let reference = graph
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid: head },
            &cancellation,
        )
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let package = root.path().join("head");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: head,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &cancellation,
        )
        .unwrap();
    let first_dir = root.path().join("first");
    let second_dir = root.path().join("second");
    let import = |destination: &std::path::Path| {
        GraphForge::import_portable_v2(
            destination,
            &PortableV2ImportRequest {
                input: package.clone(),
                operation_id: OperationId(Uuid::now_v7()),
                limits: Default::default(),
            },
            None,
        )
        .unwrap()
    };
    import(&first_dir);
    import(&second_dir);
    let first = GraphForge::new(first_dir.to_str()).unwrap();
    let second = GraphForge::new(second_dir.to_str()).unwrap();
    let first_ref = first
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid: head },
            &cancellation,
        )
        .unwrap();
    let second_ref = second
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid: head },
            &cancellation,
        )
        .unwrap();
    assert_eq!(first_ref.version, reference.version);
    assert_eq!(first_ref.genealogy, reference.genealogy);
    assert_eq!(first_ref.identity_sha256, reference.identity_sha256);
    assert_eq!(second_ref.version, reference.version);
    assert_eq!(second_ref.genealogy, reference.genealogy);
    assert_eq!(second_ref.identity_sha256, reference.identity_sha256);
}
