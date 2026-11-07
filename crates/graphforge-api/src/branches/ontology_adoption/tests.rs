use super::*;
use crate::{BranchSource, CreateResearchBranchRequest, SelectResearchBranchRequest};

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: crate::OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}

fn resolved_fixture() -> ResolvedOntologyModule {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/hub/generated/v1");
    let manifest_json = std::fs::read(root.join("manifest.json")).unwrap();
    let refs_json = std::fs::read(root.join("refs.json")).unwrap();
    let manifest = graphforge_discovery::DiscoveryManifest::from_json(
        &manifest_json,
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .unwrap();
    let advertised = &manifest.ontology.as_ref().unwrap().modules[0];
    let module = graphforge_discovery::ExactIdentity {
        id: advertised.id.clone(),
        version: advertised.version.clone(),
        content_digest: advertised.content_digest.clone(),
    };
    let package = root.join(format!(
        "objects/ontology-module-{}.gfpb",
        module.content_digest.0.trim_start_matches("sha256:")
    ));
    crate::resolve_discovered_ontology_module(&crate::DiscoveryOntologyModuleRequest {
        manifest_json: &manifest_json,
        refs_json: &refs_json,
        expected_repository: &manifest.repository,
        module: &module,
        package: &package,
        discovery_limits: graphforge_discovery::DiscoveryLimits::default(),
        portable_limits: crate::PortableLimits::default(),
        cancelled: None,
    })
    .unwrap()
}

fn selected_main(graph: &mut GraphForge) -> Uuid {
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
                author: None,
                committer: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    branch
}

#[test]
fn concurrent_exact_replay_refreshes_live_ontology_before_refusing_stale_preparation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut first = GraphForge::new(path.to_str()).unwrap();
    first.execute("CREATE (:Retained {value:42})").unwrap();
    let branch = selected_main(&mut first);
    let resolved = resolved_fixture();
    let operation = context();
    let source = first.generation_for_read().unwrap();
    assert_eq!(first.ontology_mode(), crate::OntologyMode::Exploratory);
    assert!(first.default_composition_snapshot().is_none());
    // Pause the first call after validating/capturing its source and selected
    // Branch, exactly at the public method's private continuation boundary.
    let mut second = GraphForge::new(path.to_str()).unwrap();
    let published = second
        .adopt_resolved_ontology_module_for_publication(&resolved, operation.clone(), None)
        .unwrap();
    let expected_heads = second.research_version_retention().unwrap().heads;
    assert_ne!(published.project_generation_uuid, source.generation_uuid());
    assert_eq!(second.ontology_mode(), crate::OntologyMode::Advisory);

    // The matching durable receipt permits begin, despite the first captured
    // authority being old. Any refusal must leave a coherently refreshed facade.
    let error = first
        .adopt_verified_ontology_with_branch(
            &resolved,
            &operation,
            &CancellationToken::new(),
            &source,
            branch,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Project {
            code: crate::ProjectErrorCode::WriteConflict,
            ..
        }
    ));
    assert_eq!(
        *first.current_generation_uuid.lock().unwrap(),
        published.project_generation_uuid
    );
    assert_eq!(first.ontology_mode(), second.ontology_mode());
    let live = first
        .default_composition_snapshot()
        .expect("refreshed binding context");
    assert_eq!(live.composition().modules.len(), 1);
    assert_eq!(&live.composition().modules[0].id, resolved.module());
    assert_eq!(
        first.workspace_ontology_composition().unwrap(),
        second.workspace_ontology_composition().unwrap()
    );
    assert_eq!(
        first.research_project_metadata().unwrap(),
        second.research_project_metadata().unwrap()
    );
    assert_eq!(
        first
            .execute("MATCH (n:Retained) RETURN n.value")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );

    for retry in [operation, context()] {
        let receipt = first
            .adopt_resolved_ontology_module_for_publication(&resolved, retry, None)
            .unwrap();
        assert!(receipt.already_adopted);
        assert_eq!(
            receipt.project_generation_uuid,
            published.project_generation_uuid
        );
        assert_eq!(first.ontology_mode(), crate::OntologyMode::Advisory);
        assert!(first.default_composition_snapshot().is_some());
        assert_eq!(
            first.research_version_retention().unwrap().heads,
            expected_heads
        );
    }
    let reopened = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(reopened.ontology_mode(), crate::OntologyMode::Advisory);
    assert_eq!(
        reopened.research_version_retention().unwrap().heads,
        expected_heads
    );
}
